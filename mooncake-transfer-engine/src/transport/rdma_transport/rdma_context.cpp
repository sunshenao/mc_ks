// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "transport/rdma_transport/rdma_context.h"

#include <fcntl.h>
#include <sys/epoll.h>

#include <atomic>
#include <cassert>
#include <fstream>
#include <memory>
#include <thread>

#include "config.h"
#include "transport/rdma_transport/endpoint_store.h"
#include "transport/rdma_transport/rdma_endpoint.h"
#include "transport/rdma_transport/rdma_transport.h"
#include "transport/rdma_transport/worker_pool.h"
#include "transport/transport.h"

namespace mooncake {

/**
 * @brief 检查GID是否为空
 * @param gid 待检查的GID指针
 * @return 如果GID全为0返回1，否则返回0
 *
 * GID(Global Identifier)是RDMA网络中的全局标识符，
 * 用于RoCE网络中的路由和寻址。
 */
static int isNullGid(union ibv_gid *gid) {
    for (int i = 0; i < 16; ++i) {
        if (gid->raw[i] != 0) return 0;
    }
    return 1;
}

/**
 * @brief RDMA上下文构造函数
 * @param engine RDMA传输引擎引用
 * @param device_name 设备名称（如"mlx5_0"）
 *
 * 初始化RDMA上下文：
 * 1. 设置设备参数
 * 2. 初始化计数器
 * 3. 确保fork安全性
 */
RdmaContext::RdmaContext(RdmaTransport &engine, const std::string &device_name)
    : device_name_(device_name),            // RDMA设备名称
      engine_(engine),                      // 传输引擎引用
      next_comp_channel_index_(0),          // 完成通道索引
      next_comp_vector_index_(0),           // 完成向量索引
      next_cq_list_index_(0),               // 完成队列索引
      worker_pool_(nullptr),                // 工作线程池指针
      active_(true) {                       // 设备活跃状态
    // 确保fork安全性的一次性初始化
    static std::once_flag g_once_flag;
    auto fork_init = []() {
        int ret = ibv_fork_init();
        if (ret) PLOG(ERROR) << "RDMA context setup failed: fork compatibility";
    };
    std::call_once(g_once_flag, fork_init);
}

RdmaContext::~RdmaContext() {
    if (context_) deconstruct();
}

/**
 * @brief 构建RDMA上下文
 * @param num_cq_list 完成队列数量
 * @param num_comp_channels 完成通道数量
 * @param port RDMA端口号
 * @param gid_index GID索引
 * @param max_cqe 每个完成队列的最大条目数
 * @param max_endpoints 最大端点数
 * @return 成功返回0，失败返回错误码
 *
 * 该函数完成RDMA上下文的完整初始化：
 * 1. 创建端点存储管理器
 * 2. 打开并配置RDMA设备
 * 3. 分配保护域(PD)
 * 4. 创建完成通道和事件系统
 * 5. 创建完成队列(CQ)
 * 6. 启动工作线程池
 */
int RdmaContext::construct(size_t num_cq_list, size_t num_comp_channels,
                           uint8_t port, int gid_index, size_t max_cqe,
                           int max_endpoints) {
    // 创建端点存储管理器（使用SIEVE缓存策略）
    endpoint_store_ = std::make_shared<SIEVEEndpointStore>(max_endpoints);
    // 打开RDMA设备
    if (openRdmaDevice(device_name_, port, gid_index)) {
        LOG(ERROR) << "Failed to open device " << device_name_ << " on port "
                   << port << " with GID " << gid_index;
        return ERR_CONTEXT;
    }
    // 分配保护域(PD)
    pd_ = ibv_alloc_pd(context_);
    if (!pd_) {
        PLOG(ERROR) << "Failed to allocate new protection domain on device "
                    << device_name_;
        return ERR_CONTEXT;
    }
    // 创建完成通道
    num_comp_channel_ = num_comp_channels;
    comp_channel_ = new ibv_comp_channel *[num_comp_channels];
    for (size_t i = 0; i < num_comp_channels; ++i) {
        comp_channel_[i] = ibv_create_comp_channel(context_);
        if (!comp_channel_[i]) {
            PLOG(ERROR) << "Failed to create completion channel on device "
                        << device_name_;
            return ERR_CONTEXT;
        }
    }
    // 创建epoll事件系统
    event_fd_ = epoll_create1(0);
    if (event_fd_ < 0) {
        PLOG(ERROR) << "Failed to create epoll";
        return ERR_CONTEXT;
    }
    // 注册异步事件文件描述符
    if (joinNonblockingPollList(event_fd_, context_->async_fd)) {
        LOG(ERROR) << "Failed to register context async fd to epoll";
        close(event_fd_);
        return ERR_CONTEXT;
    }
// 注册完成通道文件描述符
    for (size_t i = 0; i < num_comp_channel_; ++i)
        if (joinNonblockingPollList(event_fd_, comp_channel_[i]->fd)) {
            LOG(ERROR) << "Failed to register completion channel " << i
                       << " to epoll";
            close(event_fd_);
            return ERR_CONTEXT;
        }
    // 创建完成队列
    cq_list_.resize(num_cq_list);
    for (size_t i = 0; i < num_cq_list; ++i) {
        cq_list_[i] = ibv_create_cq(context_, max_cqe, this /* CQ context */,
                                    compChannel(), compVector());
        if (!cq_list_[i]) {
            PLOG(ERROR) << "Failed to create completion queue";
            close(event_fd_);
            return ERR_CONTEXT;
        }
    }
    // 创建工作线程池
    worker_pool_ = std::make_shared<WorkerPool>(*this, socketId());
    // 输出设备信息
    LOG(INFO) << "RDMA device: " << context_->device->name << ", LID: " << lid_
              << ", GID: (GID_Index " << gid_index_ << ") " << gid();

    return 0;
}

/**
 * @brief 获取设备的NUMA节点ID
 * @return NUMA节点ID，如果无法获取则返回0
 *
 * 通过读取sysfs文件系统获取RDMA设备的NUMA节点信息，
 * 用于CPU亲和性设置。
 */
int RdmaContext::socketId() {
    // 构建sysfs路径
    std::string path =
        "/sys/class/infiniband/" + device_name_ + "/device/numa_node";
    // 读取NUMA节点ID
    std::ifstream file(path);
    if (file.is_open()) {
        int socket_id;
        file >> socket_id;
        file.close();
        return socket_id;
    } else {
        return 0;  // 如果无法读取，默认使用节点0
    }
}

/**
 * @brief 清理RDMA上下文资源
 * @return 成功返回0
 *
 * 按以下顺序清理资源：
 * 1. 清理工作线程池
 * 2. 销毁所有队列对
 * 3. 注销内存区域
 * 4. 销毁完成队列
 * 5. 关闭事件系统
 * 6. 销毁完成通道
 * 7. 释放保护域
 * 8. 关闭设备上下文
 */
int RdmaContext::deconstruct() {
    // 清理工作线程池
    worker_pool_.reset();

    // 销毁所有队列对
    endpoint_store_->destroyQPs();

    // 注销所有内存区域
    for (auto &entry : memory_region_list_) {
        int ret = ibv_dereg_mr(entry);
        if (ret) {
            PLOG(ERROR) << "Failed to unregister memory region";
        }
    }
    memory_region_list_.clear();

    // 销毁所有完成队列
    for (size_t i = 0; i < cq_list_.size(); ++i) {
        int ret = ibv_destroy_cq(cq_list_[i]);
        if (ret) {
            PLOG(ERROR) << "Failed to destroy completion queue";
        }
    }
    cq_list_.clear();
    // 关闭epoll事件系统
    if (event_fd_ >= 0) {
        if (close(event_fd_)) LOG(ERROR) << "Failed to close epoll fd";
        event_fd_ = -1;
    }
    // 销毁完成通道
    if (comp_channel_) {
        for (size_t i = 0; i < num_comp_channel_; ++i)
            if (comp_channel_[i])
                if (ibv_destroy_comp_channel(comp_channel_[i]))
                    LOG(ERROR) << "Failed to destroy completion channel";
        delete[] comp_channel_;
        comp_channel_ = nullptr;
    }
    // 释放保护域
    if (pd_) {
        if (ibv_dealloc_pd(pd_))
            PLOG(ERROR) << "Failed to deallocate protection domain";
        pd_ = nullptr;
    }
    // 关闭设备上下文
    if (context_) {
        if (ibv_close_device(context_))
            PLOG(ERROR) << "Failed to close device context";
        context_ = nullptr;
    }

    if (globalConfig().verbose)
        LOG(INFO) << "Release resources of RDMA device: " << device_name_;

    return 0;
}

/**
 * @brief 注册内存区域
 * @param addr 内存地址
 * @param length 内存长度
 * @param access 访问权限
 * @return 成功返回0，失败返回错误码
 *
 * 将内存区域注册到RDMA设备：
 * 1. 调用ibv_reg_mr注册内存
 * 2. 获取本地和远程访问密钥
 * 3. 保存内存区域记录
 */
int RdmaContext::registerMemoryRegion(void *addr, size_t length, int access) {
    // 向RDMA设备注册内存区域
    ibv_mr *mr = ibv_reg_mr(pd_, addr, length, access);
    if (!mr) {
        PLOG(ERROR) << "Failed to register memory " << addr;
        return ERR_CONTEXT;
    }

    RWSpinlock::WriteGuard guard(memory_regions_lock_);
    memory_region_list_.push_back(mr);

    if (globalConfig().verbose) {
        LOG(INFO) << "Memory region: " << addr << " -- "
                  << (void *)((uintptr_t)addr + length)
                  << ", Device name: " << device_name_ << ", Length: " << length
                  << " (" << length / 1024 / 1024 << " MB)"
                  << ", Permission: " << access << std::hex
                  << ", LKey: " << mr->lkey << ", RKey: " << mr->rkey;
    }

    return 0;
}
/**
 * @brief 注销内存区域
 * @param addr 待注销的内存地址
 * @return 成功返回0
 *
 * 从RDMA设备注销内存区域：
 * 1. 遍历已注册的内存区域
 * 2. 找到与给定地址匹配的内存区域
 * 3. 调用ibv_dereg_mr注销内存区域
 */
int RdmaContext::unregisterMemoryRegion(void *addr) {
    RWSpinlock::WriteGuard guard(memory_regions_lock_);
    bool has_removed;
    do {
        has_removed = false;
        for (auto iter = memory_region_list_.begin();
             iter != memory_region_list_.end(); ++iter) {
            if ((*iter)->addr <= addr &&
                addr < (char *)((*iter)->addr) + (*iter)->length) {
                if (ibv_dereg_mr(*iter)) {
                    LOG(ERROR) << "Failed to unregister memory " << addr;
                    return ERR_CONTEXT;
                }
                memory_region_list_.erase(iter);
                has_removed = true;
                break;
            }
        }
    } while (has_removed);
    return 0;
}

/**
 * @brief 获取远程访问密钥
 * @param addr 内存地址
 * @return 对应的远程访问密钥（rkey），未找到时返回0
 *
 * 通过遍历已注册的内存区域，查找给定地址对应的远程访问密钥。
 */
uint32_t RdmaContext::rkey(void *addr) {
    RWSpinlock::ReadGuard guard(memory_regions_lock_);
    for (auto iter = memory_region_list_.begin();
         iter != memory_region_list_.end(); ++iter)
        if ((*iter)->addr <= addr &&
            addr < (char *)((*iter)->addr) + (*iter)->length)
            return (*iter)->rkey;

    LOG(ERROR) << "Address " << addr << " rkey not found for " << deviceName();
    return 0;
}

/**
 * @brief 获取本地访问密钥
 * @param addr 内存地址
 * @return 对应的本地访问密钥（lkey），未找到时返回0
 *
 * 通过遍历已注册的内存区域，查找给定地址对应的本地访问密钥。
 */
uint32_t RdmaContext::lkey(void *addr) {
    RWSpinlock::ReadGuard guard(memory_regions_lock_);
    for (auto iter = memory_region_list_.begin();
         iter != memory_region_list_.end(); ++iter)
        if ((*iter)->addr <= addr &&
            addr < (char *)((*iter)->addr) + (*iter)->length)
            return (*iter)->lkey;

    LOG(ERROR) << "Address " << addr << " lkey not found for " << deviceName();
    return 0;
}

/**
 * @brief 获取或创建RDMA端点
 * @param peer_nic_path 对端NIC的路径
 * @return 成功返回RDMA端点指针，失败返回nullptr
 *
 * 通过端点存储管理器获取或创建与对端NIC路径对应的RDMA端点。
 * 如果端点存储管理器中已存在该端点，则直接返回；
 * 否则，创建一个新的端点并返回。
 */
std::shared_ptr<RdmaEndPoint> RdmaContext::endpoint(
    const std::string &peer_nic_path) {
    if (!active_) {
        LOG(ERROR) << "Endpoint is not active";
        return nullptr;
    }

    if (peer_nic_path.empty()) {
        LOG(ERROR) << "Invalid peer NIC path";
        return nullptr;
    }

    auto endpoint = endpoint_store_->getEndpoint(peer_nic_path);
    if (endpoint) {
        return endpoint;
    } else {
        auto endpoint = endpoint_store_->insertEndpoint(peer_nic_path, this);
        return endpoint;
    }

    endpoint_store_->reclaimEndpoint();
    return nullptr;
}

/**
 * @brief 删除RDMA端点
 * @param peer_nic_path 对端NIC的路径
 * @return 成功返回0，失败返回错误码
 *
 * 从端点存储管理器中删除与对端NIC路径对应的RDMA端点。
 */
int RdmaContext::deleteEndpoint(const std::string &peer_nic_path) {
    return endpoint_store_->deleteEndpoint(peer_nic_path);
}

/**
 * @brief 获取NIC路径
 * @return 当前设备的NIC路径
 *
 * 根据本地服务器名称和设备名称生成当前设备的NIC路径。
 */
std::string RdmaContext::nicPath() const {
    return MakeNicPath(engine_.local_server_name_, device_name_);
}

/**
 * @brief 获取GID字符串
 * @return 当前设备的GID字符串
 *
 * 将GID转换为可读的字符串格式（如"00:11:22:33:44:55:66:77"）。
 */
std::string RdmaContext::gid() const {
    std::string gid_str;
    char buf[16] = {0};
    const static size_t kGidLength = 16;
    for (size_t i = 0; i < kGidLength; ++i) {
        sprintf(buf, "%02x", gid_.raw[i]);
        gid_str += i == 0 ? buf : std::string(":") + buf;
    }

    return gid_str;
}

/**
 * @brief 获取一个可用的完成队列
 * @return 指向可用完成队列的指针
 *
 * 轮询所有可用的完成队列，返回一个可用的完成队列指针。
 */
ibv_cq *RdmaContext::cq() {
    int index = (next_cq_list_index_++) % cq_list_.size();
    return cq_list_[index];
}

/**
 * @brief 获取一个可用的完成通道
 * @return 指向可用完成通道的指针
 *
 * 轮询所有可用的完成通道，返回一个可用的完成通道指针。
 */
ibv_comp_channel *RdmaContext::compChannel() {
    int index = (next_comp_channel_index_++) % num_comp_channel_;
    return comp_channel_[index];
}

/**
 * @brief 获取下一个完成向量索引
 * @return 下一个完成向量索引
 *
 * 轮询所有可用的完成向量，返回一个可用的完成向量索引。
 */
int RdmaContext::compVector() {
    return (next_comp_vector_index_++) % context_->num_comp_vectors;
}

static inline int ipv6_addr_v4mapped(const struct in6_addr *a) {
    return ((a->s6_addr32[0] | a->s6_addr32[1]) |
            (a->s6_addr32[2] ^ htonl(0x0000ffff))) == 0UL ||
           /* IPv4 encoded multicast addresses */
           (a->s6_addr32[0] == htonl(0xff0e0000) &&
            ((a->s6_addr32[1] | (a->s6_addr32[2] ^ htonl(0x0000ffff))) == 0UL));
}

/**
 * @brief 获取最佳GID索引
 * @param device_name 设备名称
 * @param context RDMA上下文
 * @param port_attr 端口属性
 * @param port RDMA端口号
 * @return 最佳GID索引，失败返回-1
 *
 * 遍历所有可能的GID索引，找到最佳的GID索引：
 * 1. 查询每个GID的属性
 * 2. 判断是否为IPv4映射地址
 * 3. 返回第一个IPv4映射的GID索引
 */
int RdmaContext::getBestGidIndex(const std::string &device_name,
                                 struct ibv_context *context,
                                 ibv_port_attr &port_attr, uint8_t port) {
    int gid_index = 0, i;
    union ibv_gid temp_gid, temp_gid_rival;
    int is_ipv4, is_ipv4_rival;

    if (ibv_query_gid(context, port, gid_index, &temp_gid)) {
        PLOG(ERROR) << "Failed to query GID " << gid_index << " on "
                    << device_name << "/" << port;
        return -1;
    }
    is_ipv4 = ipv6_addr_v4mapped((struct in6_addr *)temp_gid.raw);

    for (i = 1; i < port_attr.gid_tbl_len; i++) {
        if (ibv_query_gid(context, port, i, &temp_gid_rival)) {
            PLOG(ERROR) << "Failed to query GID " << i << " on " << device_name
                        << "/" << port;
            return -1;
        }
        is_ipv4_rival =
            ipv6_addr_v4mapped((struct in6_addr *)temp_gid_rival.raw);
        if (is_ipv4_rival && !is_ipv4) {
            gid_index = i;
            break;
        }
    }
    return gid_index;
}

/**
 * @brief 打开RDMA设备
 * @param device_name 设备名称
 * @param port RDMA端口号
 * @param gid_index GID索引
 * @return 成功返回0，失败返回错误码
 *
 * 打开并初始化RDMA设备：
 * 1. 获取设备列表
 * 2. 遍历设备，找到匹配的设备
 * 3. 打开设备并查询端口属性
 * 4. 更新全局配置
 * 5. 查询并保存GID
 */
int RdmaContext::openRdmaDevice(const std::string &device_name, uint8_t port,
                                int gid_index) {
    int num_devices = 0;
    struct ibv_context *context = nullptr;
    struct ibv_device **devices = ibv_get_device_list(&num_devices);
    if (!devices || num_devices <= 0) {
        LOG(ERROR) << "ibv_get_device_list failed";
        return ERR_DEVICE_NOT_FOUND;
    }

    for (int i = 0; i < num_devices; ++i) {
        if (device_name != ibv_get_device_name(devices[i])) continue;

        context = ibv_open_device(devices[i]);
        if (!context) {
            LOG(ERROR) << "ibv_open_device(" << device_name << ") failed";
            ibv_free_device_list(devices);
            return ERR_CONTEXT;
        }

        ibv_port_attr attr;
        int ret = ibv_query_port(context, port, &attr);
        if (ret) {
            PLOG(ERROR) << "Failed to query port " << port << " on "
                        << device_name;
            if (ibv_close_device(context)) {
                PLOG(ERROR) << "ibv_close_device(" << device_name << ") failed";
            }
            ibv_free_device_list(devices);
            return ERR_CONTEXT;
        }

        if (attr.state != IBV_PORT_ACTIVE) {
            LOG(WARNING) << "Device " << device_name << " port not active";
            if (ibv_close_device(context)) {
                PLOG(ERROR) << "ibv_close_device(" << device_name << ") failed";
            }
            ibv_free_device_list(devices);
            return ERR_CONTEXT;
        }

        ibv_device_attr device_attr;
        ret = ibv_query_device(context, &device_attr);
        if (ret) {
            PLOG(WARNING) << "Failed to query attributes on " << device_name;
            if (ibv_close_device(context)) {
                PLOG(ERROR) << "ibv_close_device(" << device_name << ") failed";
            }
            ibv_free_device_list(devices);
            return ERR_CONTEXT;
        }

        ibv_port_attr port_attr;
        ret = ibv_query_port(context, port, &port_attr);
        if (ret) {
            PLOG(WARNING) << "Failed to query port attributes on "
                          << device_name << "/" << port;
            if (ibv_close_device(context)) {
                PLOG(ERROR) << "ibv_close_device(" << device_name << ") failed";
            }
            ibv_free_device_list(devices);
            return ERR_CONTEXT;
        }

        updateGlobalConfig(device_attr);
        if (gid_index == 0) {
            int ret = getBestGidIndex(device_name, context, port_attr, port);
            if (ret >= 0) {
                LOG(INFO) << "Find best gid index: " << ret << " on "
                          << device_name << "/" << port;
                gid_index = ret;
            }
        }

        ret = ibv_query_gid(context, port, gid_index, &gid_);
        if (ret) {
            PLOG(ERROR) << "Failed to query GID " << gid_index << " on "
                        << device_name << "/" << port;
            if (ibv_close_device(context)) {
                PLOG(ERROR) << "ibv_close_device(" << device_name << ") failed";
            }
            ibv_free_device_list(devices);
            return ERR_CONTEXT;
        }

#ifndef CONFIG_SKIP_NULL_GID_CHECK
        if (isNullGid(&gid_)) {
            LOG(WARNING) << "GID is NULL, please check your GID index by "
                            "specifying MC_GID_INDEX";
            if (ibv_close_device(context)) {
                PLOG(ERROR) << "ibv_close_device(" << device_name << ") failed";
            }
            ibv_free_device_list(devices);
            return ERR_CONTEXT;
        }
#endif  // CONFIG_SKIP_NULL_GID_CHECK

        context_ = context;
        port_ = port;
        lid_ = attr.lid;
        active_mtu_ = attr.active_mtu;
        active_speed_ = attr.active_speed;
        gid_index_ = gid_index;

        ibv_free_device_list(devices);
        return 0;
    }

    ibv_free_device_list(devices);
    LOG(ERROR) << "No matched device found: " << device_name;
    return ERR_DEVICE_NOT_FOUND;
}

/**
 * @brief 将文件描述符添加到非阻塞的epoll事件列表
 * @param event_fd epoll事件文件描述符
 * @param data_fd 数据文件描述符
 * @return 成功返回0，失败返回错误码
 *
 * 将给定的文件描述符添加到epoll事件列表中，
 * 并设置为非阻塞模式。
 */
int RdmaContext::joinNonblockingPollList(int event_fd, int data_fd) {
    epoll_event event;
    memset(&event, 0, sizeof(epoll_event));

    int flags = fcntl(data_fd, F_GETFL, 0);
    if (flags == -1) {
        PLOG(ERROR) << "Failed to get file descriptor flags";
        return ERR_CONTEXT;
    }
    if (fcntl(data_fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        PLOG(ERROR) << "Failed to set file descriptor nonblocking";
        return ERR_CONTEXT;
    }

    event.events = EPOLLIN | EPOLLET;
    event.data.fd = data_fd;
    if (epoll_ctl(event_fd, EPOLL_CTL_ADD, event.data.fd, &event)) {
        PLOG(ERROR) << "Failed to register file descriptor to epoll";
        return ERR_CONTEXT;
    }

    return 0;
}


/**
 * @brief 从完成队列中轮询完成事件
 * @param num_entries 要求的完成事件数量
 * @param wc 存放完成事件的结构体指针
 * @param cq_index 完成队列索引
 * @return 实际轮询到的完成事件数量，失败返回错误码
 *
 * 从指定的完成队列中轮询完成事件，
 * 将结果存放在给定的wc数组中。
 */
int RdmaContext::poll(int num_entries, ibv_wc *wc, int cq_index) {
    int nr_poll = ibv_poll_cq(cq_list_[cq_index], num_entries, wc);
    if (nr_poll < 0) {
        LOG(ERROR) << "Failed to poll CQ " << cq_index << " of device "
                   << device_name_;
        return ERR_CONTEXT;
    }
    return nr_poll;
}

/**
 * @brief 提交发送请求
 * @param slice_list 待发送的数据切片列表
 * @return 成功返回0，失败返回错误码
 *
 * 将发送请求提交给工作线程池，
 * 由工作线程池负责实际的发送操作。
 */
int RdmaContext::submitPostSend(
    const std::vector<Transport::Slice *> &slice_list) {
    return worker_pool_->submitPostSend(slice_list);
}
}  // namespace mooncake