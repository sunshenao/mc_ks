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

#include "transport/rdma_transport/rdma_transport.h"

#include <glog/logging.h>
#include <sys/mman.h>
#include <sys/time.h>

#include <cassert>
#include <cstddef>
#include <future>
#include <set>

#include "common.h"
#include "config.h"
#include "topology.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_endpoint.h"

namespace mooncake {

/**
 * @brief RDMA传输协议实现类
 *
 * 该类实现了基于RDMA（远程直接内存访问）的高性能数据传输：
 * 1. 零拷贝传输：数据直接在内存间传输，不经过CPU
 * 2. 内核旁路：数据传输不需要系统调用
 * 3. 硬件卸载：数据传输由网卡硬件完成
 */
RdmaTransport::RdmaTransport() : next_segment_id_(1) {}


/**
 * @brief 析构函数，清理所有RDMA资源
 *
 * 清理的资源包括：
 * 1. 批次描述符集合
 * 2. 从元数据服务移除本地段
 * 3. 释放所有上下文资源
 */
RdmaTransport::~RdmaTransport() {
#ifdef CONFIG_USE_BATCH_DESC_SET
    for (auto &entry : batch_desc_set_) delete entry.second;
    batch_desc_set_.clear();
#endif
    metadata_->removeSegmentDesc(local_server_name_);
    batch_desc_set_.clear();
    context_list_.clear();
}

/**
 * @brief 安装RDMA传输协议
 *
 * @param local_server_name 本地服务器名称
 * @param meta 元数据服务实例
 * @param topo 网络拓扑信息
 * @return 成功返回0，失败返回错误码
 *
 * 该函数完成RDMA传输的初始化：
 * 1. 验证拓扑信息
 * 2. 初始化RDMA资源
 * 3. 分配本地段ID
 * 4. 启动握手守护进程
 * 5. 发布段信息到元数据服务
 */
int RdmaTransport::install(std::string &local_server_name,
                           std::shared_ptr<TransferMetadata> meta,
                           std::shared_ptr<Topology> topo) {
    // 验证拓扑信息
    if (topo == nullptr) {
        LOG(ERROR) << "RdmaTransport: missing topology";
        return ERR_INVALID_ARGUMENT;
    }

    // 保存基本信息
    metadata_ = meta;
    local_server_name_ = local_server_name;
    local_topology_ = topo;

    // 初始化RDMA资源
    auto ret = initializeRdmaResources();
    if (ret) {
        LOG(ERROR) << "RdmaTransport: cannot initialize RDMA resources";
        return ret;
    }

    // 分配本地段ID
    ret = allocateLocalSegmentID();
    if (ret) {
        LOG(ERROR) << "Transfer engine cannot be initialized: cannot "
                      "allocate local segment";
        return ret;
    }

    // 启动握手守护进程
    ret = startHandshakeDaemon(local_server_name);
    if (ret) {
        LOG(ERROR) << "RdmaTransport: cannot start handshake daemon";
        return ret;
    }

    // 发布段信息
    ret = metadata_->updateLocalSegmentDesc();
    if (ret) {
        LOG(ERROR) << "RdmaTransport: cannot publish segments";
        return ret;
    }

    return 0;
}

/**
 * @brief 注册本地内存区域
 *
 * @param addr 内存地址
 * @param length 内存长度
 * @param name 段名称
 * @param remote_accessible 是否允许远程访问
 * @param update_metadata 是否更新元数据
 * @return 成功返回0，失败返回错误码
 *
 * 该函数将内存区域注册到RDMA设备：
 * 1. 为每个RDMA上下文注册内存区域
 * 2. 获取本地和远程访问密钥
 * 3. 构建缓冲区描述符
 * 4. 可选地更新元数据服务
 */
int RdmaTransport::registerLocalMemory(void *addr, size_t length,
                                       const std::string &name,
                                       bool remote_accessible,
                                       bool update_metadata) {
    // 构建缓冲区描述符
    BufferDesc buffer_desc;
    buffer_desc.name = name;
    buffer_desc.addr = (uint64_t)addr;
    buffer_desc.length = length;

    // 设置内存区域访问权限
    const static int access_rights = IBV_ACCESS_LOCAL_WRITE |  // 本地写权限
                                     IBV_ACCESS_REMOTE_WRITE |  // 远程写权限
                                     IBV_ACCESS_REMOTE_READ;    // 远程读权限

    // 为每个RDMA上下文注册内存区域
    for (auto &context : context_list_) {
        int ret = context->registerMemoryRegion(addr, length, access_rights);
        if (ret) return ret;
        // 收集密钥信息
        buffer_desc.lkey.push_back(context->lkey(addr));  // 本地访问密钥
        buffer_desc.rkey.push_back(context->rkey(addr));  // 远程访问密钥
    }

    // 将缓冲区描述符添加到元数据服务
    int rc = metadata_->addLocalMemoryBuffer(buffer_desc, update_metadata);

    if (rc) return rc;

    return 0;
}

/**
 * @brief 注销本地内存区域
 *
 * @param addr 内存地址
 * @param update_metadata 是否更新元数据
 * @return 成功返回0，失败返回错误码
 *
 * 该函数从RDMA设备注销内存区域：
 * 1. 从元数据服务移除内存缓冲区
 * 2. 在每个RDMA上下文中注销内存区域
 */
int RdmaTransport::unregisterLocalMemory(void *addr, bool update_metadata) {
    // 从元数据服务移除内存缓冲区
    int rc = metadata_->removeLocalMemoryBuffer(addr, update_metadata);
    if (rc) return rc;

    // 在每个RDMA上下文中注销内存区域
    for (auto &context : context_list_) context->unregisterMemoryRegion(addr);

    return 0;
}

/**
 * @brief 分配本地段ID
 *
 * @return 成功返回0，失败返回错误码
 *
 * 该函数为本地段分配一个唯一的ID：
 * 1. 创建段描述符
 * 2. 填充段描述符信息，包括设备列表和拓扑信息
 * 3. 将段描述符添加到元数据服务
 */
int RdmaTransport::allocateLocalSegmentID() {
    // 创建段描述符
    auto desc = std::make_shared<SegmentDesc>();
    if (!desc) return ERR_MEMORY;
    desc->name = local_server_name_;
    desc->protocol = "rdma";
    // 填充设备信息
    for (auto &entry : context_list_) {
        TransferMetadata::DeviceDesc device_desc;
        device_desc.name = entry->deviceName();
        device_desc.lid = entry->lid();
        device_desc.gid = entry->gid();
        desc->devices.push_back(device_desc);
    }
    // 填充拓扑信息
    desc->topology = *(local_topology_.get());
    // 将段描述符添加到元数据服务
    metadata_->addLocalSegment(LOCAL_SEGMENT_ID, local_server_name_,
                               std::move(desc));
    return 0;
}

/**
 * @brief 批量注册本地内存
 *
 * @param buffer_list 待注册的缓冲区列表
 * @param location 段名称
 * @return 成功返回0，失败返回错误码
 *
 * 该函数用于批量注册内存区域，适用于大规模数据传输场景：
 * 1. 遍历缓冲区列表，异步注册每个缓冲区
 * 2. 等待所有注册任务完成
 * 3. 更新元数据服务中的段描述
 */
int RdmaTransport::registerLocalMemoryBatch(
    const std::vector<RdmaTransport::BufferEntry> &buffer_list,
    const std::string &location) {
    std::vector<std::future<int>> results;
    // 异步注册每个缓冲区
    for (auto &buffer : buffer_list) {
        results.emplace_back(
            std::async(std::launch::async, [this, buffer, location]() -> int {
                return registerLocalMemory(buffer.addr, buffer.length, location,
                                           true, false);
            }));
    }

    // 等待所有注册任务完成
    for (size_t i = 0; i < buffer_list.size(); ++i) {
        if (results[i].get()) {
            LOG(WARNING) << "RdmaTransport: Failed to register memory: addr "
                         << buffer_list[i].addr << " length "
                         << buffer_list[i].length;
        }
    }

    // 更新元数据服务中的段描述
    return metadata_->updateLocalSegmentDesc();
}

/**
 * @brief 批量注销本地内存
 *
 * @param addr_list 待注销的内存地址列表
 * @return 成功返回0，失败返回错误码
 *
 * 该函数用于批量注销内存区域：
 * 1. 遍历内存地址列表，异步注销每个内存区域
 * 2. 等待所有注销任务完成
 * 3. 更新元数据服务中的段描述
 */
int RdmaTransport::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    std::vector<std::future<int>> results;
    // 异步注销每个内存区域
    for (auto &addr : addr_list) {
        results.emplace_back(
            std::async(std::launch::async, [this, addr]() -> int {
                return unregisterLocalMemory(addr, false);
            }));
    }

    // 等待所有注销任务完成
    for (size_t i = 0; i < addr_list.size(); ++i) {
        if (results[i].get())
            LOG(WARNING) << "RdmaTransport: Failed to unregister memory: addr "
                         << addr_list[i];
    }

    // 更新元数据服务中的段描述
    return metadata_->updateLocalSegmentDesc();
}

/**
 * @brief 提交传输请求
 *
 * @param batch_id 批次ID
 * @param entries 传输请求列表
 * @return 成功提交的请求数量，失败返回错误码
 *
 * 处理传输请求的提交过程：
 * 1. 验证批次和请求的有效性
 * 2. 根据拓扑选择最优传输路径
 * 3. 为每个请求创建RDMA工作请求
 * 4. 提交到工作线程池处理
 *
 * 注意：
 * - 请求可能会被分割成多个RDMA操作
 * - 支持双向传输（读和写）
 * - 自动处理内存对齐
 */
int RdmaTransport::submitTransfer(BatchID batch_id,
                                  const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    // 检查批次容量
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "RdmaTransport: Exceed the limitation of current batch's "
                      "capacity";
        return ERR_TOO_MANY_REQUESTS;
    }

    std::unordered_map<std::shared_ptr<RdmaContext>, std::vector<Slice *>>
        slices_to_post;
    // 调整批次任务列表大小
    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());
    auto local_segment_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    const size_t kBlockSize = globalConfig().slice_size;  // 数据块大小
    const int kMaxRetryCount = globalConfig().retry_cnt;   // 最大重试次数

    // 遍历所有传输请求
    for (auto &request : entries) {
        TransferTask &task = batch_desc.task_list[task_id];
        ++task_id;
        // 将请求拆分为多个切片
        for (uint64_t offset = 0; offset < request.length;
             offset += kBlockSize) {
            auto slice = new Slice();
            slice->source_addr = (char *)request.source + offset;
            slice->length = std::min(request.length - offset, kBlockSize);
            slice->opcode = request.opcode;
            slice->rdma.dest_addr = request.target_offset + offset;
            slice->rdma.retry_cnt = 0;
            slice->rdma.max_retry_cnt = kMaxRetryCount;
            slice->task = &task;
            slice->target_id = request.target_id;
            slice->status = Slice::PENDING;

            int buffer_id = -1, device_id = -1, retry_cnt = 0;
            // 选择合适的设备
            while (retry_cnt < kMaxRetryCount) {
                if (selectDevice(local_segment_desc.get(),
                                 (uint64_t)slice->source_addr, slice->length,
                                 buffer_id, device_id, retry_cnt++))
                    continue;
                auto &context = context_list_[device_id];
                if (!context->active()) continue;
                // 设置RDMA传输参数
                slice->rdma.source_lkey =
                    local_segment_desc->buffers[buffer_id].lkey[device_id];
                slices_to_post[context].push_back(slice);
                task.total_bytes += slice->length;
                task.slices.push_back(slice);
                break;
            }
            // 设备选择失败
            if (device_id < 0) {
                LOG(ERROR)
                    << "RdmaTransport: Address not registered by any device(s) "
                    << slice->source_addr;
                return ERR_ADDRESS_NOT_REGISTERED;
            }
        }
    }
    // 提交RDMA发送请求
    for (auto &entry : slices_to_post)
        entry.first->submitPostSend(entry.second);
    return 0;
}

/**
 * @brief 提交传输任务
 *
 * @param request_list 传输请求指针列表
 * @param task_list 传输任务指针列表
 * @return 成功返回0，失败返回错误码
 *
 * 该函数用于提交一组传输任务：
 * 1. 遍历所有请求和任务
 * 2. 为每个请求分配切片并设置RDMA传输参数
 * 3. 提交RDMA发送请求
 */
int RdmaTransport::submitTransferTask(
    const std::vector<TransferRequest *> &request_list,
    const std::vector<TransferTask *> &task_list) {
    std::unordered_map<std::shared_ptr<RdmaContext>, std::vector<Slice *>>
        slices_to_post;
    auto local_segment_desc = metadata_->getSegmentDescByID(LOCAL_SEGMENT_ID);
    const size_t kBlockSize = globalConfig().slice_size;
    const int kMaxRetryCount = globalConfig().retry_cnt;
    // 遍历所有请求和任务
    for (size_t index = 0; index < request_list.size(); ++index) {
        auto &request = *request_list[index];
        auto &task = *task_list[index];
        // 将请求拆分为多个切片
        for (uint64_t offset = 0; offset < request.length;
             offset += kBlockSize) {
            auto slice = new Slice();
            slice->source_addr = (char *)request.source + offset;
            slice->length = std::min(request.length - offset, kBlockSize);
            slice->opcode = request.opcode;
            slice->rdma.dest_addr = request.target_offset + offset;
            slice->rdma.retry_cnt = 0;
            slice->rdma.max_retry_cnt = kMaxRetryCount;
            slice->task = &task;
            slice->target_id = request.target_id;
            slice->status = Slice::PENDING;

            int buffer_id = -1, device_id = -1, retry_cnt = 0;
            // 选择合适的设备
            while (retry_cnt < kMaxRetryCount) {
                if (selectDevice(local_segment_desc.get(),
                                 (uint64_t)slice->source_addr, slice->length,
                                 buffer_id, device_id, retry_cnt++))
                    continue;
                auto &context = context_list_[device_id];
                if (!context->active()) continue;
                // 设置RDMA传输参数
                slice->rdma.source_lkey =
                    local_segment_desc->buffers[buffer_id].lkey[device_id];
                slices_to_post[context].push_back(slice);
                task.total_bytes += slice->length;
                task.slices.push_back(slice);
                break;
            }
            // 设备选择失败
            if (device_id < 0) {
                LOG(ERROR)
                    << "RdmaTransport: Address not registered by any device(s) "
                    << slice->source_addr;
                return ERR_ADDRESS_NOT_REGISTERED;
            }
        }
    }
    // 提交RDMA发送请求
    for (auto &entry : slices_to_post)
        entry.first->submitPostSend(entry.second);
    return 0;
}


/**
 * @brief 获取传输状态
 *
 * @param batch_id 批次ID
 * @param status 状态列表（输出参数）
 * @return 成功返回0，失败返回错误码
 *
 * 该函数用于获取传输批次的状态：
 * 1. 遍历所有任务，更新每个任务的传输状态
 * 2. 根据切片的成功和失败数量，判断任务是否完成
 */
int RdmaTransport::getTransferStatus(BatchID batch_id,
                                     std::vector<TransferStatus> &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    status.resize(task_count);
    // 遍历所有任务
    for (size_t task_id = 0; task_id < task_count; task_id++) {
        auto &task = batch_desc.task_list[task_id];
        status[task_id].transferred_bytes = task.transferred_bytes;
        uint64_t success_slice_count = task.success_slice_count;
        uint64_t failed_slice_count = task.failed_slice_count;
        // 判断任务是否完成
        if (success_slice_count + failed_slice_count ==
            (uint64_t)task.slices.size()) {
            if (failed_slice_count)
                status[task_id].s = TransferStatusEnum::FAILED;
            else
                status[task_id].s = TransferStatusEnum::COMPLETED;
            task.is_finished = true;
        } else {
            status[task_id].s = TransferStatusEnum::WAITING;
        }
    }
    return 0;
}

/**
 * @brief 获取单个任务的传输状态
 *
 * @param batch_id 批次ID
 * @param task_id 任务ID
 * @param status 状态（输出参数）
 * @return 成功返回0，失败返回错误码
 *
 * 该函数用于获取单个传输任务的状态：
 * 1. 验证任务ID是否合法
 * 2. 更新任务的传输状态
 */
int RdmaTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                     TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    // 验证任务ID是否合法
    if (task_id >= task_count) return ERR_INVALID_ARGUMENT;
    auto &task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    // 判断任务是否完成
    if (success_slice_count + failed_slice_count ==
        (uint64_t)task.slices.size()) {
        if (failed_slice_count)
            status.s = TransferStatusEnum::FAILED;
        else
            status.s = TransferStatusEnum::COMPLETED;
        task.is_finished = true;
    } else {
        status.s = TransferStatusEnum::WAITING;
    }
    return 0;
}
/**
 * @brief 根据段名称获取段ID
 *
 * @param segment_name 段名称
 * @return 段ID
 */
RdmaTransport::SegmentID RdmaTransport::getSegmentID(
    const std::string &segment_name) {
    return metadata_->getSegmentID(segment_name);
}

/**
 * @brief 设置RDMA连接
 *
 * @param peer_desc 对端描述
 * @param local_desc 本地描述（输出参数）
 * @return 成功返回0，失败返回错误码
 *
 * 该函数用于设置RDMA连接：
 * 1. 根据对端NIC信息查找本地RDMA上下文
 * 2. 删除旧的端点（如果存在）
 * 3. 创建新的端点并设置连接参数
 */
int RdmaTransport::onSetupRdmaConnections(const HandShakeDesc &peer_desc,
                                          HandShakeDesc &local_desc) {
    // 从对端描述中获取本地NIC名称
    auto local_nic_name = getNicNameFromNicPath(peer_desc.peer_nic_path);
    if (local_nic_name.empty()) return ERR_INVALID_ARGUMENT;

    std::shared_ptr<RdmaContext> context;
    int index = 0;
     // 查找本地RDMA上下文
    for (auto &entry : local_topology_->getHcaList()) {
        if (entry == local_nic_name) {
            context = context_list_[index];
            break;
        }
        index++;
    }
    if (!context) return ERR_INVALID_ARGUMENT;

#ifdef CONFIG_ERDMA
// 删除旧的端点
    if (context->deleteEndpoint(peer_desc.local_nic_path)) return ERR_ENDPOINT;
#endif

    // 创建新的端点
    auto endpoint = context->endpoint(peer_desc.local_nic_path);
    if (!endpoint) return ERR_ENDPOINT;
    // 设置连接参数
    return endpoint->setupConnectionsByPassive(peer_desc, local_desc);
}

/**
 * @brief 初始化RDMA资源
 *
 * @return 成功返回0，失败返回错误码
 *
 * 该函数用于初始化RDMA资源：
 * 1. 遍历拓扑中的所有设备，创建RDMA上下文
 * 2. 为每个上下文分配CQ和Completion Channel
 * 3. 初始化完成队列和事件文件描述符
 */
int RdmaTransport::initializeRdmaResources() {
    // 检查拓扑是否为空
    if (local_topology_->empty()) {
        LOG(ERROR) << "RdmaTransport: No available RNIC";
        return ERR_DEVICE_NOT_FOUND;
    }

    std::vector<int> device_speed_list;
    // 遍历拓扑中的所有设备
    for (auto &device_name : local_topology_->getHcaList()) {
        auto context = std::make_shared<RdmaContext>(*this, device_name);
        if (!context) return ERR_MEMORY;

        auto &config = globalConfig();
        // 创建RDMA上下文
        int ret = context->construct(config.num_cq_per_ctx,
                                     config.num_comp_channels_per_ctx,
                                     config.port, config.gid_index,
                                     config.max_cqe, config.max_ep_per_ctx);
        if (ret) return ret;
        device_speed_list.push_back(context->activeSpeed());
        context_list_.push_back(context);
    }

    return 0;
}

/**
 * @brief 启动握手守护进程
 *
 * @param local_server_name 本地服务器名称
 * @return 成功返回0，失败返回错误码
 *
 * 该函数用于启动RDMA握手守护进程：
 * 1. 设置握手回调函数
 * 2. 启动元数据服务的握手守护线程
 */
int RdmaTransport::startHandshakeDaemon(std::string &local_server_name) {
    return metadata_->startHandshakeDaemon(
        std::bind(&RdmaTransport::onSetupRdmaConnections, this,
                  std::placeholders::_1, std::placeholders::_2),
        metadata_->localRpcMeta().rpc_port);
}

/**
 * @brief 选择合适的设备
 *
 * @param desc 段描述符
 * @param offset 偏移量
 * @param length 长度
 * @param buffer_id 缓冲区ID（输出参数）
 * @param device_id 设备ID（输出参数）
 * @param retry_count 重试次数
 * @return 成功返回0，失败返回错误码
 *
 * 该函数根据请求描述、偏移量和长度信息，找到合适的设备和缓冲区：
 * 1. 遍历段中的所有缓冲区
 * 2. 检查偏移量和长度是否在缓冲区范围内
 * 3. 根据拓扑信息选择合适的设备
 */
// According to the request desc, offset and length information, find proper
// buffer_id and device_id as output.
// Return 0 if successful, ERR_ADDRESS_NOT_REGISTERED otherwise.
int RdmaTransport::selectDevice(SegmentDesc *desc, uint64_t offset,
                                size_t length, int &buffer_id, int &device_id,
                                int retry_count) {
    // 遍历段中的所有缓冲区
    for (buffer_id = 0; buffer_id < (int)desc->buffers.size(); ++buffer_id) {
        auto &buffer_desc = desc->buffers[buffer_id];
        // 检查偏移量和长度是否在缓冲区范围内
        if (buffer_desc.addr > offset ||
            offset + length > buffer_desc.addr + buffer_desc.length)
            continue;
        // 根据拓扑信息选择合适的设备
        device_id = desc->topology.selectDevice(buffer_desc.name, retry_count);
        if (device_id >= 0) return 0;
    }

    return ERR_ADDRESS_NOT_REGISTERED;
}
}  // namespace mooncake
