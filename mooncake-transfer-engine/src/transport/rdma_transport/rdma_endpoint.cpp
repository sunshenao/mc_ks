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

#include "transport/rdma_transport/rdma_endpoint.h"

#include <glog/logging.h>

#include <cassert>
#include <cstddef>

#include "config.h"

namespace mooncake {
/**
 * @brief RDMA连接参数常量定义
 */

// Hop Limit 是 IPv6 中的一个字段，对应于 IPv4 中的 TTL（Time To Live） 字段。
// 它的作用是：
// 限制一个数据包在网络中可以经过的 最大路由跳数，防止数据包在网络中无限循环。
const static uint8_t MAX_HOP_LIMIT = 16;  // 最大跳数限制，用于路由
const static uint8_t TIMEOUT = 14;        // 超时时间，以4.096微秒为单位
const static uint8_t RETRY_CNT = 7;       // 重试次数，发生错误时重试

/**
 * @brief RDMA端点构造函数
 * @param context RDMA上下文引用
 *
 * 初始化端点的基本状态：
 * 1. 设置初始化状态
 * 2. 标记端点为活跃状态
 * 3. 关联RDMA上下文
 */
RdmaEndPoint::RdmaEndPoint(RdmaContext &context)
    : context_(context), status_(INITIALIZING), active_(true) {}

/**
 * @brief RDMA端点析构函数
 *
 * 清理端点资源：
 * 1. 检查队列对列表
 * 2. 如果存在队列对，调用deconstruct()清理
 */
RdmaEndPoint::~RdmaEndPoint() {
    if (!qp_list_.empty()) deconstruct();
}


/**
 * @brief 构造RDMA端点
 * @param cq 完成队列指针
 * RDMA 操作（如发送/接收）完成后，会在 CQ 中生成一个 完成事件（Completion Event），用于通知应用程序操作是否成功完成。
 * 
 * @param num_qp_list 队列对数量
 * 你要创建的 QP（Queue Pair）数量。
 * 
 * @param max_sge_per_wr 每个工作请求的最大分散/聚集元素数
 * 每个 工作请求（Work Request, WR） 中最多可以包含多少个 SGE。
 * SGE  描述内存缓冲区 一个 SGE 对应一块连续的缓冲区，多个 SGE 可以组合在一个 WR 中，从而实现 Scatter/Gather 功能。
 * 
 * @param max_wr_depth 最大工作请求深度
 * 队列中最多可以排队的 工作请求（WR）
 * 
 * @param max_inline_bytes 最大内联数据大小
 * 内联数据是指在发送工作请求时，直接将数据嵌入到工作请求中，而不是通过内存区域引用。
 * 
 * @return 成功返回0，失败返回错误码
 *
 * 该函数完成端点的初始化配置：
 * 1. 验证端点状态
 * 2. 分配队列对资源
 * 3. 初始化工作请求深度计数器
 * 4. 创建所需数量的队列对
 */
int RdmaEndPoint::construct(ibv_cq *cq, size_t num_qp_list,
                            size_t max_sge_per_wr, size_t max_wr_depth,
                            size_t max_inline_bytes) {
    // 验证端点状态
    if (status_.load(std::memory_order_relaxed) != INITIALIZING) {
        LOG(ERROR) << "Endpoint has already been constructed";
        return ERR_ENDPOINT;
    }
    // 初始化队列对列表
    qp_list_.resize(num_qp_list);

    // 设置最大工作请求深度
    max_wr_depth_ = (int)max_wr_depth;
    // 为每个队列对分配工作请求深度计数器
    wr_depth_list_ = new volatile int[num_qp_list];
    if (!wr_depth_list_) {
        LOG(ERROR) << "Failed to allocate memory for work request depth list";
        return ERR_MEMORY;
    }

    // 初始化每个队列对的工作请求深度
    for (size_t i = 0; i < num_qp_list; ++i) {
        wr_depth_list_[i] = 0;
        ibv_qp_init_attr attr; // 创建队列对
        memset(&attr, 0, sizeof(attr));
        attr.send_cq = cq;
        attr.recv_cq = cq;
        attr.sq_sig_all = false;
        attr.qp_type = IBV_QPT_RC;
        attr.cap.max_send_wr = attr.cap.max_recv_wr = max_wr_depth;
        attr.cap.max_send_sge = attr.cap.max_recv_sge = max_sge_per_wr;
        attr.cap.max_inline_data = max_inline_bytes;
        // 这里的 context_.pd() 表示从 context_ 这个对象中获取 保护域（Protection Domain, PD） 的指针，用于创建 QP
        qp_list_[i] = ibv_create_qp(context_.pd(), &attr); // 常见qp
        if (!qp_list_[i]) {
            PLOG(ERROR) << "Failed to create QP";
            return ERR_ENDPOINT;
        }
    }
    // 构造完成，状态从 INITIALIZING 变为 UNCONNECTED，表示尚未建立连接。
    status_.store(UNCONNECTED, std::memory_order_relaxed);
    return 0;
}
 /**
 * @brief 销毁RDMA端点
 * @return 成功返回0，失败返回错误码
 *
 * 该函数清理端点资源：
 * 1. 检查每个队列对的工作请求深度
 * 2. 销毁每个队列对
 * 3. 清空队列对列表和工作请求深度列表
 */
int RdmaEndPoint::deconstruct() {
    for (size_t i = 0; i < qp_list_.size(); ++i) {
        if (wr_depth_list_[i] != 0)
            LOG(WARNING)
                << "Outstanding work requests found, CQ will not be generated";

        if (ibv_destroy_qp(qp_list_[i])) {
            PLOG(ERROR) << "Failed to destroy QP";
            return ERR_ENDPOINT;
        }
    }
    qp_list_.clear();
    delete[] wr_depth_list_;
    return 0;
}

int RdmaEndPoint::destroyQP() { return deconstruct(); }

// 用于设置远程节点的网络接口路径 (peer_nic_path)。
// 在设置新路径之前，它会检查当前连接状态，并在必要时断开现有连接
void RdmaEndPoint::setPeerNicPath(const std::string &peer_nic_path) {
    RWSpinlock::WriteGuard guard(lock_);
    if (connected()) { // connected()就是判断status_是否等于CONNECTED
        LOG(WARNING) << "Previous connection will be discarded";
        disconnectUnlocked();
    }
    peer_nic_path_ = peer_nic_path;
}


// 用于主动建立 RDMA 连接。它通过握手过程获取远程节点的信息，并根据这些信息设置连接
int RdmaEndPoint::setupConnectionsByActive() {
    RWSpinlock::WriteGuard guard(lock_);
    if (connected()) { // 如果已经连接，则不需要重新建立连接
        LOG(INFO) << "Connection has been established";
        return 0;
    }
    // 创建本地握手描述符和远程握手描述符
    HandShakeDesc local_desc, peer_desc;
    local_desc.local_nic_path = context_.nicPath();
    local_desc.peer_nic_path = peer_nic_path_;
    local_desc.qp_num = qpNum();
    // 获取对端服务器名称和网卡名称
    auto peer_server_name = getServerNameFromNicPath(peer_nic_path_);
    auto peer_nic_name = getNicNameFromNicPath(peer_nic_path_);
    // 如果无法解析对端网卡路径，则返回错误
    if (peer_server_name.empty() || peer_nic_name.empty()) {
        LOG(ERROR) << "Parse peer nic path failed: " << peer_nic_path_;
        return ERR_INVALID_ARGUMENT;
    }
    // 发送本地数据，获取对端的握手描述符
    int rc = context_.engine().sendHandshake(peer_server_name, local_desc,
                                             peer_desc);
    if (rc) return rc;
    // 验证握手相应是否匹配
    if (peer_desc.local_nic_path != peer_nic_path_ ||
        peer_desc.peer_nic_path != local_desc.local_nic_path) {
        LOG(ERROR) << "Invalid argument: received packet mismatch";
        return ERR_REJECT_HANDSHAKE;
    }
    // 通过对端握手信息获得相应的segment描述符
    // 如果找到匹配的设备，调用 
    // doSetupConnection(nic.gid, nic.lid, peer_desc.qp_num) 
    // 使用设备的 GID 和 LID，以及从握手中获得的队列对编号来设置连接
    auto segment_desc =
        context_.engine().meta()->getSegmentDescByName(peer_server_name);
    if (segment_desc) {
        for (auto &nic : segment_desc->devices)
            if (nic.name == peer_nic_name)
                return doSetupConnection(nic.gid, nic.lid, peer_desc.qp_num);
    }
    LOG(ERROR) << "Peer NIC " << peer_nic_name << " not found in "
               << peer_server_name;
    return ERR_DEVICE_NOT_FOUND;
}
// 被动方接收到连接请求后进行处理。
int RdmaEndPoint::setupConnectionsByPassive(const HandShakeDesc &peer_desc,
                                            HandShakeDesc &local_desc) {
    RWSpinlock::WriteGuard guard(lock_);
    // 被动方接收到连接请求后进行处理。
    if (connected()) {
        LOG(WARNING) << "Re-establish connection: " << toString();
        disconnectUnlocked();
    }

    peer_nic_path_ = peer_desc.local_nic_path;
    if (peer_desc.peer_nic_path != context_.nicPath()) {
        local_desc.reply_msg = "Invalid argument: peer nic path inconsistency";
        LOG(ERROR) << local_desc.reply_msg;
        return ERR_REJECT_HANDSHAKE;
    }

    auto peer_server_name = getServerNameFromNicPath(peer_nic_path_);
    auto peer_nic_name = getNicNameFromNicPath(peer_nic_path_);
    if (peer_server_name.empty() || peer_nic_name.empty()) {
        local_desc.reply_msg = "Parse peer nic path failed: " + peer_nic_path_;
        LOG(ERROR) << local_desc.reply_msg;
        return ERR_INVALID_ARGUMENT;
    }

    local_desc.local_nic_path = context_.nicPath();
    local_desc.peer_nic_path = peer_nic_path_;
    local_desc.qp_num = qpNum();
    // 主动与被动的关系就是不同的，主动方发送握手，而被动方只从元数据服务器中获取
    auto segment_desc =
        context_.engine().meta()->getSegmentDescByName(peer_server_name);
    if (segment_desc) {
        for (auto &nic : segment_desc->devices)
            if (nic.name == peer_nic_name)
                return doSetupConnection(nic.gid, nic.lid, peer_desc.qp_num,
                                         &local_desc.reply_msg);
    }
    local_desc.reply_msg =
        "Peer nic not found in that server: " + peer_nic_path_;
    LOG(ERROR) << local_desc.reply_msg;
    return ERR_DEVICE_NOT_FOUND;
}

void RdmaEndPoint::disconnect() {
    RWSpinlock::WriteGuard guard(lock_);
    disconnectUnlocked();
}
// 断开连接的实现
// 该函数会重置所有队列对的状态，并清空远程NIC路径和工作请求深度列表。
// 注意：在调用此函数之前，必须确保没有未完成的工作请求。
// 如果有未完成的工作请求，函数会发出警告，并重置队列对的状态为 RESET。
// 这意味着所有未完成的工作请求将被丢弃，可能导致数据丢失或不一致。
void RdmaEndPoint::disconnectUnlocked() {
    for (size_t i = 0; i < qp_list_.size(); ++i) {
        if (wr_depth_list_[i] != 0)
            LOG(WARNING) << "Outstanding work requests will be dropped";
    }
    ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RESET;
    for (size_t i = 0; i < qp_list_.size(); ++i) {
        int ret = ibv_modify_qp(qp_list_[i], &attr, IBV_QP_STATE);
        if (ret) PLOG(ERROR) << "Failed to modity QP to RESET";
    }
    peer_nic_path_.clear();
    for (size_t i = 0; i < qp_list_.size(); ++i) wr_depth_list_[i] = 0;
    status_.store(UNCONNECTED, std::memory_order_release);
}

// 打印端点信息
const std::string RdmaEndPoint::toString() const {
    auto status = status_.load(std::memory_order_relaxed);
    if (status == CONNECTED)
        return "EndPoint: local " + context_.nicPath() + ", peer " +
               peer_nic_path_;
    else
        return "EndPoint: local " + context_.nicPath() + " (unconnected)";
}
// 判断是否还有未完成的任务
bool RdmaEndPoint::hasOutstandingSlice() const {
    if (active_) return true; // 如果端点处于活跃状态，表示存在任务正在运行
    for (size_t i = 0; i < qp_list_.size(); i++)
        if (wr_depth_list_[i] != 0) return true;
    return false;
}
// 提交发送请求
// 该函数将一组切片（Slice）提交到 RDMA 发送队列中。
// 它会随机选择一个队列对（QP）来处理这些切片。
// 如果队列对的工作请求深度不足以处理所有切片，则只处理部分切片。
// 成功提交的切片会从 slice_list 中移除，失败的切片会被添加到 failed_slice_list 中。
// 注意：
// - 每个切片必须包含源地址、长度和远程地址等信息。
// - 切片的状态会被更新为 POSTED，表示已提交到 RDMA 发送队列。
// - 如果提交失败，bad_wr 会指向第一个失败的工作请求（WR），
int RdmaEndPoint::submitPostSend(
    std::vector<Transport::Slice *> &slice_list,
    std::vector<Transport::Slice *> &failed_slice_list) {
    RWSpinlock::WriteGuard guard(lock_);

    // 随机选择一个队列对（QP）来处理发送请求，有利于负载均衡，并且简单高效
    int qp_index = SimpleRandom::Get().next(qp_list_.size());
    int wr_count = std::min(max_wr_depth_ - wr_depth_list_[qp_index],
                            (int)slice_list.size());
    if (wr_count == 0) return 0; // 没有足够的工作请求深度来处理切片，就返回0

    ibv_send_wr wr_list[wr_count], *bad_wr = nullptr;
    ibv_sge sge_list[wr_count];
    memset(wr_list, 0, sizeof(ibv_send_wr) * wr_count);
    for (int i = 0; i < wr_count; ++i) {
        auto slice = slice_list[i];
        auto &sge = sge_list[i];
        // 给sge赋值
        sge.addr = (uint64_t)slice->source_addr;
        sge.length = slice->length;
        sge.lkey = slice->rdma.source_lkey;
        // 给wr赋值
        // wr_id 是一个唯一标识符，用于区分不同的工作请求
        // opcode 指定了工作请求的类型（如 RDMA 读或写）
        // num_sge 指定了 SGE 的数量，这里每个 WR 只有一个 SGE
        // sg_list 指向 SGE 数组
        // send_flags 指定了发送标志，这里使用 IBV_SEND_SIGNALED
        // next 指向下一个工作请求，如果是最后一个工作请求则为 nullptr
        // imm_data 是立即数据，这里设置为 0
        // rdma.remote_addr 是远程地址，rdma.rkey 是远程密钥
        // rdma.qp_depth 是指向工作请求深度计数器的指针，
        // 用于跟踪当前队列对的工作请求深度
        // 注意：slice->opcode 必须是 Transport::TransferRequest::READ 或 WRITE
        //       否则会导致错误
        auto &wr = wr_list[i];
        wr.wr_id = (uint64_t)slice;
        wr.opcode = slice->opcode == Transport::TransferRequest::READ
                        ? IBV_WR_RDMA_READ
                        : IBV_WR_RDMA_WRITE;
        wr.num_sge = 1;
        wr.sg_list = &sge;
        wr.send_flags = IBV_SEND_SIGNALED;
        wr.next = (i + 1 == wr_count) ? nullptr : &wr_list[i + 1];
        wr.imm_data = 0;
        wr.wr.rdma.remote_addr = slice->rdma.dest_addr;
        wr.wr.rdma.rkey = slice->rdma.dest_rkey;
        slice->status = Transport::Slice::POSTED;
        slice->rdma.qp_depth = &wr_depth_list_[qp_index];
        // if (globalConfig().verbose)
        // {
        //     LOG(INFO) << "WR: local addr " << slice->source_addr
        //               << " remote addr " << slice->rdma.dest_addr
        //               << " rkey " << slice->rdma.dest_rkey;
        // }
    }

    // 使用原子操作更新该 QP 的 WR 使用计数。
    // 表示我们已经提交了 wr_count 个 WR。
    __sync_fetch_and_add(&wr_depth_list_[qp_index], wr_count);
    
    // 使用 ibv_post_send 提交 WR 列表到 QP
    int rc = ibv_post_send(qp_list_[qp_index], wr_list, &bad_wr);
    if (rc) {
        PLOG(ERROR) << "Failed to ibv_post_send";
        while (bad_wr) {
            int i = bad_wr - wr_list;
            failed_slice_list.push_back(slice_list[i]);
            __sync_fetch_and_sub(&wr_depth_list_[qp_index], 1);
            bad_wr = bad_wr->next;
        }
    }
    // 从原始列表中移除已提交的切片
    slice_list.erase(slice_list.begin(), slice_list.begin() + wr_count);
    return 0;
}

// 返回当前 RdmaEndPoint 实例中所有 QP（Queue Pair）的 qp_num 列表
// 该函数遍历 qp_list_，将每个 QP 的 qp_num 添加到返回的 vector 中
// 返回的 vector 包含所有 QP 的编号，方便后续操作或调试
// 注意：
// - qp_list_ 是一个存储所有 QP 的列表，每个 QP 都是一个指向 ibv_qp 结构的智能指针
// - qp_num 是 ibv_qp 结构中的一个成员，表示该 QP 的编号
// - 返回的 vector 是一个无符号整数类型的列表，包含所有 QP 的编号
std::vector<uint32_t> RdmaEndPoint::qpNum() const {
    std::vector<uint32_t> ret;
    for (int qp_index = 0; qp_index < (int)qp_list_.size(); ++qp_index)
        ret.push_back(qp_list_[qp_index]->qp_num);// 获得每个队列对的编号
    return ret;
}

// 用于设置 RDMA 连接的队列对 (Queue Pair, QP)。
// 该过程涉及多个状态转换，以便正确地配置和初始化 QP，从而实现与远程节点的通信
int RdmaEndPoint::doSetupConnection(const std::string &peer_gid,
                                    uint16_t peer_lid,
                                    std::vector<uint32_t> peer_qp_num_list,
                                    std::string *reply_msg) {

// peer_gid：远程节点的全局标识符 (GID)。
// peer_lid：远程节点的本地标识符 (LID)。
// peer_qp_num_list：远程节点的队列对编号列表。
// reply_msg：用于存储错误信息的字符串指针。
    if (qp_list_.size() != peer_qp_num_list.size()) { 
        // 检查本地和远程端点的 QP 数量是否匹配
        std::string message =
            "QP count mismatch in peer and local endpoints, check "
            "MC_MAX_EP_PER_CTX";
        LOG(ERROR) << "[Handshake] " << message;
        if (reply_msg) *reply_msg = message;
        return ERR_INVALID_ARGUMENT;
    }

    for (int qp_index = 0; qp_index < (int)qp_list_.size(); ++qp_index) {
        // 对每个队列对进行连接设置
        int ret = doSetupConnection(qp_index, peer_gid, peer_lid,
                                    peer_qp_num_list[qp_index], reply_msg);
        if (ret) return ret;
    }
    // 所有队列对都成功设置连接后，更新端点状态为 CONNECTED
    status_.store(CONNECTED, std::memory_order_relaxed);
    return 0;
}
// 这是对两个端点进行连接设置的具体实现。
int RdmaEndPoint::doSetupConnection(int qp_index, const std::string &peer_gid,
                                    uint16_t peer_lid, uint32_t peer_qp_num,
                                    std::string *reply_msg) {
    // RESET → INIT → RTR → RTS 状态转移顺序
    // qp_index：队列对的索引。
    // 检查是否错误
    if (qp_index < 0 || qp_index > (int)qp_list_.size())
        return ERR_INVALID_ARGUMENT;
    // 获取指定索引的队列对
    auto &qp = qp_list_[qp_index];

    // Any state -> RESET
    ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RESET;
    // 设置状态到IBV_QPS_RESET
    // 将 QP 设置为 RESET 状态，这是所有 QP 的初始状态。
    // 在这个状态下，QP 不能进行任何通信。
    // 目的是将 QP 复位，为后续配置做准备。
    int ret = ibv_modify_qp(qp, &attr, IBV_QP_STATE);
    if (ret) {
        std::string message = "Failed to modity QP to RESET";
        PLOG(ERROR) << "[Handshake] " << message;
        if (reply_msg) *reply_msg = message + ": " + strerror(errno);
        return ERR_ENDPOINT;
    }

    // RESET -> INIT
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;
    // 将 QP 设置为 INIT 状态。
    // 配置一些基本参数：
    attr.port_num = context_.portNum(); // 使用哪个物理端口
    attr.pkey_index = 0; //  分区密钥索引， 用于隔离 RDMA 通信 使用默认的网络分区
    // 大多数情况下，默认 PKey（索引 0）是全局可用的，确保与其他设备的兼容性。
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                           IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_ATOMIC;
    
//     qp_access_flags: 设置该 QP 允许的远程访问权限。
// IBV_ACCESS_LOCAL_WRITE: 本地可以写。
// IBV_ACCESS_REMOTE_READ: 允许远程读取本地内存。
// IBV_ACCESS_REMOTE_WRITE: 允许远程写入本地内存。
// IBV_ACCESS_REMOTE_ATOMIC: 允许远程原子操作
    
    ret = ibv_modify_qp(
        qp, &attr,
        IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
    if (ret) {
        std::string message =
            "Failed to modity QP to INIT, check local context port num";
        PLOG(ERROR) << "[Handshake] " << message;
        if (reply_msg) *reply_msg = message + ": " + strerror(errno);
        return ERR_ENDPOINT;
    }

    // INIT -> RTR
//     设置 QP 为 RTR 状态。
// 表示 QP 已准备好接收数据，但还不能发送数据。
// 配置 MTU（最大传输单元）：
// 取当前设备支持的 MTU 和全局配置的最小值。
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    attr.path_mtu = context_.activeMTU();
    if (globalConfig().mtu_length < attr.path_mtu)
        attr.path_mtu = globalConfig().mtu_length;
    ibv_gid peer_gid_raw;
    std::istringstream iss(peer_gid);
    for (int i = 0; i < 16; ++i) {
        int value;
        iss >> std::hex >> value;
        peer_gid_raw.raw[i] = static_cast<uint8_t>(value);
        if (i < 15) iss.ignore(1, ':');
    }
    // 反正就是设置各种参数，准备进行发送
    // 设置远程 GID
    attr.ah_attr.grh.dgid = peer_gid_raw;
    // TODO gidIndex and portNum must fetch from REMOTE
    attr.ah_attr.grh.sgid_index = context_.gidIndex();
    attr.ah_attr.grh.hop_limit = MAX_HOP_LIMIT;
    attr.ah_attr.dlid = peer_lid;
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.static_rate = 0;
    attr.ah_attr.is_global = 1;
    attr.ah_attr.port_num = context_.portNum();
    attr.dest_qp_num = peer_qp_num;
    attr.rq_psn = 0;
    attr.max_dest_rd_atomic = 16;
    attr.min_rnr_timer = 12;  // 12 in previous implementation
    ret = ibv_modify_qp(qp, &attr,
                        IBV_QP_STATE | IBV_QP_PATH_MTU | IBV_QP_MIN_RNR_TIMER |
                            IBV_QP_AV | IBV_QP_MAX_DEST_RD_ATOMIC |
                            IBV_QP_DEST_QPN | IBV_QP_RQ_PSN);
    if (ret) {
        std::string message =
            "Failed to modity QP to RTR, check mtu, gid, peer lid, peer qp num";
        PLOG(ERROR) << "[Handshake] " << message;
        if (reply_msg) *reply_msg = message + ": " + strerror(errno);
        return ERR_ENDPOINT;
    }

    // RTR -> RTS
//     设置 QP 为 RTS 状态。
// 表示 QP 已准备好发送和接收数据。
// timeout: 传输超时时间（单位：slot time）。
// retry_cnt: 重试次数（发送失败后最多重试几次）。
// rnr_retry: RNR（接收方未准备好）时的重试次数。
// sq_psn: 发送队列的初始 PSN。
// max_rd_atomic: 最大允许的远程原子操作数。
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = TIMEOUT;
    attr.retry_cnt = RETRY_CNT;
    attr.rnr_retry = 7;  // or 7,RNR error
    attr.sq_psn = 0;
    attr.max_rd_atomic = 16;
    ret = ibv_modify_qp(qp, &attr,
                        IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                            IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                            IBV_QP_MAX_QP_RD_ATOMIC);
    if (ret) {
        std::string message = "Failed to modity QP to RTS";
        PLOG(ERROR) << "[Handshake] " << message;
        if (reply_msg) *reply_msg = message + ": " + strerror(errno);
        return ERR_ENDPOINT;
    }

    return 0;
}
}  // namespace mooncake