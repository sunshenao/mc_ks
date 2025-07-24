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

#include "multi_transport.h"

#include "transport/rdma_transport/rdma_transport.h"
#include "transport/tcp_transport/tcp_transport.h"
#include "transport/transport.h"
#ifdef USE_CUDA
#include "transport/nvmeof_transport/nvmeof_transport.h"
#endif

namespace mooncake {
/**
 * @brief 构造多协议传输管理器
 *
 * @param metadata 元数据服务实例
 * @param local_server_name 本地服务器名称
 *
 * 初始化管理器的基本组件：
 * - 保存元数据服务引用
 * - 记录本地服务器标识
 * - 初始化内部数据结构
 */
MultiTransport::MultiTransport(std::shared_ptr<TransferMetadata> metadata,
                               std::string &local_server_name)
    : metadata_(metadata), local_server_name_(local_server_name) {
    // ...
}

MultiTransport::~MultiTransport() {
    // ...
}

/**
 * @brief 分配新的批次ID
 *
 * @param batch_size 批次大小（最大任务数）
 * @return 批次ID，失败返回ERR_MEMORY
 *
 * 该函数创建新的批次：
 * 1. 分配批次描述符
 * 2. 初始化任务列表
 * 3. 注册到批次集合
 */
MultiTransport::BatchID MultiTransport::allocateBatchID(size_t batch_size) {
    auto batch_desc = new BatchDesc();
    if (!batch_desc) return ERR_MEMORY;
    batch_desc->id = BatchID(batch_desc);
    batch_desc->batch_size = batch_size;
    batch_desc->task_list.reserve(batch_size);
    batch_desc->context = NULL;
#ifdef CONFIG_USE_BATCH_DESC_SET
    batch_desc_lock_.lock();
    batch_desc_set_[batch_desc->id] = batch_desc;
    batch_desc_lock_.unlock();
#endif
    return batch_desc->id;
}

/**
 * @brief 释放批次ID
 *
 * @param batch_id 要释放的批次ID
 * @return 成功返回0，失败返回错误码
 *
 * 该函数完成批次的清理工作：
 * 1. 检查所有任务是否完成
 * 2. 释放批次描述符
 * 3. 从批次集合中移除
 */
int MultiTransport::freeBatchID(BatchID batch_id) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    for (size_t task_id = 0; task_id < task_count; task_id++) {
        if (!batch_desc.task_list[task_id].is_finished) {
            LOG(ERROR) << "BatchID cannot be freed until all tasks are done";
            return ERR_BATCH_BUSY;
        }
    }
    delete &batch_desc;
#ifdef CONFIG_USE_BATCH_DESC_SET
    RWSpinlock::WriteGuard guard(batch_desc_lock_);
    batch_desc_set_.erase(batch_id);
#endif
    return 0;
}

/**
 * @brief 提交传输请求
 *
 * @param batch_id 批次ID
 * @param entries 传输请求列表
 * @return 成功返回0，失败返回错误码
 *
 * 该函数处理传输请求的提交：
 * 1. 验证批次容量
 * 2. 为每个请求选择传输协议
 * 3. 按协议分组请求
 * 4. 分发到各个传输协议处理
 */
int MultiTransport::submitTransfer(
    BatchID batch_id, const std::vector<TransferRequest> &entries) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    if (batch_desc.task_list.size() + entries.size() > batch_desc.batch_size) {
        LOG(ERROR) << "MultiTransport: Exceed the limitation of batch capacity";
        return ERR_TOO_MANY_REQUESTS;
    }

    // 分配任务ID并准备任务列表
    size_t task_id = batch_desc.task_list.size();
    batch_desc.task_list.resize(task_id + entries.size());
    // 按传输协议分组的请求和任务
    struct SubmitTasks {
        std::vector<TransferRequest *> request_list;
        std::vector<Transport::TransferTask *> task_list;
    };
    std::unordered_map<Transport *, SubmitTasks> submit_tasks;
    // 为每个请求选择传输协议并分组
    for (auto &request : entries) {
        auto transport = selectTransport(request);
        if (!transport) return ERR_INVALID_ARGUMENT;
        auto &task = batch_desc.task_list[task_id];
        ++task_id;
        // 将请求和任务添加到对应协议的列表
        submit_tasks[transport].request_list.push_back(
            (TransferRequest *)&request);
        submit_tasks[transport].task_list.push_back(&task);
    }
    // 分发请求到各个传输协议
    for (auto &entry : submit_tasks) { // entry.first is Transport*
        // 为每个传输协议提交任务,然后就会更具传入的结果进行数据传输
        int ret = entry.first->submitTransferTask(entry.second.request_list,
                                                  entry.second.task_list);
        if (ret) {
            LOG(ERROR) << "MultiTransport: Failed to submit transfer task to "
                       << entry.first->getName();
            return ret;
        }
    }
    return 0;
}

/**
 * @brief 获取传输任务的状态
 *
 * @param batch_id 批次ID
 * @param task_id 任务ID
 * @param status 状态输出参数
 * @return 成功返回0，失败返回错误码
 *
 * 该函数检查传输任务的完成情况：
 * 1. 统计成功和失败的切片数量
 * 2. 计算已传输的字节数
 * 3. 判断任务状态（等待/完成/失败）
 */
int MultiTransport::getTransferStatus(BatchID batch_id, size_t task_id,
                                      TransferStatus &status) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    if (task_id >= task_count) return ERR_INVALID_ARGUMENT;
    auto &task = batch_desc.task_list[task_id];
    status.transferred_bytes = task.transferred_bytes;
    uint64_t success_slice_count = task.success_slice_count;
    uint64_t failed_slice_count = task.failed_slice_count;
    if (success_slice_count + failed_slice_count ==
        (uint64_t)task.slices.size()) {
        if (failed_slice_count) {
            status.s = Transport::TransferStatusEnum::FAILED;
        } else {
            status.s = Transport::TransferStatusEnum::COMPLETED;
        }
        task.is_finished = true;
    } else {
        status.s = Transport::TransferStatusEnum::WAITING;
    }
    return 0;
}

/**
 * @brief 安装传输协议
 *
 * @param proto 协议名称（如"rdma"、"tcp"等）
 * @param topo 网络拓扑信息
 * @return 传输协议实例指针，失败返回nullptr
 *
 * 该函数完成传输协议的安装：
 * 1. 根据协议名称创建实例
 * 2. 初始化协议实例
 * 3. 注册到协议映射表
 */
Transport *MultiTransport::installTransport(const std::string &proto,
                                            std::shared_ptr<Topology> topo) {
    Transport *transport = nullptr;
    if (std::string(proto) == "rdma") {
        transport = new RdmaTransport();
    } else if (std::string(proto) == "tcp") {
        transport = new TcpTransport();
    }
#ifdef USE_CUDA
    else if (std::string(proto) == "nvmeof") {
        transport = new NVMeoFTransport();
    }
#endif

    if (!transport) {
        LOG(ERROR) << "MultiTransport: Failed to initialize transport "
                   << proto;
        return nullptr;
    }

    if (transport->install(local_server_name_, metadata_, topo)) {
        return nullptr;
    }

    transport_map_[proto] = std::shared_ptr<Transport>(transport);
    return transport;
}


/**
 * @brief 为传输请求选择合适的传输协议
 *
 * @param entry 传输请求
 * @return 传输协议实例指针，失败返回nullptr
 *
 * 该函数根据以下规则选择协议：
 * 1. 本地传输优先使用local协议
 * 2. 根据目标段的协议类型选择
 * 3. 确保所需协议已安装
 */
Transport *MultiTransport::selectTransport(const TransferRequest &entry) {
     // 本地传输检查
    if (entry.target_id == LOCAL_SEGMENT_ID && transport_map_.count("local"))
        return transport_map_["local"].get();
    // 获取目标段信息
    auto target_segment_desc = metadata_->getSegmentDescByID(entry.target_id);
    if (!target_segment_desc) {
        LOG(ERROR) << "MultiTransport: Incorrect target segment id "
                   << entry.target_id;
        return nullptr;
    }
    // 获取目标段使用的协议
    auto proto = target_segment_desc->protocol;
    if (!transport_map_.count(proto)) {
        LOG(ERROR) << "MultiTransport: Transport " << proto << " not installed";
        return nullptr;
    }
    return transport_map_[proto].get();
}

/**
 * @brief 获取指定协议的实例
 *
 * @param proto 协议名称
 * @return 传输协议实例指针，未找到返回nullptr
 */
Transport *MultiTransport::getTransport(const std::string &proto) {
    if (!transport_map_.count(proto)) return nullptr;
    return transport_map_[proto].get();
}

/**
 * @brief 列出所有已安装的传输协议
 *
 * @return 传输协议实例指针列表
 */
std::vector<Transport *> MultiTransport::listTransports() {
    std::vector<Transport *> transport_list;
    for (auto &entry : transport_map_)
        transport_list.push_back(entry.second.get());
    return transport_list;
}

}  // namespace mooncake