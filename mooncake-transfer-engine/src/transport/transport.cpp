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

#include "transport/transport.h"

#include "error.h"
#include "transfer_engine.h"

namespace mooncake {

/**
 * @brief 分配新的批次ID
 * @param batch_size 批次中可容纳的最大任务数量
 * @return 成功返回批次ID，失败返回错误码
 *
 * 该函数用于创建新的传输批次：
 * 1. 分配批次描述符内存
 * 2. 初始化批次相关参数
 * 3. 预分配任务列表空间
 * 4. 将批次加入全局管理集合
 */
ransport::BatchID Transport::allocateBatchID(size_t batch_size) {
    // 创建新的批次描述符
    auto batch_desc = new BatchDesc();
    if (!batch_desc) return ERR_MEMORY;

    // 初始化批次参数
    batch_desc->id = BatchID(batch_desc);          // 使用描述符地址作为ID
    batch_desc->batch_size = batch_size;          // 设置最大任务数
    batch_desc->task_list.reserve(batch_size);    // 预分配任务列表空间
    batch_desc->context = NULL;                   // 初始化上下文为空

    // 如果启用了批次描述符集合管理
#ifdef CONFIG_USE_BATCH_DESC_SET
    // 将新批次加入全局管理集合
    batch_desc_lock_.lock();                     // 加锁保护并发访问
    batch_desc_set_[batch_desc->id] = batch_desc; // 添加到映射表
    batch_desc_lock_.unlock();                   // 释放锁
#endif

    return batch_desc->id;                       // 返回批次ID
}
/**
 * @brief 释放批次ID及其关联资源
 * @param batch_id 要释放的批次ID
 * @return 成功返回0，失败返回错误码
 *
 * 该函数在批次完成后清理资源：
 * 1. 检查所有任务是否完成
 * 2. 从全局管理集合中移除
 * 3. 释放批次描述符内存
 * 首先写完补充材料的函，然后我就要去找人盖章
 */
int Transport::freeBatchID(BatchID batch_id) {
    auto &batch_desc = *((BatchDesc *)(batch_id));
    const size_t task_count = batch_desc.task_list.size();
    for (size_t task_id = 0; task_id < task_count; task_id++) {
        // 如果存在未完成的任务，返回错误
        if (!batch_desc.task_list[task_id].is_finished) {
            LOG(ERROR) << "BatchID cannot be freed until all tasks are done";
            return ERR_BATCH_BUSY;
        }
    }
    /**
     * @brief 删除批次描述符并清理相关资源
     */
    delete &batch_desc;
#ifdef CONFIG_USE_BATCH_DESC_SET
    // 从全局管理集合中移除批次
    RWSpinlock::WriteGuard guard(batch_desc_lock_);  // 使用读写锁保护并发访问
    batch_desc_set_.erase(batch_id);                // 从映射表中删除
#endif
    return 0;
}
/**
 * @brief 安装传输层组件
 * @param local_server_name 本地服务器名称
 * @param meta 元数据服务实例
 * @param topo 网络拓扑管理器实例
 * @return 成功返回0，失败返回错误码
 *
 * 该函数完成传输层的基础初始化：
 * 1. 设置本地服务器标识
 * 2. 关联元数据服务实例
 * 3. 准备网络拓扑环境
 *
 * 注意：具体的传输协议实现（如RDMA、TCP）可能会重写此方法，
 * 添加协议特定的初始化逻辑。
 */
int Transport::install(std::string &local_server_name,
                       std::shared_ptr<TransferMetadata> meta,
                       std::shared_ptr<Topology> topo) {
    local_server_name_ = local_server_name;
    metadata_ = meta;
    return 0;
}
}  // namespace mooncake