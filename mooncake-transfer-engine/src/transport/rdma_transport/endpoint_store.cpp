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

#include "transport/rdma_transport/endpoint_store.h"

#include <glog/logging.h>

#include <atomic>
#include <cassert>
#include <cstddef>
#include <memory>
#include <utility>

#include "config.h"
#include "transport/rdma_transport/rdma_context.h"
#include "transport/rdma_transport/rdma_endpoint.h"

namespace mooncake {

/**
 * @brief 从端点存储中获取RDMA端点
 * @param peer_nic_path 对端网卡路径
 * @return 端点的共享指针，如果不存在返回nullptr
 *
 * 该函数从FIFO缓存中查找端点：
 * 1. 使用读锁保护并发访问
 * 2. 在映射表中查找对端路径
 * 3. 返回找到的端点或nullptr
 */
// 这个端点存储器，为FIFO缓存策略的端点存储器
std::shared_ptr<RdmaEndPoint> FIFOEndpointStore::getEndpoint(
    std::string peer_nic_path) {
    RWSpinlock::ReadGuard guard(endpoint_map_lock_);
    auto iter = endpoint_map_.find(peer_nic_path);
    if (iter != endpoint_map_.end()) return iter->second;
    return nullptr;
}

/**
 * @brief 向端点存储中插入新的RDMA端点
 * @param peer_nic_path 对端网卡路径
 * @param context RDMA上下文指针
 * @return 新创建的端点或已存在的端点
 *
 * 该函数处理端点的创建和插入：
 * 1. 检查端点是否已存在
 * 2. 创建新端点并初始化
 * 3. 如果缓存已满，触发淘汰
 * 4. 将新端点加入FIFO队列
 */
std::shared_ptr<RdmaEndPoint> FIFOEndpointStore::insertEndpoint(
    std::string peer_nic_path, RdmaContext *context) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    // 检查端点是否已存在
    if (endpoint_map_.find(peer_nic_path) != endpoint_map_.end()) {
        LOG(INFO) << "Endpoint " << peer_nic_path
                  << " already exists in FIFOEndpointStore";
        return endpoint_map_[peer_nic_path];
    }
    // 创建新的端点
    auto endpoint = std::make_shared<RdmaEndPoint>(*context);
    if (!endpoint) {
        LOG(ERROR) << "Failed to allocate memory for RdmaEndPoint";
        return nullptr;
    }

    // 配置端点参数
    auto &config = globalConfig();
    int ret =
        endpoint->construct(context->cq(), config.num_qp_per_ep, config.max_sge,
                            config.max_wr, config.max_inline);
    if (ret) return nullptr;
    // 如果缓存已满，触发淘汰
    while (this->getSize() >= max_size_) evictEndpoint();
    // 设置对端路径
    endpoint->setPeerNicPath(peer_nic_path);

    // 将新端点加入FIFO队列和映射表
    endpoint_map_[peer_nic_path] = endpoint;
    fifo_list_.push_back(peer_nic_path);
    auto it = fifo_list_.end();
    fifo_map_[peer_nic_path] = --it;
    return endpoint;
}
/**
 * @brief 从端点存储中删除RDMA端点
 * @param peer_nic_path 对端网卡路径
 * @return 成功返回0，失败返回错误码
 *
 * 该函数处理端点的删除：
 * 1. 从映射表中移除端点
 * 2. 从FIFO队列中移除
 * 3. 清理端点资源
 */
int FIFOEndpointStore::deleteEndpoint(std::string peer_nic_path) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    // 从映射表中查找端点
    auto iter = endpoint_map_.find(peer_nic_path);
    if (iter != endpoint_map_.end()) {
        // 找到后，就进行删除操作
        waiting_list_.insert(iter->second);
        iter->second->set_active(false);
        endpoint_map_.erase(iter);
        auto fifo_iter = fifo_map_[peer_nic_path];
        fifo_list_.erase(fifo_iter);
        fifo_map_.erase(peer_nic_path);
    }
    return 0;
}

/**
 * @brief 淘汰最早插入的端点
 *
 * FIFO淘汰策略：
 * 1. 从队列头部获取最早的端点
 * 2. 检查端点是否可以被淘汰
 * 3. 删除端点并清理资源
 */
void FIFOEndpointStore::evictEndpoint() {
    if (fifo_list_.empty()) return;
    std::string victim = fifo_list_.front();
    fifo_list_.pop_front();
    fifo_map_.erase(victim);
    LOG(INFO) << victim << " evicted";
    waiting_list_.insert(endpoint_map_[victim]);
    endpoint_map_.erase(victim);
    return;
}

/**
 * @brief 回收已完成传输的端点
 *
 * 检查等待列表中的端点：
 * 1. 遍历所有等待回收的端点
 * 2. 对于没有未完成传输的端点进行清理
 * 3. 从等待列表中移除已处理的端点
 */
void FIFOEndpointStore::reclaimEndpoint() {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    std::vector<std::shared_ptr<RdmaEndPoint>> to_delete;
    for (auto &endpoint : waiting_list_)
    // // 判断是否还有未完成的任务
        if (!endpoint->hasOutstandingSlice()) to_delete.push_back(endpoint);
    for (auto &endpoint : to_delete) waiting_list_.erase(endpoint);
}

size_t FIFOEndpointStore::getSize() { return endpoint_map_.size(); }

/**
 * @brief 销毁所有端点的队列对
 * @return 成功返回0，失败返回错误码
 *
 * 该函数在关闭时清理所有资源：
 * 1. 遍历所有端点
 * 2. 销毁每个端点的队列对
 * 3. 清空存储结构
 */
int FIFOEndpointStore::destroyQPs() {
    for (auto &kv : endpoint_map_) {
        kv.second->destroyQP();
    }
    return 0;
}

/**
 * @brief 从端点存储中获取RDMA端点
 * @param peer_nic_path 对端网卡路径
 * @return 端点的共享指针，如果不存在返回nullptr
 *
 * 该函数从SIEVEE缓存中查找端点：
 * 1. 使用读锁保护并发访问
 * 2. 在映射表中查找对端路径
 * 3. 返回找到的端点或nullptr
 */
std::shared_ptr<RdmaEndPoint> SIEVEEndpointStore::getEndpoint(
    std::string peer_nic_path) {
    RWSpinlock::ReadGuard guard(endpoint_map_lock_);
    auto iter = endpoint_map_.find(peer_nic_path);
    if (iter != endpoint_map_.end()) {
        iter->second.second.store( // 找的节点设置为1
            true, std::memory_order_relaxed);  // This is safe within read lock
                                               // because of idempotence
        return iter->second.first;
    }
    // 将 iter->second.second 中的布尔值设置为 true，
    // 使用 std::memory_order_relaxed 内存序。
    // 这种操作是安全的，因为在读锁下，多个线程可以安全地进行这种无序的写入。
    // LOG(INFO) << "Endpoint " << peer_nic_path << " not found in
    // SIEVEEndpointStore";
    return nullptr;
}
// 按照算法，端点还是会被插入到首节点之中
std::shared_ptr<RdmaEndPoint> SIEVEEndpointStore::insertEndpoint(
    std::string peer_nic_path, RdmaContext *context) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    // 检查端点是否已存在
    if (endpoint_map_.find(peer_nic_path) != endpoint_map_.end()) {
        LOG(INFO) << "Endpoint " << peer_nic_path
                  << " already exists in SIEVEEndpointStore";
        return endpoint_map_[peer_nic_path].first;
    }
    auto endpoint = std::make_shared<RdmaEndPoint>(*context);
    if (!endpoint) {
        LOG(ERROR) << "Failed to allocate memory for RdmaEndPoint";
        return nullptr;
    }
    auto &config = globalConfig();
    int ret =
        endpoint->construct(context->cq(), config.num_qp_per_ep, config.max_sge,
                            config.max_wr, config.max_inline);
    if (ret) return nullptr;

    while (this->getSize() >= max_size_) evictEndpoint();

    endpoint->setPeerNicPath(peer_nic_path);
    endpoint_map_[peer_nic_path] = std::make_pair(endpoint, false);
    fifo_list_.push_front(peer_nic_path);
    fifo_map_[peer_nic_path] = fifo_list_.begin();
    return endpoint;
}

int SIEVEEndpointStore::deleteEndpoint(std::string peer_nic_path) {
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    auto iter = endpoint_map_.find(peer_nic_path);
    if (iter != endpoint_map_.end()) {
        waiting_list_len_++;
        
        iter->second.first->set_active(false);
        waiting_list_.insert(iter->second.first);
        endpoint_map_.erase(iter);

        auto fifo_iter = fifo_map_[peer_nic_path];
        if (hand_.has_value() && hand_.value() == fifo_iter) {
            fifo_iter == fifo_list_.begin() ? hand_ = std::nullopt
                                            : hand_ = std::prev(fifo_iter);
        }
        fifo_list_.erase(fifo_iter);
        fifo_map_.erase(peer_nic_path);
    }
    return 0;
}
// 进行SIEVE缓存淘汰
// 1. 从FIFO列表中获取当前手指针位置
// 2. 如果手指针无效，则设置为列表末尾
// 3. 遍历列表，找到第一个未被访问的端点
// 4. 将该端点标记为未访问，并更新手指针位置
// 5. 从FIFO列表和映射表中删除该端点
// 6. 设置端点为非活动状态，并将其添加到等待列表中
// 7. 更新等待列表长度
// 8. 删除端点映射表中的该端点
void SIEVEEndpointStore::evictEndpoint() {
    if (fifo_list_.empty()) {
        return;
    }
    auto o = hand_.has_value() ? hand_.value() : --fifo_list_.end();
    std::string victim;
    while (true) {
        victim = *o;
        // endpoint_map_[victim].second.load(std::memory_order_relaxed)
        // 这个是用来获取访问位是否为true，是否被访问过
        if (endpoint_map_[victim].second.load(std::memory_order_relaxed)) {
            // 被访问过就将其设置为未访问
            endpoint_map_[victim].second.store(false,
                                               std::memory_order_relaxed);
            o = (o == fifo_list_.begin() ? --fifo_list_.end() : std::prev(o));
        } else {// 找到第一个没有被访问过的端点，直接跳出循环
            break;
        }
    }
    // 将hand_指针更新为当前的o的前位置，因为o需要被清除
    hand_ = (o == fifo_list_.begin() ? --fifo_list_.end() : std::prev(o));
    fifo_list_.erase(o);
    fifo_map_.erase(victim);
    LOG(INFO) << victim << " evicted";
    auto victim_instance = endpoint_map_[victim].first;
    victim_instance->set_active(false);
    waiting_list_len_++;
    waiting_list_.insert(victim_instance);
    endpoint_map_.erase(victim);
    return;
}

// 回收已经完成的节点，在waiting_list_查找即可
void SIEVEEndpointStore::reclaimEndpoint() {
    if (waiting_list_len_.load(std::memory_order_relaxed) == 0) return;
    RWSpinlock::WriteGuard guard(endpoint_map_lock_);
    std::vector<std::shared_ptr<RdmaEndPoint>> to_delete;
    for (auto &endpoint : waiting_list_)
        if (!endpoint->hasOutstandingSlice()) to_delete.push_back(endpoint);
    for (auto &endpoint : to_delete) waiting_list_.erase(endpoint);
    waiting_list_len_ -= to_delete.size();
}
// 销毁所有端点的队列对
int SIEVEEndpointStore::destroyQPs() {
    for (auto &endpoint : waiting_list_) endpoint->destroyQP();
    for (auto &kv : endpoint_map_) kv.second.first->destroyQP();
    return 0;
}

size_t SIEVEEndpointStore::getSize() { return endpoint_map_.size(); }
}  // namespace mooncake