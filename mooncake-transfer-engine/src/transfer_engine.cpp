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

#include "transfer_engine.h"

#include "transport/transport.h"

namespace mooncake {

    /**
 * @brief 初始化传输引擎
 *
 * @param metadata_conn_string 元数据服务连接字符串，如 "http://localhost:2379"
 * @param local_server_name 本地服务器名称，用于标识节点
 * @param ip_or_host_name 本机IP或主机名
 * @param rpc_port RPC服务端口号
 * @return 成功返回0，失败返回错误码
 *
 * 该函数完成传输引擎的初始化工作：
 * 1. 创建元数据服务实例
 * 2. 创建多协议传输管理器
 * 3. 注册本地RPC服务
 * 4. 自动发现网络拓扑（如果启用）
 * 5. 自动安装传输协议
 */
int TransferEngine::init(const std::string &metadata_conn_string,
                         const std::string &local_server_name,
                         const std::string &ip_or_host_name,
                         uint64_t rpc_port) {
    // 保存本地服务器名称
    local_server_name_ = local_server_name;
    // 创建元数据服务实例
    metadata_ = std::make_shared<TransferMetadata>(metadata_conn_string);

    // 创建多协议传输管理器
    multi_transports_ =
        std::make_shared<MultiTransport>(metadata_, local_server_name_);
    // 注册本地RPC服务信息到元数据服务    
    TransferMetadata::RpcMetaDesc desc;
    desc.ip_or_host_name = ip_or_host_name;
    desc.rpc_port = rpc_port;
    int ret = metadata_->addRpcMetaEntry(local_server_name_, desc);
    if (ret) return ret;

    // 如果启用了自动发现功能
    if (auto_discover_) {
        // 自动发现网络拓扑
        local_topology_->discover();

        // 如果发现了RDMA网卡（HCA）
        if (local_topology_->getHcaList().size() > 0) {
            // 安装RDMA传输协议
            multi_transports_->installTransport("rdma", local_topology_);
        } else {
            // 没有RDMA网卡，退回到TCP传输
            multi_transports_->installTransport("tcp", nullptr);
        }
        // TODO: 自动安装其他传输协议
    }

    return 0;
}
/**
 * @brief 释放传输引擎资源
 *
 * @return 成功返回0，失败返回错误码
 *
 * 该函数清理传输引擎使用的资源：
 * 1. 从元数据服务注销RPC条目
 * 2. 释放元数据服务实例
 * 3. 清理其他相关资源
 */
int TransferEngine::freeEngine() {
    if (metadata_) {
        metadata_->removeRpcMetaEntry(local_server_name_);
        metadata_.reset();
    }
    return 0;
}

// Only for testing
/**
 * @brief 安装传输协议（仅用于测试）
 *
 * @param proto 协议名称（如"rdma"、"tcp"等）
 * @param args 协议参数数组，args[0]可以是网卡优先级矩阵
 * @return 传输协议实例指针，失败返回nullptr
 *
 * 该函数手动安装传输协议：
 * 1. 检查协议是否已安装
 * 2. 解析网卡优先级配置（如果提供）
 * 3. 安装传输协议
 * 4. 注册已有的内存区域
 */
Transport *TransferEngine::installTransport(const std::string &proto,
                                            void **args) {
    // 检查协议是否已安装
    Transport *transport = multi_transports_->getTransport(proto);
    if (transport) {
        LOG(INFO) << "Transport " << proto << " already installed";
        return transport;
    }

    // 如果提供了网卡优先级矩阵
    if (args != nullptr && args[0] != nullptr) {
        const std::string nic_priority_matrix = static_cast<char *>(args[0]);
        int ret = local_topology_->parse(nic_priority_matrix);
        if (ret) {
            LOG(ERROR) << "Failed to parse NIC priority matrix";
            return nullptr;
        }
    }

    transport = multi_transports_->installTransport(proto, local_topology_);
    if (!transport) return nullptr;
    for (auto &entry : local_memory_regions_) {
        int ret = transport->registerLocalMemory(
            entry.addr, entry.length, entry.location, entry.remote_accessible);
        if (ret < 0) return nullptr;
    }
    return transport;
}

/**
 * @brief 卸载传输协议
 *
 * @param proto 协议名称
 * @return 成功返回0，失败返回错误码
 */
int TransferEngine::uninstallTransport(const std::string &proto) { return 0; }


/**
 * @brief 打开一个内存段
 *
 * @param segment_name 段名称
 * @return 段句柄，失败返回错误码
 *
 * 该函数用于打开一个命名的内存段：
 * 1. 验证段名称合法性
 * 2. 规范化段名称（去除前导斜杠）
 * 3. 从元数据服务获取段ID
 */
Transport::SegmentHandle TransferEngine::openSegment(
    const std::string &segment_name) {
    if (segment_name.empty()) return ERR_INVALID_ARGUMENT;
    std::string trimmed_segment_name = segment_name;
    while (!trimmed_segment_name.empty() && trimmed_segment_name[0] == '/')
        trimmed_segment_name.erase(0, 1);
    if (trimmed_segment_name.empty()) return ERR_INVALID_ARGUMENT;
    return metadata_->getSegmentID(trimmed_segment_name);
}

int TransferEngine::closeSegment(Transport::SegmentHandle handle) { return 0; }

/**
 * @brief 检查内存区域是否与已注册区域重叠
 *
 * @param addr 待检查的内存地址
 * @param length 内存长度
 * @return true表示有重叠，false表示无重叠
 *
 * 该函数遍历所有已注册的内存区域，
 * 检查新区域是否与它们有重叠。
 */
bool TransferEngine::checkOverlap(void *addr, uint64_t length) {
    for (auto &local_memory_region : local_memory_regions_) { 
        if (overlap(addr, length, local_memory_region.addr,
                    local_memory_region.length)) {
            return true;
        }
    }
    return false;
}

/**
 * @brief 注册本地内存区域
 *
 * @param addr 内存地址
 * @param length 内存长度
 * @param location 位置标识
 * @param remote_accessible 是否允许远程访问
 * @param update_metadata 是否更新元数据
 * @return 成功返回0，失败返回错误码
 *
 * 该函数完成内存区域的注册：
 * 1. 检查是否与已注册区域重叠
 * 2. 在所有传输协议中注册
 * 3. 添加到本地记录
 */
int TransferEngine::registerLocalMemory(void *addr, size_t length,
                                        const std::string &location,
                                        bool remote_accessible,
                                        bool update_metadata) {
    // 检查重叠
    if (checkOverlap(addr, length)) {
        LOG(ERROR)
            << "Transfer Engine does not support overlapped memory region";
        return ERR_ADDRESS_OVERLAPPED;
    }// multi_transports_->listTransports()列出所有已经安装的协议
    // 在所有传输协议中注册
    for (auto transport : multi_transports_->listTransports()) {
        int ret = transport->registerLocalMemory(
            addr, length, location, remote_accessible, update_metadata);
        if (ret < 0) return ret;
    }
    // 添加到本地记录
    local_memory_regions_.push_back(
        {addr, length, location, remote_accessible});
    return 0;
}

/**
 * @brief 取消注册本地内存区域
 *
 * @param addr 内存地址
 * @param update_metadata 是否更新元数据
 * @return 成功返回0，失败返回错误码
 *
 * 该函数完成内存区域的注销：
 * 1. 在所有传输协议中注销
 * 2. 从本地记录中移除
 */
int TransferEngine::unregisterLocalMemory(void *addr, bool update_metadata) {
    for (auto &transport : multi_transports_->listTransports()) {
        int ret = transport->unregisterLocalMemory(addr, update_metadata);
        if (ret) return ret;
    }
    for (auto it = local_memory_regions_.begin();
         it != local_memory_regions_.end(); ++it) {
        if (it->addr == addr) {
            local_memory_regions_.erase(it);
            break;
        }
    }
    return 0;
}
/**
 * @brief 批量注册本地内存区域
 *
 * @param buffer_list 缓冲区列表
 * @param location 位置标识
 * @return 成功返回0，失败返回错误码
 *
 * 该函数批量注册多个内存区域：
 * 1. 检查所有区域是否有重叠
 * 2. 在所有传输协议中批量注册
 * 3. 批量添加到本地记录
 */
int TransferEngine::registerLocalMemoryBatch(
    const std::vector<BufferEntry> &buffer_list, const std::string &location) {
    // 检查所有区域是否有重叠
    for (auto &buffer : buffer_list) {
        if (checkOverlap(buffer.addr, buffer.length)) {
            LOG(ERROR)
                << "Transfer Engine does not support overlapped memory region";
            return ERR_ADDRESS_OVERLAPPED;
        }
    }
    // 在所有传输协议中批量注册
    for (auto transport : multi_transports_->listTransports()) {
        int ret = transport->registerLocalMemoryBatch(buffer_list, location);
        if (ret < 0) return ret;
    }
    // 批量添加到本地记录
    for (auto &buffer : buffer_list) {
        local_memory_regions_.push_back(
            {buffer.addr, buffer.length, location, true});
    }
    return 0;
}
/**
 * @brief 批量取消注册本地内存区域
 *
 * @param addr_list 地址列表
 * @return 成功返回0，失败返回错误码
 *
 * 该函数批量注销多个内存区域：
 * 1. 在所有传输协议中批量注销
 * 2. 从本地记录中批量移除
 */
int TransferEngine::unregisterLocalMemoryBatch(
    const std::vector<void *> &addr_list) {
    for (auto transport : multi_transports_->listTransports()) {
        int ret = transport->unregisterLocalMemoryBatch(addr_list);
        if (ret < 0) return ret;
    }
    for (auto &addr : addr_list) {
        for (auto it = local_memory_regions_.begin();
             it != local_memory_regions_.end(); ++it) {
            if (it->addr == addr) {
                local_memory_regions_.erase(it);
                break;
            }
        }
    }
    return 0;
}
}  // namespace mooncake
