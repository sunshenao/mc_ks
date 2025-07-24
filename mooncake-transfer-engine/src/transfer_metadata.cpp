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

#include "transfer_metadata.h"

#include <jsoncpp/json/value.h>

#include <cassert>
#include <set>

#include "common.h"
#include "config.h"
#include "error.h"
#include "transfer_metadata_plugin.h"

namespace mooncake {

// 元数据键前缀定义
const static std::string kCommonKeyPrefix = "mooncake/";
const static std::string kRpcMetaPrefix = kCommonKeyPrefix + "rpc_meta/";

/**
 * @brief 获取完整的元数据键名
 *
 * @param segment_name 段名称
 * @return 完整的键名
 *
 * 根据段名称的格式生成元数据键名：
 * - 如果段名称不包含"/"，则认为是内存段，添加"ram/"前缀
 * - 否则直接使用段名称，仅添加通用前缀
 */
// mooncake/segments/[...]
static inline std::string getFullMetadataKey(const std::string &segment_name) {
    auto pos = segment_name.find("/");
    if (pos == segment_name.npos)
        return kCommonKeyPrefix + "ram/" + segment_name;
    else
        return kCommonKeyPrefix + segment_name;
}
/**
 * @struct TransferHandshakeUtil
 * @brief 传输握手协议的序列化工具
 *
 * 提供了握手描述符与JSON格式之间的转换功能，
 * 用于握手信息的网络传输和存储。
 */
struct TransferHandshakeUtil {
    /**
     * @brief 将握手描述符编码为JSON
     * @param desc 握手描述符
     * @return JSON对象
     */
    static Json::Value encode(const TransferMetadata::HandShakeDesc &desc) {
        Json::Value root;
        // 编码基本信息
        root["local_nic_path"] = desc.local_nic_path;
        root["peer_nic_path"] = desc.peer_nic_path;
        // 编码QP号列表
        Json::Value qpNums(Json::arrayValue);
        for (const auto &qp : desc.qp_num) qpNums.append(qp);
        root["qp_num"] = qpNums;
        // 编码回复消息
        root["reply_msg"] = desc.reply_msg;
        return root;
    }

    /**
     * @brief 从JSON解码握手描述符
     * @param root JSON对象
     * @param desc 握手描述符输出参数
     * @return 成功返回0，失败返回错误码
     */
    static int decode(Json::Value root, TransferMetadata::HandShakeDesc &desc) {
        Json::Reader reader;
        // 解码基本信息
        desc.local_nic_path = root["local_nic_path"].asString();
        desc.peer_nic_path = root["peer_nic_path"].asString();
        // 解码QP号列表
        for (const auto &qp : root["qp_num"])
            desc.qp_num.push_back(qp.asUInt());
        // 解码回复消息
        desc.reply_msg = root["reply_msg"].asString();

        // 调试输出
        if (globalConfig().verbose) {
            LOG(INFO) << "TransferHandshakeUtil::decode: local_nic_path "
                      << desc.local_nic_path << " peer_nic_path "
                      << desc.peer_nic_path << " qp_num count "
                      << desc.qp_num.size();
        }
        return 0;
    }
};

/**
 * @brief 构造传输元数据管理器
 * @param conn_string 连接字符串
 *
 * 该函数完成元数据管理器的初始化：
 * 1. 创建握手协议插件
 * 2. 创建存储插件
 * 3. 初始化段ID计数器
 */
TransferMetadata::TransferMetadata(const std::string &conn_string) {
    // 创建握手和存储插件
    handshake_plugin_ = HandShakePlugin::Create(conn_string); // conn_string只有一个握手插件
    storage_plugin_ = MetadataStoragePlugin::Create(conn_string); // 可以设置http、etcd、redis等存储插件
    if (!handshake_plugin_ || !storage_plugin_) {
        LOG(ERROR) << "Unable to create metadata plugins with conn string "
                   << conn_string;
    }
    // 初始化段ID计数器
    next_segment_id_.store(1);
}
/**
 * @brief 析构函数，释放所有资源
 */
TransferMetadata::~TransferMetadata() { handshake_plugin_.reset(); }

/**
 * @brief 更新段描述符
 *
 * @param segment_name 段名称
 * @param desc 段描述符
 * @return 成功返回0，失败返回错误码
 *
 * 该函数将段描述符序列化为JSON并存储：
 * 1. 编码基本信息（名称、协议）
 * 2. 根据协议类型编码不同的设备信息
 * 3. 编码缓冲区信息
 * 4. 存储到元数据服务
 */
int TransferMetadata::updateSegmentDesc(const std::string &segment_name,
                                        const SegmentDesc &desc) {
    // 编码基本信息
    Json::Value segmentJSON;
    segmentJSON["name"] = desc.name;
    segmentJSON["protocol"] = desc.protocol;

    // 对于RDMA协议的特殊处理
    if (segmentJSON["protocol"] == "rdma") {
        // 编码RDMA设备信息
        Json::Value devicesJSON(Json::arrayValue);
        for (const auto &device : desc.devices) {
            Json::Value deviceJSON;
            deviceJSON["name"] = device.name;
            deviceJSON["lid"] = device.lid;
            deviceJSON["gid"] = device.gid;
            devicesJSON.append(deviceJSON);
        }
        segmentJSON["devices"] = devicesJSON;

        // 编码RDMA缓冲区信息
        Json::Value buffersJSON(Json::arrayValue);
        for (const auto &buffer : desc.buffers) {
            Json::Value bufferJSON;
            bufferJSON["name"] = buffer.name;
            bufferJSON["addr"] = static_cast<Json::UInt64>(buffer.addr);
            bufferJSON["length"] = static_cast<Json::UInt64>(buffer.length);
            Json::Value rkeyJSON(Json::arrayValue);
            for (auto &entry : buffer.rkey) rkeyJSON.append(entry);
            bufferJSON["rkey"] = rkeyJSON;
            Json::Value lkeyJSON(Json::arrayValue);
            for (auto &entry : buffer.lkey) lkeyJSON.append(entry);
            bufferJSON["lkey"] = lkeyJSON;
            buffersJSON.append(bufferJSON);
        }
        segmentJSON["buffers"] = buffersJSON;
        segmentJSON["priority_matrix"] = desc.topology.toJson();
    } else if (segmentJSON["protocol"] == "tcp") {
        // 编码TCP缓冲区信息
        Json::Value buffersJSON(Json::arrayValue);
        for (const auto &buffer : desc.buffers) {
            Json::Value bufferJSON;
            bufferJSON["name"] = buffer.name;
            bufferJSON["addr"] = static_cast<Json::UInt64>(buffer.addr);
            bufferJSON["length"] = static_cast<Json::UInt64>(buffer.length);
            buffersJSON.append(bufferJSON);
        }
        segmentJSON["buffers"] = buffersJSON;
    } else {
        LOG(ERROR) << "Unsupported segment descriptor for register, name "
                   << desc.name << " protocol " << desc.protocol;
        return ERR_METADATA;
    }

    // 存储段描述符
    if (!storage_plugin_->set(getFullMetadataKey(segment_name), segmentJSON)) {
        LOG(ERROR) << "Failed to register segment descriptor, name "
                   << desc.name << " protocol " << desc.protocol;
        return ERR_METADATA;
    }

    return 0;
}
/**
 * @brief 删除段描述符
 *
 * @param segment_name 段名称
 * @return 成功返回0，失败返回错误码
 *
 * 该函数从元数据服务中删除指定的段描述符。
 */
int TransferMetadata::removeSegmentDesc(const std::string &segment_name) {
    if (!storage_plugin_->remove(getFullMetadataKey(segment_name))) {
        LOG(ERROR) << "Failed to unregister segment descriptor, name "
                   << segment_name;
        return ERR_METADATA;
    }
    return 0;
}

/**
 * @brief 获取段描述符
 *
 * @param segment_name 段名称
 * @return 段描述符指针，失败返回nullptr
 *
 * 该函数从元数据服务中获取指定的段描述符，并解析为SegmentDesc结构。
 */
std::shared_ptr<TransferMetadata::SegmentDesc> TransferMetadata::getSegmentDesc(
    const std::string &segment_name) {
    Json::Value segmentJSON;
    if (!storage_plugin_->get(getFullMetadataKey(segment_name), segmentJSON)) {
        LOG(WARNING) << "Failed to retrieve segment descriptor, name "
                     << segment_name;
        return nullptr;
    }

    auto desc = std::make_shared<SegmentDesc>();
    desc->name = segmentJSON["name"].asString();
    desc->protocol = segmentJSON["protocol"].asString();

    if (desc->protocol == "rdma") {
        for (const auto &deviceJSON : segmentJSON["devices"]) {
            DeviceDesc device;
            device.name = deviceJSON["name"].asString();
            device.lid = deviceJSON["lid"].asUInt();
            device.gid = deviceJSON["gid"].asString();
            if (device.name.empty() || device.gid.empty()) {
                LOG(WARNING) << "Corrupted segment descriptor, name "
                             << segment_name << " protocol " << desc->protocol;
                return nullptr;
            }
            desc->devices.push_back(device);
        }

        for (const auto &bufferJSON : segmentJSON["buffers"]) {
            BufferDesc buffer;
            buffer.name = bufferJSON["name"].asString();
            buffer.addr = bufferJSON["addr"].asUInt64();
            buffer.length = bufferJSON["length"].asUInt64();
            for (const auto &rkeyJSON : bufferJSON["rkey"])
                buffer.rkey.push_back(rkeyJSON.asUInt());
            for (const auto &lkeyJSON : bufferJSON["lkey"])
                buffer.lkey.push_back(lkeyJSON.asUInt());
            if (buffer.name.empty() || !buffer.addr || !buffer.length ||
                buffer.rkey.empty() ||
                buffer.rkey.size() != buffer.lkey.size()) {
                LOG(WARNING) << "Corrupted segment descriptor, name "
                             << segment_name << " protocol " << desc->protocol;
                return nullptr;
            }
            desc->buffers.push_back(buffer);
        }

        int ret = desc->topology.parse(
            segmentJSON["priority_matrix"].toStyledString());
        if (ret) {
            LOG(WARNING) << "Corrupted segment descriptor, name "
                         << segment_name << " protocol " << desc->protocol;
        }
    } else if (desc->protocol == "tcp") {
        for (const auto &bufferJSON : segmentJSON["buffers"]) {
            BufferDesc buffer;
            buffer.name = bufferJSON["name"].asString();
            buffer.addr = bufferJSON["addr"].asUInt64();
            buffer.length = bufferJSON["length"].asUInt64();
            if (buffer.name.empty() || !buffer.addr || !buffer.length) {
                LOG(WARNING) << "Corrupted segment descriptor, name "
                             << segment_name << " protocol " << desc->protocol;
                return nullptr;
            }
            desc->buffers.push_back(buffer);
        }
    } else if (desc->protocol == "nvmeof") {
        for (const auto &bufferJSON : segmentJSON["buffers"]) {
            NVMeoFBufferDesc buffer;
            buffer.file_path = bufferJSON["file_path"].asString();
            buffer.length = bufferJSON["length"].asUInt64();
            const Json::Value &local_path_map = bufferJSON["local_path_map"];
            for (const auto &key : local_path_map.getMemberNames()) {
                buffer.local_path_map[key] = local_path_map[key].asString();
            }
            desc->nvmeof_buffers.push_back(buffer);
        }
    } else {
        LOG(ERROR) << "Unsupported segment descriptor, name " << segment_name
                   << " protocol " << desc->protocol;
        return nullptr;
    }

    return desc;
}

/**
 * @brief 同步段缓存
 *
 * @return 成功返回0，失败返回错误码
 *
 * 该函数用于更新本地段描述符缓存：
 * 1. 遍历所有段ID到描述符的映射
 * 2. 跳过本地段ID
 * 3. 重新获取段描述符并更新缓存
 */ 
// 从远程元数据服务中获取段描述符并更新本地缓存
int TransferMetadata::syncSegmentCache() {
    RWSpinlock::WriteGuard guard(segment_lock_);
    LOG(INFO) << "Invalidate segment descriptor cache";
    for (auto &entry : segment_id_to_desc_map_) {
        if (entry.first == LOCAL_SEGMENT_ID) continue;
        auto segment_desc = getSegmentDesc(entry.second->name);
        if (segment_desc) entry.second = segment_desc;
    }
    return 0;
}


/**
 * @brief 根据段名称获取段描述符
 *
 * @param segment_name 段名称
 * @param force_update 是否强制更新
 * @return 段描述符指针，失败返回nullptr
 *
 * 该函数根据段名称查找段描述符：
 * - 首先在本地缓存中查找
 * - 如果未找到且不需要强制更新，则返回nullptr
 * - 否则从元数据服务中获取段描述符并更新缓存
 */
std::shared_ptr<TransferMetadata::SegmentDesc>
TransferMetadata::getSegmentDescByName(const std::string &segment_name,
                                       bool force_update) {
    if (!force_update) {
        RWSpinlock::ReadGuard guard(segment_lock_);
        auto iter = segment_name_to_id_map_.find(segment_name);
        if (iter != segment_name_to_id_map_.end())
            return segment_id_to_desc_map_[iter->second];
    }

    RWSpinlock::WriteGuard guard(segment_lock_);
    auto iter = segment_name_to_id_map_.find(segment_name);
    SegmentID segment_id;
    if (iter != segment_name_to_id_map_.end())
        segment_id = iter->second;
    else
        segment_id = next_segment_id_.fetch_add(1);
    auto segment_desc = this->getSegmentDesc(segment_name);
    if (!segment_desc) return nullptr;
    segment_id_to_desc_map_[segment_id] = segment_desc;
    segment_name_to_id_map_[segment_name] = segment_id;
    return segment_desc;
}

/**
 * @brief 根据段ID获取段描述符
 *
 * @param segment_id 段ID
 * @param force_update 是否强制更新
 * @return 段描述符指针，失败返回nullptr
 *
 * 该函数根据段ID查找段描述符：
 * - 首先在本地缓存中查找
 * - 如果未找到且需要强制更新，则从元数据服务中重新获取
 */

std::shared_ptr<TransferMetadata::SegmentDesc>
TransferMetadata::getSegmentDescByID(SegmentID segment_id, bool force_update) {
    if (force_update) {
        RWSpinlock::WriteGuard guard(segment_lock_);
        if (!segment_id_to_desc_map_.count(segment_id)) return nullptr;
        auto segment_desc =
            getSegmentDesc(segment_id_to_desc_map_[segment_id]->name);
        if (!segment_desc) return nullptr;
        segment_id_to_desc_map_[segment_id] = segment_desc;
        return segment_id_to_desc_map_[segment_id];
    } else {
        RWSpinlock::ReadGuard guard(segment_lock_);
        if (!segment_id_to_desc_map_.count(segment_id)) return nullptr;
        return segment_id_to_desc_map_[segment_id];
    }
}

/**
 * @brief 根据段名称获取段ID
 *
 * @param segment_name 段名称
 * @return 段ID，未找到返回-1
 *
 * 该函数根据段名称查找段ID：
 * - 首先在本地缓存中查找
 * - 如果未找到，则分配一个新的段ID
 */
TransferMetadata::SegmentID TransferMetadata::getSegmentID(
    const std::string &segment_name) {
    {
        RWSpinlock::ReadGuard guard(segment_lock_);
        if (segment_name_to_id_map_.count(segment_name))
            return segment_name_to_id_map_[segment_name];
    }

    RWSpinlock::WriteGuard guard(segment_lock_);
    if (segment_name_to_id_map_.count(segment_name))
        return segment_name_to_id_map_[segment_name];
    auto segment_desc = this->getSegmentDesc(segment_name);
    if (!segment_desc) return -1;
    SegmentID id = next_segment_id_.fetch_add(1);
    segment_id_to_desc_map_[id] = segment_desc;
    segment_name_to_id_map_[segment_name] = id;
    return id;
}

/**
 * @brief 更新本地段描述符
 *
 * @param segment_id 段ID
 * @return 成功返回0，失败返回错误码
 *
 * 该函数用于更新本地段描述符：
 * 1. 根据段ID获取当前段描述符
 * 2. 调用updateSegmentDesc更新段描述符
 */
int TransferMetadata::updateLocalSegmentDesc(uint64_t segment_id) {
    RWSpinlock::ReadGuard guard(segment_lock_);
    // 这个保存的段描述符指的是 在本地维护的段描述符信息
    auto desc = segment_id_to_desc_map_[segment_id];
    // 更新名字为desc->name 的段的内容文desc
    return this->updateSegmentDesc(desc->name, *desc); 
}

/**
 * @brief 添加本地段
 *
 * @param segment_id 段ID
 * @param segment_name 段名称
 * @param desc 段描述符
 * @return 成功返回0，失败返回错误码
 *
 * 该函数用于添加一个新的本地段：
 * 1. 获取写锁
 * 2. 将段描述符添加到段ID和段名称的映射中
 */
int TransferMetadata::addLocalSegment(SegmentID segment_id,
                                      const std::string &segment_name,
                                      std::shared_ptr<SegmentDesc> &&desc) {
    RWSpinlock::WriteGuard guard(segment_lock_);
    segment_id_to_desc_map_[segment_id] = desc;
    segment_name_to_id_map_[segment_name] = segment_id;
    return 0;
}

/**
 * @brief 添加本地内存缓冲区
 *
 * @param buffer_desc 缓冲区描述符
 * @param update_metadata 是否更新元数据
 * @return 成功返回0，失败返回错误码
 *
 * 该函数用于向本地段添加内存缓冲区：
 * 1. 获取写锁
 * 2. 克隆当前本地段描述符
 * 3. 将新缓冲区添加到段描述符中
 * 4. 可选地更新元数据
 */
int TransferMetadata::addLocalMemoryBuffer(const BufferDesc &buffer_desc,
                                           bool update_metadata) {
    // 向本地段添加一个新的内存缓冲区
    {
        RWSpinlock::WriteGuard guard(segment_lock_);
        auto new_segment_desc = std::make_shared<SegmentDesc>();
        auto &segment_desc = segment_id_to_desc_map_[LOCAL_SEGMENT_ID];
        *new_segment_desc = *segment_desc;
        segment_desc = new_segment_desc;
        segment_desc->buffers.push_back(buffer_desc);
    }
    if (update_metadata) return updateLocalSegmentDesc();
    return 0;
}

/**
 * @brief 移除本地内存缓冲区
 *
 * @param addr 缓冲区地址
 * @param update_metadata 是否更新元数据
 * @return 成功返回0，失败返回错误码
 *
 * 该函数用于从本地段移除内存缓冲区：
 * 1. 获取写锁
 * 2. 克隆当前本地段描述符
 * 3. 从段描述符中移除指定地址的缓冲区
 * 4. 可选地更新元数据
 */
int TransferMetadata::removeLocalMemoryBuffer(void *addr,
                                              bool update_metadata) {
    bool addr_exist = false;
    {
        RWSpinlock::WriteGuard guard(segment_lock_);
        auto new_segment_desc = std::make_shared<SegmentDesc>();
        auto &segment_desc = segment_id_to_desc_map_[LOCAL_SEGMENT_ID];
        *new_segment_desc = *segment_desc;
        segment_desc = new_segment_desc;
        for (auto iter = segment_desc->buffers.begin();
             iter != segment_desc->buffers.end(); ++iter) {
            if (iter->addr == (uint64_t)addr) {
                segment_desc->buffers.erase(iter);
                addr_exist = true;
                break;
            }
        }
    }
    if (addr_exist) {
        if (update_metadata) return updateLocalSegmentDesc();
        return 0;
    }
    return ERR_ADDRESS_NOT_REGISTERED;
}

/**
 * @brief 添加RPC元数据条目
 *
 * @param server_name 服务器名称
 * @param desc RPC元数据描述符
 * @return 成功返回0，失败返回错误码
 *
 * 该函数用于添加或更新RPC服务的元数据：
 * 1. 将RPC元数据序列化为JSON
 * 2. 存储到元数据服务
 * 3. 更新本地缓存
 */
// 这里实际上就是将prefill或者decode url加入到服务器中，方便对方的实例可以进行读取
int TransferMetadata::addRpcMetaEntry(const std::string &server_name,
                                      RpcMetaDesc &desc) {
    Json::Value rpcMetaJSON;
    rpcMetaJSON["ip_or_host_name"] = desc.ip_or_host_name;
    rpcMetaJSON["rpc_port"] = static_cast<Json::UInt64>(desc.rpc_port);
    if (!storage_plugin_->set(kRpcMetaPrefix + server_name, rpcMetaJSON)) {
        LOG(ERROR) << "Failed to set location of " << server_name;
        return ERR_METADATA;
    }
    local_rpc_meta_ = desc; // 作为本地通信的rpc端口
    return 0;
}

// 移除相应的条目
int TransferMetadata::removeRpcMetaEntry(const std::string &server_name) {
    if (!storage_plugin_->remove(kRpcMetaPrefix + server_name)) {
        LOG(ERROR) << "Failed to remove location of " << server_name;
        return ERR_METADATA;
    }
    return 0;
}
/**
 * @brief 获取RPC元数据条目
 *
 * @param server_name 服务器名称
 * @param desc RPC元数据描述符输出参数
 * @return 成功返回0，失败返回错误码
 *
 * 该函数用于获取指定RPC服务的元数据：
 * 1. 首先检查本地缓存
 * 2. 如果未找到，则从元数据服务中获取并更新缓存
 */
int TransferMetadata::getRpcMetaEntry(const std::string &server_name,
                                      RpcMetaDesc &desc) {
    {
        RWSpinlock::ReadGuard guard(rpc_meta_lock_);
        if (rpc_meta_map_.count(server_name)) {
            desc = rpc_meta_map_[server_name];
            return 0;
        }
    }
    RWSpinlock::WriteGuard guard(rpc_meta_lock_);
    Json::Value rpcMetaJSON;
    if (!storage_plugin_->get(kRpcMetaPrefix + server_name, rpcMetaJSON)) {
        LOG(ERROR) << "Failed to find location of " << server_name;
        return ERR_METADATA;
    }
    desc.ip_or_host_name = rpcMetaJSON["ip_or_host_name"].asString();
    desc.rpc_port = (uint16_t)rpcMetaJSON["rpc_port"].asUInt();
    rpc_meta_map_[server_name] = desc;
    return 0;
}


/**
 * @brief 启动握手守护进程
 *
 * @param on_receive_handshake 握手接收回调函数
 * @param listen_port 监听端口
 * @return 成功返回0，失败返回错误码
 *
 * 该函数用于启动握手协议的监听：
 * 1. 注册握手接收回调
 * 2. 启动监听指定端口
 */
// TransferMetadata::startHandshakeDaemon 函数用于启动一个握手守护进程，
// 监听指定端口以处理传入的握手请求。
// 这个函数的核心是将一个回调函数传递给 handshake_plugin_，
// 以便在收到握手请求时执行特定的操作。
int TransferMetadata::startHandshakeDaemon(
    OnReceiveHandShake on_receive_handshake, uint16_t listen_port) {
    return handshake_plugin_->startDaemon(
        [on_receive_handshake](const Json::Value &local,
                               Json::Value &peer) -> int {
            HandShakeDesc local_desc, peer_desc;
            TransferHandshakeUtil::decode(local, local_desc);
            int ret = on_receive_handshake(local_desc, peer_desc);
            if (ret) return ret;
            peer = TransferHandshakeUtil::encode(peer_desc);
            return 0;
        },
        listen_port);
}

/**
 * @brief 发送握手请求
 *
 * @param peer_server_name 对端服务器名称
 * @param local_desc 本地握手描述符
 * @param peer_desc 对端握手描述符输出参数
 * @return 成功返回0，失败返回错误码
 *
 * 该函数用于向指定的对端服务器发送握手请求：
 * 1. 获取对端服务器的RPC元数据
 * 2. 编码本地握手描述符为JSON
 * 3. 通过RPC发送握手请求
 * 4. 解析对端的握手回复
 */
int TransferMetadata::sendHandshake(const std::string &peer_server_name,
                                    const HandShakeDesc &local_desc,
                                    HandShakeDesc &peer_desc) { 
                                    // peer_desc接受对方服务器的相应信息
    RpcMetaDesc peer_location;
    if (getRpcMetaEntry(peer_server_name, peer_location)) {
        return ERR_METADATA;
    }
    auto local = TransferHandshakeUtil::encode(local_desc);
    Json::Value peer;// 就是接受对法发送回来的结果的数据
    int ret = handshake_plugin_->send(peer_location.ip_or_host_name,
                                      peer_location.rpc_port, local, peer);
    if (ret) return ret;
    TransferHandshakeUtil::decode(peer, peer_desc);
    if (!peer_desc.reply_msg.empty()) {
        LOG(ERROR) << "Handshake rejected by " << peer_server_name << ": "
                   << peer_desc.reply_msg;
        return ERR_METADATA;
    }
    return 0;
}
}  // namespace mooncake