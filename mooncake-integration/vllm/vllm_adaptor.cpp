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

#include "vllm_adaptor.h"

#include <cassert>

VLLMAdaptor::VLLMAdaptor() {}

VLLMAdaptor::~VLLMAdaptor() {
    for (auto &handle : handle_map_) engine_->closeSegment(handle.second);
    handle_map_.clear();
    engine_.reset();
    for (auto &buffer : buffer_list_) free(buffer);
    buffer_list_.clear();
    for (auto &buffer : large_buffer_list_) free(buffer);
    large_buffer_list_.clear();
}

std::string formatDeviceNames(const std::string &device_names) {
//     tokens 向量将包含：["device1", "device2", "device3"]
// formatted 字符串将变为："\"device1\",\"device2\",\"device3\""
    std::stringstream ss(device_names);
    std::string item;
    std::vector<std::string> tokens;
    while (getline(ss, item, ',')) {
        tokens.push_back(item);
    }

    std::string formatted;
    for (size_t i = 0; i < tokens.size(); ++i) {
        formatted += "\"" + tokens[i] + "\"";
        if (i < tokens.size() - 1) {
            formatted += ",";
        }
    }
    return formatted;
}

std::pair<std::string, std::string> parseConnectionString(
    const std::string &conn_string) {
    std::pair<std::string, std::string> result;
    std::string proto = "etcd";
    std::string domain;
    std::size_t pos = conn_string.find("://");

    if (pos != std::string::npos) {
        proto = conn_string.substr(0, pos);
        domain = conn_string.substr(pos + 3);
    } else {
        domain = conn_string;
    }

    result.first = proto;
    result.second = domain;
    return result;
}

int VLLMAdaptor::initialize(const char *local_hostname,
                            const char *metadata_server, const char *protocol,
                            const char *device_name) {
    auto conn_string = parseConnectionString(metadata_server);
    return initializeExt(local_hostname, conn_string.second.c_str(), protocol,
                         device_name, conn_string.first.c_str());
}
// local_hostname 相当于prefill_url 包含端口
// metadata_server 相当于kv键值数据库的地址 包含端口
// protocol 相当于传输协议
// device_name 相当于设备名称 【例如：rdma设备名称,mlx1】
// metadata_type 相当于元数据类型 【例如：etcd】
int VLLMAdaptor::initializeExt(const char *local_hostname,
                               const char *metadata_server,
                               const char *protocol, const char *device_name,
                               const char *metadata_type) {
    std::string conn_string = metadata_server;
    if (conn_string.find("://") == std::string::npos)
        conn_string =
            std::string(metadata_type) + "://" + std::string(metadata_server);

    // TODO: remove `false` in the feature, it's for keep same API in vllm.
    engine_ = std::make_unique<TransferEngine>(false);
    auto hostname_port = parseHostNameWithPort(local_hostname);
    int ret = engine_->init(conn_string, local_hostname,
                            hostname_port.first.c_str(), hostname_port.second);
    if (ret) return -1;

    xport_ = nullptr;
    if (strcmp(protocol, "rdma") == 0) {
        auto device_names = formatDeviceNames(device_name);
        std::string nic_priority_matrix =
            "{\"cpu:0\": [[" + device_names + "], []]}";
        void **args = (void **)malloc(2 * sizeof(void *));
        args[0] = (void *)nic_priority_matrix.c_str();
        args[1] = nullptr;
        xport_ = engine_->installTransport("rdma", args);
    } else if (strcmp(protocol, "tcp") == 0) {
        xport_ = engine_->installTransport("tcp", nullptr);
    } else {
        LOG(ERROR) << "Unsupported protocol";
        return -1;
    }

    if (!xport_) return -1;
    free_list_.resize(kSlabSizeKBTabLen);
    doBuddyAllocate(kMaxClassId);
    return 0;
}

char *VLLMAdaptor::allocateRawBuffer(size_t capacity) {
    auto buffer = malloc(capacity);
    if (!buffer) return nullptr;
    int ret = engine_->registerLocalMemory(buffer, capacity, "cpu:0");
    if (ret) {
        free(buffer);
        return nullptr;
    }
    return (char *)buffer;
}

// 🔍 根据输入的 size（单位：字节），找到对应的“类 ID”（class ID），
// 这个 class ID 通常用于 内存分配器 中的 分配粒度分类
// （比如 slab allocator、buddy allocator 等）
int VLLMAdaptor::findClassId(size_t size) {
    if (size > 1024ull * kSlabSizeKB[kMaxClassId]) return -1;
    for (int i = kMaxClassId - 2; i >= 0; --i)
        if (size > 1024ull * kSlabSizeKB[i]) return i + 1;
    return 0;
}

// 这段代码是一个经典的 Buddy 内存分配算法（Buddy Allocator） 的实现部分，
// 具体是用于在指定 class_id 的内存块不足时，从更大的内存块中“拆分”出两个更小的块。
int VLLMAdaptor::doBuddyAllocate(int class_id) {
    if (class_id == kMaxClassId) {
        auto buffer = allocateRawBuffer(kDefaultBufferCapacity);
        buffer_list_.push_back(buffer);
        for (size_t offset = 0; offset < kDefaultBufferCapacity;
             offset += 1024ull * kSlabSizeKB[kMaxClassId])
            free_list_[kMaxClassId].push(buffer + offset);
        return 0;
    }
    if (free_list_[class_id + 1].empty()) {
        int ret = doBuddyAllocate(class_id + 1);
        if (ret) return ret;
    }
    assert(!free_list_[class_id + 1].empty());
    char *buffer = free_list_[class_id + 1].top();
    free_list_[class_id + 1].pop();
    free_list_[class_id].push(buffer);
    free_list_[class_id].push(buffer + kSlabSizeKB[class_id] * 1024);
    return 0;
}

uintptr_t VLLMAdaptor::allocateManagedBuffer(size_t length) {
    std::lock_guard<std::mutex> guard(mutex_);
    int class_id = findClassId(length);
    if (class_id < 0) {
        char *buffer = allocateRawBuffer(length);
        if (buffer) large_buffer_list_.insert(buffer); // 分配一个大内存块
        return (uintptr_t)buffer;
    }
    if (free_list_[class_id].empty()) //该大小的内存块不存在，就将更大的进行分块
        if (doBuddyAllocate(class_id)) return 0;
    assert(!free_list_[class_id].empty());
    char *buffer = free_list_[class_id].top();
    free_list_[class_id].pop();
    return (uintptr_t)buffer;
}

int VLLMAdaptor::freeManagedBuffer(uintptr_t buffer_addr, size_t length) {
    std::lock_guard<std::mutex> guard(mutex_);
    auto buffer = (char *)buffer_addr;
    int class_id = findClassId(length);
    if (class_id < 0) {
        large_buffer_list_.erase(buffer);
        engine_->unregisterLocalMemory(buffer);
        free(buffer);
        return 0;
    }
    free_list_[class_id].push(buffer);
    return 0;
}

// 将本地地址 buffer 处的数据，通过网络或某种传输机制，
//同步地传输到远端主机 target_hostname 的 peer_buffer_address 处，
// 长度为 length 字节。


int VLLMAdaptor::transferSync(const char *target_hostname, uintptr_t buffer,
                              uintptr_t peer_buffer_address, size_t length) {
    Transport::SegmentHandle handle;
    if (handle_map_.count(target_hostname)) {
        handle = handle_map_[target_hostname];
    } else { // target_hostname这里对应的就是segment_name名
        handle = engine_->openSegment(target_hostname);
        if (handle == (Transport::SegmentHandle)-1) return -1;
        handle_map_[target_hostname] = handle;
    }

    auto batch_id = engine_->allocateBatchID(1);
    TransferRequest entry;
    entry.opcode = TransferRequest::READ;
    entry.length = length;
    entry.source = (void *)buffer;
    entry.target_id = handle;
    entry.target_offset = peer_buffer_address;

    int ret = engine_->submitTransfer(batch_id, {entry});
    if (ret < 0) return -1;

    TransferStatus status;
    while (true) {
        int ret = engine_->getTransferStatus(batch_id, 0, status);
        LOG_ASSERT(!ret);
        if (status.s == TransferStatusEnum::COMPLETED) {
            engine_->freeBatchID(batch_id);
            return 0;
        } else if (status.s == TransferStatusEnum::FAILED) {
            engine_->freeBatchID(batch_id);
            return -1;
        }
    }
}

// 用于在本地注册和注销内存区域，供后续的远程数据传输使用。
int VLLMAdaptor::expRegisterMemory(uintptr_t buffer_addr, size_t capacity) {
    char *buffer = reinterpret_cast<char *>(buffer_addr);
    return engine_->registerLocalMemory(buffer, capacity, "cpu:0");
}

int VLLMAdaptor::expUnregisterMemory(uintptr_t buffer_addr) {
    char *buffer = reinterpret_cast<char *>(buffer_addr);
    return engine_->unregisterLocalMemory(buffer);
}

namespace py = pybind11;

PYBIND11_MODULE(mooncake_vllm_adaptor, m) {
    py::class_<VLLMAdaptor>(m, "mooncake_vllm_adaptor")
        .def(py::init<>())
        .def("initialize", &VLLMAdaptor::initialize)
        .def("initializeExt", &VLLMAdaptor::initializeExt)
        .def("allocateManagedBuffer", &VLLMAdaptor::allocateManagedBuffer)
        .def("freeManagedBuffer", &VLLMAdaptor::freeManagedBuffer)
        .def("transferSync", &VLLMAdaptor::transferSync)
        .def("writeBytesToBuffer", &VLLMAdaptor::writeBytesToBuffer)
        .def("readBytesFromBuffer", &VLLMAdaptor::readBytesFromBuffer)
        .def("expRegisterMemory", &VLLMAdaptor::expRegisterMemory)
        .def("expUnregisterMemory", &VLLMAdaptor::expUnregisterMemory);
}
