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

#include "transfer_metadata_plugin.h"

#include <arpa/inet.h>
#include <bits/stdint-uintn.h>
#include <jsoncpp/json/value.h>
#include <netdb.h>
#include <sys/socket.h>

#ifdef USE_REDIS
#include <hiredis/hiredis.h>
#endif

#ifdef USE_HTTP
#include <curl/curl.h>
#endif

#include <cassert>
#include <set>

#include "common.h"
#include "config.h"
#include "error.h"

namespace mooncake {
#ifdef USE_REDIS
struct RedisStoragePlugin : public MetadataStoragePlugin {
    RedisStoragePlugin(const std::string &metadata_uri)
        : client_(nullptr), metadata_uri_(metadata_uri) {
        auto hostname_port = parseHostNameWithPort(metadata_uri);
        client_ =
            redisConnect(hostname_port.first.c_str(), hostname_port.second);
        if (!client_ || client_->err) {
            LOG(ERROR) << "RedisStoragePlugin: unable to connect "
                       << metadata_uri_ << ": " << client_->errstr;
            client_ = nullptr;
        }
    }

    virtual ~RedisStoragePlugin() {}

    virtual bool get(const std::string &key, Json::Value &value) {
        Json::Reader reader;
        redisReply *resp =
            (redisReply *)redisCommand(client_, "GET %s", key.c_str());
        if (!resp) {
            LOG(ERROR) << "RedisStoragePlugin: unable to get " << key
                       << " from " << metadata_uri_;
            return false;
        }
        auto json_file = std::string(resp->str);
        freeReplyObject(resp);
        if (!reader.parse(json_file, value)) return false;
        if (globalConfig().verbose)
            LOG(INFO) << "RedisStoragePlugin: get: key=" << key
                      << ", value=" << json_file;
        return true;
    }

    virtual bool set(const std::string &key, const Json::Value &value) {
        Json::FastWriter writer;
        const std::string json_file = writer.write(value);
        if (globalConfig().verbose)
            LOG(INFO) << "RedisStoragePlugin: set: key=" << key
                      << ", value=" << json_file;
        redisReply *resp = (redisReply *)redisCommand(
            client_, "SET %s %s", key.c_str(), json_file.c_str());
        if (!resp) {
            LOG(ERROR) << "RedisStoragePlugin: unable to put " << key
                       << " from " << metadata_uri_;
            return false;
        }
        freeReplyObject(resp);
        return true;
    }

    virtual bool remove(const std::string &key) {
        redisReply *resp =
            (redisReply *)redisCommand(client_, "DEL %s", key.c_str());
        if (!resp) {
            LOG(ERROR) << "RedisStoragePlugin: unable to remove " << key
                       << " from " << metadata_uri_;
            return false;
        }
        freeReplyObject(resp);
        return true;
    }

    redisContext *client_;
    const std::string metadata_uri_;
};
#endif  // USE_REDIS

#ifdef USE_HTTP
struct HTTPStoragePlugin : public MetadataStoragePlugin {
    HTTPStoragePlugin(const std::string &metadata_uri)
        : client_(nullptr), metadata_uri_(metadata_uri) {
        curl_global_init(CURL_GLOBAL_ALL);
        client_ = curl_easy_init();
        if (!client_) {
            LOG(ERROR) << "Cannot allocate CURL objects";
            exit(EXIT_FAILURE);
        }
    }

    virtual ~HTTPStoragePlugin() {
        curl_easy_cleanup(client_);
        curl_global_cleanup();
    }

    /**
     * @brief HTTP元数据存储插件的回调函数
     * 用于处理HTTP请求的响应数据
     *
     * @param contents 响应内容的指针
     * @param size 每个数据块的大小
     * @param nmemb 数据块的数量
     * @param userdata 用户自定义数据指针
     * @return size_t 实际处理的数据大小
     */ // 将要写入的内容追加到用户数据中
    static size_t writeCallback(void *contents, size_t size, size_t nmemb,
                                std::string *userp) {
        userp->append(static_cast<char *>(contents), size * nmemb);
        return size * nmemb;
    }

    std::string encodeUrl(const std::string &key) {
        char *newkey = curl_easy_escape(client_, key.c_str(), key.size());
        std::string encodedKey(newkey);
        std::string url = metadata_uri_ + "?key=" + encodedKey;
        curl_free(newkey);
        return url;
    }

    virtual bool get(const std::string &key, Json::Value &value) {
        curl_easy_reset(client_);
        curl_easy_setopt(client_, CURLOPT_TIMEOUT_MS, 3000);  // 3s timeout

        std::string url = encodeUrl(key);
        curl_easy_setopt(client_, CURLOPT_URL, url.c_str());
        curl_easy_setopt(client_, CURLOPT_WRITEFUNCTION, writeCallback);

        // get response body
        std::string readBuffer;
        curl_easy_setopt(client_, CURLOPT_WRITEDATA, &readBuffer);
        CURLcode res = curl_easy_perform(client_);
        if (res != CURLE_OK) {
            LOG(ERROR) << "Error from http client, GET " << url
                       << " error: " << curl_easy_strerror(res);
            return false;
        }

        // Get the HTTP response code
        long responseCode;
        curl_easy_getinfo(client_, CURLINFO_RESPONSE_CODE, &responseCode);
        if (responseCode != 200) {
            LOG(ERROR) << "Unexpected code in http response, GET " << url
                       << " response code: " << responseCode
                       << " response body: " << readBuffer;
            return false;
        }

        if (globalConfig().verbose)
            LOG(INFO) << "Get segment desc, key=" << key
                      << ", value=" << readBuffer;

        Json::Reader reader;
        if (!reader.parse(readBuffer, value)) return false;
        return true;
    }

    virtual bool set(const std::string &key, const Json::Value &value) {
        curl_easy_reset(client_);
        curl_easy_setopt(client_, CURLOPT_TIMEOUT_MS, 3000);  // 3s timeout

        Json::FastWriter writer;
        const std::string json_file = writer.write(value);
        if (globalConfig().verbose)
            LOG(INFO) << "Put segment desc, key=" << key
                      << ", value=" << json_file;

        std::string url = encodeUrl(key);
        curl_easy_setopt(client_, CURLOPT_URL, url.c_str());
        curl_easy_setopt(client_, CURLOPT_WRITEFUNCTION, writeCallback);
        curl_easy_setopt(client_, CURLOPT_POSTFIELDS, json_file.c_str());
        curl_easy_setopt(client_, CURLOPT_POSTFIELDSIZE, json_file.size());
        curl_easy_setopt(client_, CURLOPT_CUSTOMREQUEST, "PUT");

        // get response body
        std::string readBuffer;
        curl_easy_setopt(client_, CURLOPT_WRITEDATA, &readBuffer);

        // set content-type to application/json
        struct curl_slist *headers = NULL;
        headers = curl_slist_append(headers, "Content-Type: application/json");
        curl_easy_setopt(client_, CURLOPT_HTTPHEADER, headers);
        CURLcode res = curl_easy_perform(client_);
        curl_slist_free_all(headers);  // Free headers
        if (res != CURLE_OK) {
            LOG(ERROR) << "Error from http client, PUT " << url
                       << " error: " << curl_easy_strerror(res);
            return false;
        }

        // Get the HTTP response code
        long responseCode;
        curl_easy_getinfo(client_, CURLINFO_RESPONSE_CODE, &responseCode);
        if (responseCode != 200) {
            LOG(ERROR) << "Unexpected code in http response, PUT " << url
                       << " response code: " << responseCode
                       << " response body: " << readBuffer;
            return false;
        }

        return true;
    }

    virtual bool remove(const std::string &key) {
        curl_easy_reset(client_);
        curl_easy_setopt(client_, CURLOPT_TIMEOUT_MS, 3000);  // 3s timeout

        if (globalConfig().verbose)
            LOG(INFO) << "Remove segment desc, key=" << key;

        std::string url = encodeUrl(key);
        curl_easy_setopt(client_, CURLOPT_URL, url.c_str());
        curl_easy_setopt(client_, CURLOPT_WRITEFUNCTION, writeCallback);
        curl_easy_setopt(client_, CURLOPT_CUSTOMREQUEST, "DELETE");

        // get response body
        std::string readBuffer;
        curl_easy_setopt(client_, CURLOPT_WRITEDATA, &readBuffer);
        CURLcode res = curl_easy_perform(client_);
        if (res != CURLE_OK) {
            LOG(ERROR) << "Error from http client, DELETE " << url
                       << " error: " << curl_easy_strerror(res);
            return false;
        }

        // Get the HTTP response code
        long responseCode;
        curl_easy_getinfo(client_, CURLINFO_RESPONSE_CODE, &responseCode);
        if (responseCode != 200) {
            LOG(ERROR) << "Unexpected code in http response, DELETE " << url
                       << " response code: " << responseCode
                       << " response body: " << readBuffer;
            return false;
        }
        return true;
    }

    CURL *client_;
    const std::string metadata_uri_;
};
#endif  // USE_HTTP

struct EtcdStoragePlugin : public MetadataStoragePlugin {
    EtcdStoragePlugin(const std::string &metadata_uri)
        : client_(metadata_uri), metadata_uri_(metadata_uri) {}

    virtual ~EtcdStoragePlugin() {}

    virtual bool get(const std::string &key, Json::Value &value) {
        Json::Reader reader;
        auto resp = client_.get(key);
        if (!resp.is_ok()) {
            LOG(ERROR) << "EtcdStoragePlugin: unable to get " << key << " from "
                       << metadata_uri_ << ": " << resp.error_message();
            return false;
        }
        auto json_file = resp.value().as_string();
        if (!reader.parse(json_file, value)) return false;
        if (globalConfig().verbose)
            LOG(INFO) << "EtcdStoragePlugin: get: key=" << key
                      << ", value=" << json_file;
        return true;
    }

    virtual bool set(const std::string &key, const Json::Value &value) {
        Json::FastWriter writer;
        const std::string json_file = writer.write(value);
        if (globalConfig().verbose)
            LOG(INFO) << "EtcdStoragePlugin: set: key=" << key
                      << ", value=" << json_file;
        auto resp = client_.put(key, json_file);
        if (!resp.is_ok()) {
            LOG(ERROR) << "EtcdStoragePlugin: unable to set " << key << " from "
                       << metadata_uri_ << ": " << resp.error_message();
            return false;
        }
        return true;
    }

    virtual bool remove(const std::string &key) {
        auto resp = client_.rm(key);
        if (!resp.is_ok()) {
            LOG(ERROR) << "EtcdStoragePlugin: unable to delete " << key
                       << " from " << metadata_uri_ << ": "
                       << resp.error_message();
            return false;
        }
        return true;
    }

    etcd::SyncClient client_;
    const std::string metadata_uri_;
};

//上面三个都是元数据存储插件的实现，实现的基本功能包括增删改，
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

std::shared_ptr<MetadataStoragePlugin> MetadataStoragePlugin::Create(
    const std::string &conn_string) {
    auto parsed_conn_string = parseConnectionString(conn_string);
    if (parsed_conn_string.first == "etcd") {
        return std::make_shared<EtcdStoragePlugin>(parsed_conn_string.second);
#ifdef USE_REDIS
    } else if (parsed_conn_string.first == "redis") {
        return std::make_shared<RedisStoragePlugin>(parsed_conn_string.second);
#endif  // USE_REDIS
#ifdef USE_HTTP
    } else if (parsed_conn_string.first == "http" ||
               parsed_conn_string.first == "https") {
        return std::make_shared<HTTPStoragePlugin>(
            conn_string);  // including prefix
#endif                     // USE_HTTP
    } else {
        LOG(FATAL) << "Unable to find metadata storage plugin "
                   << parsed_conn_string.first;
        return nullptr;
    }
}


// getNetworkAddress 用于将一个通用的 sockaddr 结构体转换为可读的网络地址字符串
// 简单说就是获取ip地址和端口号
static inline const std::string getNetworkAddress(struct sockaddr *addr) {
    if (addr->sa_family == AF_INET) { // ipv4
        struct sockaddr_in *sock_addr = (struct sockaddr_in *)addr;
        char ip[INET_ADDRSTRLEN];
        if (inet_ntop(addr->sa_family, &(sock_addr->sin_addr), ip,
                      INET_ADDRSTRLEN) != NULL)
            return std::string(ip) + ":" +
                   std::to_string(ntohs(sock_addr->sin_port));
    } else if (addr->sa_family == AF_INET6) { // ipv6
        struct sockaddr_in6 *sock_addr = (struct sockaddr_in6 *)addr;
        char ip[INET6_ADDRSTRLEN];
        if (inet_ntop(addr->sa_family, &(sock_addr->sin6_addr), ip,
                      INET6_ADDRSTRLEN) != NULL)
            return std::string(ip) + ":" +
                   std::to_string(ntohs(sock_addr->sin6_port));
    }
    PLOG(ERROR) << "Failed to parse socket address";
    return "";
}
// SocketHandShakePlugin 是一个用于“节点间握手通信”的插件类，
// 主要作用是通过 TCP Socket 实现分布式系统中节点之间的身份交换、能力协商或连接建立等“握手”过程。

struct SocketHandShakePlugin : public HandShakePlugin {
    SocketHandShakePlugin() : listener_running_(false), listen_fd_(-1) {}

    void closeListen() {
        if (listen_fd_ >= 0) {
            LOG(INFO) << "SocketHandShakePlugin: closing listen socket";
            close(listen_fd_);
            listen_fd_ = -1;
        }
    }

    virtual ~SocketHandShakePlugin() {
        closeListen();
        if (listener_running_) {
            listener_running_ = false;
            listener_.join();
        }
    }

       /**
     * @brief 启动握手监听守护线程
     *
     * 该函数会在指定端口启动一个TCP监听socket，并在后台线程中循环等待其他节点的连接请求。
     * 每当有新连接到来时，会读取对方发送的JSON格式握手消息，调用回调函数处理，
     * 然后将本地的握手信息回传给对方，最后关闭连接。
     *
     * @param on_recv_callback 处理收到的握手消息的回调函数，类型为 void(const Json::Value &peer, Json::Value &local)
     * @param listen_port      监听的本地端口号
     * @return int             0表示启动成功，负数表示失败（如端口被占用、socket错误等）
     *
     * 主要流程：
     * 1. 创建socket并设置超时、端口复用等参数
     * 2. 绑定到指定端口并开始监听
     * 3. 启动后台线程，循环accept新连接
     * 4. 对每个连接：
     *    - 读取对方JSON握手消息
     *    - 调用回调处理，生成本地JSON握手消息
     *    - 回传本地JSON消息
     *    - 关闭连接
     * 5. 支持多连接并发处理，直到插件析构或关闭监听
     */
    virtual int startDaemon(OnReceiveCallBack on_recv_callback,
                            uint16_t listen_port) {
        sockaddr_in bind_address;                  // 定义一个 IPv4 地址结构体，用于绑定本地地址和端口
        int on = 1;                               // 用于后续 setsockopt 设置端口复用
        memset(&bind_address, 0, sizeof(sockaddr_in)); // 将 bind_address 结构体内容清零
        bind_address.sin_family = AF_INET;        // 设置地址族为 IPv4
        bind_address.sin_port = htons(listen_port); // 设置端口号（主机字节序转网络字节序）
        bind_address.sin_addr.s_addr = INADDR_ANY;   // 绑定到本机所有网卡（0.0.0.0）

        listen_fd_ = socket(AF_INET, SOCK_STREAM, 0); // 创建一个 TCP socket，返回文件描述符
        if (listen_fd_ < 0) {
            PLOG(ERROR) << "SocketHandShakePlugin: socket()";
            return ERR_SOCKET;
        }

        struct timeval timeout;
        timeout.tv_sec = 1;
        timeout.tv_usec = 0;
        // 设置监听socket的接收超时时间为1秒，防止accept等操作长时间阻塞
        if (setsockopt(listen_fd_, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                       sizeof(timeout))) {
            PLOG(ERROR) << "SocketHandShakePlugin: setsockopt(SO_RCVTIMEO)";
            closeListen();
            return ERR_SOCKET;
        }
        // 设置端口复用，允许端口被重复绑定（常用于服务重启时端口未完全释放的情况）
        if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) {
            PLOG(ERROR) << "SocketHandShakePlugin: setsockopt(SO_REUSEADDR)";
            closeListen();
            return ERR_SOCKET;
        }
// 绑定socket到本地指定端口和地址
        if (bind(listen_fd_, (sockaddr *)&bind_address, sizeof(sockaddr_in)) <
            0) {
            PLOG(ERROR) << "SocketHandShakePlugin: bind (port " << listen_port
                        << ")";
            closeListen();
            return ERR_SOCKET;
        }
// 开始监听端口，准备接受连接请求
        if (listen(listen_fd_, 5)) {
            PLOG(ERROR) << "SocketHandShakePlugin: listen()";
            closeListen();
            return ERR_SOCKET;
        }
// 这种“短连接”方式适合偶尔通信、数据量小、对实时性要求不高的场景。
// 如果需要高频率、持续的数据交换，建议使用长连接（即连接建立后多次收发数据，直到一方主动关闭）
// 这样可以显著提升效率和性能。
        listener_running_ = true;
        listener_ = std::thread([this, on_recv_callback]() {
            while (listener_running_) {
                sockaddr_in addr;
                socklen_t addr_len = sizeof(sockaddr_in);
                int conn_fd = accept(listen_fd_, (sockaddr *)&addr, &addr_len);
                if (conn_fd < 0) {
                    if (errno != EWOULDBLOCK)
                        PLOG(ERROR) << "SocketHandShakePlugin: accept()";
                    continue;
                }

                if (addr.sin_family != AF_INET && addr.sin_family != AF_INET6) {
                    LOG(ERROR) << "SocketHandShakePlugin: unsupported socket "
                                  "type, should be AF_INET or AF_INET6";
                    close(conn_fd);
                    continue;
                }

                struct timeval timeout;
                timeout.tv_sec = 60;
                timeout.tv_usec = 0;
                if (setsockopt(conn_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                               sizeof(timeout))) {
                    PLOG(ERROR)
                        << "SocketHandShakePlugin: setsockopt(SO_RCVTIMEO)";
                    close(conn_fd);
                    continue;
                }

                auto peer_hostname =
                    getNetworkAddress((struct sockaddr *)&addr);
                if (globalConfig().verbose)
                    LOG(INFO) << "SocketHandShakePlugin: new connection: "
                              << peer_hostname.c_str();

                Json::Value local, peer;
                Json::Reader reader;
                if (!reader.parse(readString(conn_fd), peer)) {
                    LOG(ERROR) << "SocketHandShakePlugin: failed to receive "
                                  "handshake message: "
                                  "malformed json format, check tcp connection";
                    close(conn_fd);
                    continue;
                }

                on_recv_callback(peer, local);
                int ret = writeString(conn_fd, Json::FastWriter{}.write(local));
                if (ret) {
                    LOG(ERROR) << "SocketHandShakePlugin: failed to send "
                                  "handshake message: "
                                  "malformed json format, check tcp connection";
                    close(conn_fd);
                    continue;
                }

                close(conn_fd);
            }
            return;
        });

        return 0;
    }
    // 这个函数 send 是一个用于发送数据到远程服务器的虚函数。
    // 它使用传输控制协议（TCP）通过套接字连接到指定的 IP 地址或主机名和端口，
    // 并发送 JSON 数据。
    virtual int send(std::string ip_or_host_name, uint16_t rpc_port,
                     const Json::Value &local, Json::Value &peer) {
// std::string ip_or_host_name：目标服务器的 IP 地址或主机名。
// uint16_t rpc_port：目标服务器的端口号。
// const Json::Value &local：要发送的 JSON 数据。
// Json::Value &peer：用于存储从服务器接收到的 JSON 数据。
        struct addrinfo hints;
        struct addrinfo *result, *rp;
        memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_INET;
        hints.ai_socktype = SOCK_STREAM;

        char service[16];
        sprintf(service, "%u", rpc_port);
        // 解析主机名和端口，获取可用的地址信息链表
        // result这个结果由DNS服务器返回，不用连接到目标地址才返回
        // 调用 getaddrinfo 将主机名或 IP 地址和端口号转换为地址信息列表。
        if (getaddrinfo(ip_or_host_name.c_str(), service, &hints, &result)) {
            PLOG(ERROR)
                << "SocketHandShakePlugin: failed to get IP address of peer "
                   "server "
                << ip_or_host_name << ":" << rpc_port
                << ", check DNS and /etc/hosts, or use IPv4 address instead";
            return ERR_DNS;
        }

        int ret = 0;
        //         某个主机可能同时支持 IPv4 和 IPv6。
// 某个服务器可能有多个 IP 地址绑定在同一个端口上（比如多网卡或多宿主主机）。
// DNS 解析可能会返回多个 A 记录（IPv4）或 AAAA 记录（IPv6）。
// 这个就是找到其中合适的一个进行发送
// 遍历 result 链表中每个 addrinfo 结构体，尝试连接和发送数据。
// 调用 doSend 函数执行实际的发送操作。
        for (rp = result; rp; rp = rp->ai_next) {
            // 尝试用当前地址发送消息
            ret = doSend(rp, local, peer);
            if (ret == 0) {
                freeaddrinfo(result);// 成功则释放资源并返回
                return 0;
            }
            if (ret == ERR_MALFORMED_JSON) {// 如果是JSON格式错误，直接返回
                return ret;
            }
        }

        freeaddrinfo(result);
        return ret;
    }

    // doSend 负责通过套接字连接到指定的地址并发送 JSON 数据，同时接收服务器的响应。
    int doSend(struct addrinfo *addr, const Json::Value &local,
               Json::Value &peer) {
        if (globalConfig().verbose)
            LOG(INFO) << "SocketHandShakePlugin: connecting "
                      << getNetworkAddress(addr->ai_addr);

        int on = 1;
        int conn_fd =
            socket(addr->ai_family, addr->ai_socktype, addr->ai_protocol);
        if (conn_fd == -1) {
            PLOG(ERROR) << "SocketHandShakePlugin: socket()";
            return ERR_SOCKET;
        }
        if (setsockopt(conn_fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on))) {
            PLOG(ERROR) << "SocketHandShakePlugin: setsockopt(SO_REUSEADDR)";
            close(conn_fd);
            return ERR_SOCKET;
        }

        struct timeval timeout;
        timeout.tv_sec = 60;
        timeout.tv_usec = 0;
        if (setsockopt(conn_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout,
                       sizeof(timeout))) {
            PLOG(ERROR) << "SocketHandShakePlugin: setsockopt(SO_RCVTIMEO)";
            close(conn_fd);
            return ERR_SOCKET;
        }

        if (connect(conn_fd, addr->ai_addr, addr->ai_addrlen)) {
            PLOG(ERROR) << "SocketHandShakePlugin: connect()"
                        << getNetworkAddress(addr->ai_addr);
            close(conn_fd);
            return ERR_SOCKET;
        }

        int ret = writeString(conn_fd, Json::FastWriter{}.write(local));
        if (ret) {
            LOG(ERROR)
                << "SocketHandShakePlugin: failed to send handshake message: "
                   "malformed json format, check tcp connection";
            close(conn_fd);
            return ret;
        }

        Json::Reader reader;
        // 写入获取到的结果
        if (!reader.parse(readString(conn_fd), peer)) {
            LOG(ERROR) << "SocketHandShakePlugin: failed to receive handshake "
                          "message: "
                          "malformed json format, check tcp connection";
            close(conn_fd);
            return ERR_MALFORMED_JSON;
        }

        close(conn_fd);
        return 0;
    }

    std::atomic<bool> listener_running_;
    std::thread listener_;
    int listen_fd_;
};

std::shared_ptr<HandShakePlugin> HandShakePlugin::Create(
    const std::string &conn_string) {
    return std::make_shared<SocketHandShakePlugin>();
}

}  // namespace mooncake