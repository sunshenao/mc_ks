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

#include "config.h"

namespace mooncake {

 /**
 * @brief 加载全局配置参数
 * @param config 配置结构体引用
 *
 * 该函数从环境变量读取配置参数：
 * - 如果环境变量存在且值合法，则使用环境变量的值
 * - 否则保持默认值不变
 * - 对于非法值会记录警告日志
 */
void loadGlobalConfig(GlobalConfig &config) {
    // 每个上下文的完成队列数量
    const char *num_cq_per_ctx_env = std::getenv("MC_NUM_CQ_PER_CTX");
    if (num_cq_per_ctx_env) {
        int val = atoi(num_cq_per_ctx_env);
        // 验证值的合法性（大于0且小于256）
        if (val > 0 && val < 256)
            config.num_cq_per_ctx = val;
        else
            LOG(WARNING)
                << "Ignore value from environment variable MC_NUM_CQ_PER_CTX";
    }

    // 每个上下文的完成通道数量
    const char *num_comp_channels_per_ctx_env =
        std::getenv("MC_NUM_COMP_CHANNELS_PER_CTX");
    if (num_comp_channels_per_ctx_env) {
        int val = atoi(num_comp_channels_per_ctx_env);
        if (val > 0 && val < 256)
            config.num_comp_channels_per_ctx = val;
        else
            LOG(WARNING) << "Ignore value from environment variable "
                            "MC_NUM_COMP_CHANNELS_PER_CTX";
    }

    // IB端口号
    const char *port_env = std::getenv("MC_IB_PORT");
    if (port_env) {
        int val = atoi(port_env);
        if (val >= 0 && val < 256)
            config.port = uint8_t(val);
        else
            LOG(WARNING) << "Ignore value from environment variable MC_IB_PORT";
    }

    // GID索引，先尝试MC_GID_INDEX，如果不存在则尝试NCCL_IB_GID_INDEX
    const char *gid_index_env = std::getenv("MC_GID_INDEX");
    if (!gid_index_env) gid_index_env = std::getenv("NCCL_IB_GID_INDEX");

    if (gid_index_env) {
        int val = atoi(gid_index_env);
        if (val >= 0 && val < 256)
            config.gid_index = val;
        else
            LOG(WARNING)
                << "Ignore value from environment variable MC_GID_INDEX";
    }

    // 每个上下文的最大完成队列项数
    const char *max_cqe_per_ctx_env = std::getenv("MC_MAX_CQE_PER_CTX");
    if (max_cqe_per_ctx_env) {
        size_t val = atoi(max_cqe_per_ctx_env);
        if (val > 0 && val <= UINT16_MAX)
            config.max_cqe = val;
        else
            LOG(WARNING)
                << "Ignore value from environment variable MC_MAX_CQE_PER_CTX";
    }

    // 每个上下文的最大端点数
    const char *max_ep_per_ctx_env = std::getenv("MC_MAX_EP_PER_CTX");
    if (max_ep_per_ctx_env) {
        size_t val = atoi(max_ep_per_ctx_env);
        if (val > 0 && val <= UINT16_MAX)
            config.max_ep_per_ctx = val;
        else
            LOG(WARNING)
                << "Ignore value from environment variable MC_MAX_EP_PER_CTX";
    }

    // 每个端点的队列对数量
    const char *num_qp_per_ep_env = std::getenv("MC_NUM_QP_PER_EP");
    if (num_qp_per_ep_env) {
        int val = atoi(num_qp_per_ep_env);
        if (val > 0 && val < 256)
            config.num_qp_per_ep = val;
        else
            LOG(WARNING)
                << "Ignore value from environment variable MC_NUM_QP_PER_EP";
    }

    // 最大分散/聚集元素数量
    const char *max_sge_env = std::getenv("MC_MAX_SGE");
    if (max_sge_env) {
        size_t val = atoi(max_sge_env);
        if (val > 0 && val <= UINT16_MAX)
            config.max_sge = val;
        else
            LOG(WARNING) << "Ignore value from environment variable MC_MAX_SGE";
    }

    // 最大工作请求数量
    const char *max_wr_env = std::getenv("MC_MAX_WR");
    if (max_wr_env) {
        size_t val = atoi(max_wr_env);
        if (val > 0 && val <= UINT16_MAX)
            config.max_wr = val;
        else
            LOG(WARNING) << "Ignore value from environment variable MC_MAX_WR";
    }

    // 最大内联数据大小
    const char *max_inline_env = std::getenv("MC_MAX_INLINE");
    if (max_inline_env) {
        size_t val = atoi(max_inline_env);
        if (val > 0 && val <= UINT16_MAX)
            config.max_inline = val;
        else
            LOG(WARNING) << "Ignore value from environment variable MC_MAX_INLINE";
    }

    // 网络MTU大小
    const char *mtu_env = std::getenv("MC_MTU");
    if (mtu_env) {
        int val = atoi(mtu_env);
        switch (val) {
            case 256:
                config.mtu_length = IBV_MTU_256;
                break;
            case 512:
                config.mtu_length = IBV_MTU_512;
                break;
            case 1024:
                config.mtu_length = IBV_MTU_1024;
                break;
            case 2048:
                config.mtu_length = IBV_MTU_2048;
                break;
            case 4096:
                config.mtu_length = IBV_MTU_4096;
                break;
            default:
                LOG(WARNING) << "Ignore value from environment variable MC_MTU";
                break;
        }
    }

    // 握手端口号
    const char *handshake_port_env = std::getenv("MC_HANDSHAKE_PORT");
    if (handshake_port_env) {
        int val = atoi(handshake_port_env);
        if (val > 0 && val < 65536)
            config.handshake_port = uint16_t(val);
        else
            LOG(WARNING)
                << "Ignore value from environment variable MC_HANDSHAKE_PORT";
    }

    // 每个上下文的工作线程数
    const char *workers_per_ctx_env = std::getenv("MC_WORKERS_PER_CTX");
    if (workers_per_ctx_env) {
        int val = atoi(workers_per_ctx_env);
        if (val > 0 && val < 256)
            config.workers_per_ctx = val;
        else
            LOG(WARNING)
                << "Ignore value from environment variable MC_WORKERS_PER_CTX";
    }

    // 调试日志开关
    const char *verbose_env = std::getenv("MC_VERBOSE");
    if (verbose_env) {
        int val = atoi(verbose_env);
        config.verbose = (val != 0);
    }

    // 传输切片大小
    const char *slice_size_env = std::getenv("MC_SLICE_SIZE");
    if (slice_size_env) {
        size_t val = atoi(slice_size_env);
        if (val > 0 && val <= UINT32_MAX)
            config.slice_size = val;
        else
            LOG(WARNING) << "Ignore value from environment variable MC_SLICE_SIZE";
    }

    // 传输重试次数
    const char *retry_cnt_env = std::getenv("MC_RETRY_CNT");
    if (retry_cnt_env) {
        int val = atoi(retry_cnt_env);
        if (val >= 0 && val < 256)
            config.retry_cnt = val;
        else
            LOG(WARNING) << "Ignore value from environment variable MC_RETRY_CNT";
    }

    const char *verbose_env = std::getenv("MC_VERBOSE");
    if (verbose_env) {
        config.verbose = true;
    }
}

std::string mtuLengthToString(ibv_mtu mtu) {
    if (mtu == IBV_MTU_512)
        return "IBV_MTU_512";
    else if (mtu == IBV_MTU_1024)
        return "IBV_MTU_1024";
    else if (mtu == IBV_MTU_2048)
        return "IBV_MTU_2048";
    else if (mtu == IBV_MTU_4096)
        return "IBV_MTU_4096";
    else
        return "UNKNOWN";
}

/**
 * @brief 根据设备属性更新全局配置
 * @param device_attr RDMA设备属性
 *
 * 根据实际硬件能力调整配置参数：
 * - 如果硬件支持的值小于配置值，则使用硬件支持的最大值
 * - 主要涉及队列长度、并发数等参数
 */
void updateGlobalConfig(ibv_device_attr &device_attr) {
    auto &config = globalConfig();
    if (config.max_ep_per_ctx * config.num_qp_per_ep >
        (size_t)device_attr.max_qp)
        config.max_ep_per_ctx = device_attr.max_qp / config.num_qp_per_ep;
    if (config.num_cq_per_ctx > (size_t)device_attr.max_cq)
        config.num_cq_per_ctx = device_attr.max_cq;
    if (config.max_wr > (size_t)device_attr.max_qp_wr)
        config.max_wr = device_attr.max_qp_wr;
    if (config.max_sge > (size_t)device_attr.max_sge)
        config.max_sge = device_attr.max_sge;
    if (config.max_cqe > (size_t)device_attr.max_cqe)
        config.max_cqe = device_attr.max_cqe;
}

/**
 * @brief 输出全局配置的当前值
 *
 * 将所有配置参数的值打印到日志，用于调试和排障
 */
void dumpGlobalConfig() {
    auto &config = globalConfig();
    LOG(INFO) << "=== GlobalConfig ===";
    LOG(INFO) << "num_cq_per_ctx = " << config.num_cq_per_ctx;
    LOG(INFO) << "num_comp_channels_per_ctx = "
              << config.num_comp_channels_per_ctx;
    LOG(INFO) << "port = " << config.port;
    LOG(INFO) << "gid_index = " << config.gid_index;
    LOG(INFO) << "max_cqe = " << config.max_cqe;
    LOG(INFO) << "max_ep_per_ctx = " << config.max_ep_per_ctx;
    LOG(INFO) << "num_qp_per_ep = " << config.num_qp_per_ep;
    LOG(INFO) << "max_sge = " << config.max_sge;
    LOG(INFO) << "max_wr = " << config.max_wr;
    LOG(INFO) << "max_inline = " << config.max_inline;
    LOG(INFO) << "mtu_length = " << mtuLengthToString(config.mtu_length);
    LOG(INFO) << "verbose = " << (config.verbose ? "true" : "false");
}

/**
 * @brief 获取全局配置单例
 * @return 全局配置结构体引用
 */
GlobalConfig &globalConfig() {
    static GlobalConfig config;
    static std::once_flag g_once_flag;
    std::call_once(g_once_flag, []() { loadGlobalConfig(config); });
    return config;
}

/**
 * @brief 获取默认握手端口号
 * @return 端口号
 */
uint16_t getDefaultHandshakePort() {
    return globalConfig().handshake_port;
}
}  // namespace mooncake