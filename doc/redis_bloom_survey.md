# RedisBloom 技术调研报告

> 调研日期: 2026-04-20

## 1. 概述

**RedisBloom** 是 Redis 的概率数据结构模块，提供以极小内存开销实现 O(1) 时间复杂度的近似查询能力。由 Redis Ltd.（原 RedisLabs）开发和维护，托管在 [github.com/RedisBloom/RedisBloom](https://github.com/RedisBloom/RedisBloom)。

### 1.1 与 Redis 的关系

- 历史上作为独立可加载模块存在（产出 `redisbloom.so`）
- 从 **Redis 8**（2024/2025 GA）开始，所有 RedisBloom 数据结构直接集成到 Redis Server 中，不再发布独立版本
- Redis Stack（之前的捆绑分发方式）已废弃，由 Redis 8 原生集成取代

### 1.2 License 变迁

| 阶段 | 时间 | License | 范围 |
|------|------|---------|------|
| 阶段 1 | 2018-2022 | RSALv1 | RedisBloom 及其他模块 |
| 阶段 2 | 2022.11 | RSALv2 + SSPLv1（双许可） | Redis Stack 模块；Redis 核心仍为 BSD-3 |
| 阶段 3 | 2024.03 | RSALv2 + SSPLv1（双许可） | **Redis 核心本身**也改为此许可（Redis 7.4+），引发 Valkey fork |
| 阶段 4 | 2025（Redis 8） | RSALv2 + SSPLv1 + **AGPLv3**（三选一） | Redis 8 含所有集成模块。AGPLv3 是 OSI 认证的开源许可 |

---

## 2. 数据结构与命令

### 2.1 Bloom Filter（BF.*）

空间高效的概率数据结构，用于集合成员判定。可能返回 false positive，但**绝不**返回 false negative。**不支持删除**。

| 命令 | 说明 | 时间复杂度 |
|------|------|-----------|
| `BF.RESERVE` | 创建 Bloom Filter，指定 error_rate、capacity、可选 EXPANSION/NONSCALING | O(1) |
| `BF.ADD` | 添加单个元素（key 不存在时自动以默认参数创建） | O(k) |
| `BF.MADD` | 批量添加多个元素 | O(k * n) |
| `BF.INSERT` | 不存在则创建（可自定义参数），然后批量添加 | O(k * n) |
| `BF.EXISTS` | 检查元素是否可能存在 | O(k) |
| `BF.MEXISTS` | 批量检查多个元素 | O(k * n) |
| `BF.SCANDUMP` | 增量序列化 filter 用于迁移/备份 | O(n) |
| `BF.LOADCHUNK` | 从 SCANDUMP 输出恢复 filter | O(n) |
| `BF.INFO` | 返回 filter 元数据（capacity、size、子filter数、已插入数、expansion） | O(1) |
| `BF.CARD` | 返回基数（已添加的唯一元素数量） | O(1) |

*其中 k = 哈希函数数量，n = 命令中的元素数量*

### 2.2 Cuckoo Filter（CF.*）

类似 Bloom Filter 但支持**删除**和**计数**。使用 cuckoo hashing + fingerprint 存储在 bucket 中。

| 命令 | 说明 | 时间复杂度 |
|------|------|-----------|
| `CF.RESERVE` | 创建 Cuckoo Filter，可选 BUCKETSIZE/MAXITERATIONS/EXPANSION | O(1) |
| `CF.ADD` | 添加元素（允许重复） | O(k + i) |
| `CF.ADDNX` | 仅在不存在时添加 | O(k + i) |
| `CF.INSERT` | 批量添加（自动创建） | O(n * (k + i)) |
| `CF.INSERTNX` | 批量"不存在则添加" | O(n * (k + i)) |
| `CF.EXISTS` | 检查元素是否可能存在 | O(k) |
| `CF.MEXISTS` | 批量检查 | O(k * n) |
| `CF.DEL` | 删除一个元素的一次出现 | O(k) |
| `CF.COUNT` | 统计元素可能在 filter 中的次数 | O(k) |
| `CF.SCANDUMP` | 增量序列化 | O(n) |
| `CF.LOADCHUNK` | 从 SCANDUMP 恢复 | O(n) |
| `CF.INFO` | 返回 filter 元数据 | O(1) |

*其中 k = 子 filter 数量，i = maxIterations*

> **警告**: 绝不要对未添加过的元素调用 `CF.DEL`，这会导致 filter 损坏并产生 false negative。

### 2.3 Count-Min Sketch（CMS.*）

估计数据流中事件的频率。使用亚线性空间。**只会高估，不会低估**。

| 命令 | 说明 | 时间复杂度 |
|------|------|-----------|
| `CMS.INITBYDIM` | 通过直接指定 width 和 depth 初始化 | O(1) |
| `CMS.INITBYPROB` | 通过指定误差和概率上界初始化 | O(1) |
| `CMS.INCRBY` | 增加一个或多个元素的计数 | O(n) |
| `CMS.QUERY` | 查询一个或多个元素的估计频率 | O(n) |
| `CMS.MERGE` | 合并多个 sketch 为一个 | O(n) |
| `CMS.INFO` | 返回 width、depth、总计数 | O(1) |

### 2.4 Top-K（TOPK.*）

从流中近似获取 K 个最高频元素。基于 **HeavyKeeper** 算法。

| 命令 | 说明 | 时间复杂度 |
|------|------|-----------|
| `TOPK.RESERVE` | 用 k、width、depth、decay 初始化 | O(1) |
| `TOPK.ADD` | 添加一个或多个元素；返回被淘汰的元素 | O(n * k) |
| `TOPK.INCRBY` | 增加一个或多个元素的分数 | O(n * k * incr) |
| `TOPK.QUERY` | 检查元素是否在 top-K 中 | O(n) |
| `TOPK.COUNT` | 返回元素计数（**已废弃** — 计数为概率性的，不可靠） | O(n) |
| `TOPK.LIST` | 返回完整 top-K 列表 | O(k * log(k)) |
| `TOPK.INFO` | 返回 k、width、depth、decay | O(1) |

### 2.5 t-digest（TDIGEST.*）

从流式浮点数据中估计分位数和百分位数。基于 Ted Dunning 的 t-digest 算法。RedisBloom 2.4 新增。

| 命令 | 说明 | 时间复杂度 |
|------|------|-----------|
| `TDIGEST.CREATE` | 创建新的 t-digest sketch（可选 COMPRESSION） | O(1) |
| `TDIGEST.ADD` | 添加一个或多个观测值 | O(N) |
| `TDIGEST.MERGE` | 合并多个 t-digest | O(N * K) |
| `TDIGEST.RESET` | 清空并重新初始化 | O(1) |
| `TDIGEST.QUANTILE` | 估计给定分位数处的值 | O(1) |
| `TDIGEST.CDF` | 估计 <= 给定值的观测比例 | O(1) |
| `TDIGEST.RANK` | 估计值的排名 | O(N) |
| `TDIGEST.REVRANK` | 估计值的反向排名 | O(N) |
| `TDIGEST.BYRANK` | 估计给定排名处的值 | O(N) |
| `TDIGEST.BYREVRANK` | 估计给定反向排名处的值 | O(N) |
| `TDIGEST.TRIMMED_MEAN` | 指定分位数范围内的均值（忽略离群值） | — |
| `TDIGEST.MIN` | 返回最小观测值 | O(1) |
| `TDIGEST.MAX` | 返回最大观测值 | O(1) |
| `TDIGEST.INFO` | 返回观测数、合并数、压缩参数、内存使用 | O(1) |

> 注意: 与其他 RedisBloom 结构不同，`TDIGEST.ADD` **不会**自动创建 sketch，必须先调用 `TDIGEST.CREATE`。

---

## 3. 架构与实现细节

### 3.1 Scalable Bloom Filter 内部实现

RedisBloom 实现了 2007 年 Almeida、Baquero、Preguica 和 Hutchison 论文中的 **Scalable Bloom Filter** 设计。

**核心数据结构**: `SBChain` — 一个链式的 Bloom Filter 层（子 filter）链表。通过 `SB_NewChain(capacity, error_rate, options, expansion)` 创建。

**工作原理**:
- Bloom Filter 是一个 m 位的位数组，配合 k 个哈希函数将元素映射到位位置
- 当初始子 filter 达到容量时，在其之上**堆叠一个新的子 filter**
- 新子 filter 的容量 = 前一个容量 × EXPANSION 因子（默认 2）
- 每个后续子 filter 使用**收紧比率**（默认 0.5），即每一层的错误率是上一层的**一半**，需要更多哈希函数
- 这确保整体 false positive rate 接近用户指定的目标

**查找**: 从最新层开始检查所有层。任意一层返回 "yes" 则回答 "可能存在"；所有层都返回 "no" 则回答 "确定不存在"。复杂度: O(k * n_layers)。

**插入**: 先检查所有层是否存在。若不存在，添加到最顶层（当前层）。若当前层已满，先创建新层。

**子 filter 限制**: 子 filter 数量受错误率和收紧比率限制。若新子 filter 所需的错误率为 0，命令会被拒绝。

### 3.2 哈希函数

使用 **MurmurHash2** — Austin Appleby 编写的快速非加密哈希函数（公共领域）。

```c
#define HASH(item, itemlen, i) MurmurHash2(item, itemlen, i)
```

其中 `i` 是种子值。通过不同种子值（0, 1, 2, ..., k-1）调用 MurmurHash2 来模拟多个哈希函数。与 Kirsch-Mitzenmacher 优化相关（"Less Hashing, Same Performance"）。

**最优哈希函数数量**: `k = ceil(-ln(error_rate) / ln(2))`

| 错误率 | 哈希函数数 (k) | 每元素位数 |
|--------|---------------|-----------|
| 1% | 7 | 9.585 |
| 0.1% | 10 | 14.378 |
| 0.01% | 14 | 19.170 |

### 3.3 内存布局

- **Bloom Filter**: 元数据（error_rate, capacity, expansion_factor, 子filter数量）+ 每个子filter（hash_count, entry_count, bit_count, 连续内存的原始位数组）
- **Cuckoo Filter**: bucket 数组，每个 bucket 包含固定数量的 fingerprint 槽位（BUCKETSIZE，默认 2）。容量向上取整为 2^n
- **Count-Min Sketch**: width × depth 的二维计数器数组
- **Top-K**: k、width、depth、decay 的内部数组 + HeavyKeeper 数据结构
- **t-digest**: centroid 数组（mean + weight），来自外部 `deps/t-digest-c` 库

### 3.4 持久化（RDB / AOF）

通过 `RedisModule_CreateDataType()` 注册自定义数据类型，提供 `rdb_save` / `rdb_load` 回调。

- **RDB**: 将完整内部状态（位数组、元数据、计数器）序列化为 RDB 文件中的不透明二进制 blob。紧凑 — Bloom Filter 只存储位数组和结构化元数据，不存储原始元素
- **AOF**: 每个写命令（`BF.ADD`, `CF.ADD`, `CMS.INCRBY` 等）原样记录。重启时 Redis 回放所有命令。对于 Bloom Filter，AOF 文件可能非常大
- **SCANDUMP/LOADCHUNK**: 用于迁移场景的增量序列化/反序列化

---

## 4. 关键配置参数

### Bloom Filter 默认值（Redis 8+）

| 参数 | 说明 | 有效范围 | 默认值 |
|------|------|---------|--------|
| `bf-default-error` | 默认 false positive 率 | 0 < x < 1 | 0.01 (1%) |
| `bf-default-capacity` | 默认初始容量 | [1 .. 1048576] | 100 |
| `bf-default-expansion` | 默认扩展因子 | [0 .. 32768] | 2 |

### Cuckoo Filter 默认值（Redis 8+）

| 参数 | 说明 | 有效范围 | 默认值 |
|------|------|---------|--------|
| `cf-default-capacity` | 默认初始容量 | [2*bucket_size .. 1048576] | 1024 |
| `cf-bucket-size` | 每个 bucket 中的元素数 | [1 .. 255] | 2 |
| `cf-max-iterations` | 满时最大交换尝试次数 | [1 .. 65535] | 20 |
| `cf-default-expansion` | 子 filter 扩展因子 | [0 .. 32768] | 1 |
| `cf-max-expansions` | 允许的最大扩展次数 | [1 .. 65535] | 32 |

### 每命令参数

- **BF.RESERVE**: `key error_rate capacity [EXPANSION expansion] [NONSCALING]`
  - `NONSCALING`: 阻止自动扩展；少用一个哈希函数但满时返回错误
- **CF.RESERVE**: `key capacity [BUCKETSIZE bucketsize] [MAXITERATIONS maxiterations] [EXPANSION expansion]`
- **TDIGEST.CREATE**: `key [COMPRESSION compression]`
  - `COMPRESSION`: 默认 100。更高值（如 1000）提高精度但增加内存

---

## 5. 版本历史与近期变更

| 版本 | 日期 | 主要变更 |
|------|------|---------|
| 2.4 GA | 2022 | 新增 **t-digest** 数据结构 |
| 2.6 GA | 2023.07 | 新增 **RESP3** 协议支持 |
| 2.8 GA | 2024.07 | Bug 修复、维护 |
| 2.8.5 | 2025.01 | 安全修复 |
| 2.8.16 | 2025 晚期 | 安全修复: Cuckoo filter 除零错误、计数器溢出修复 |
| 2.8.17 | 最新 | 修复加载无效 RDB 文件崩溃、无效 RDB 恢复内存泄漏 |

> 从 Redis 8 起，RedisBloom 已集成到 Redis 中，不再有独立发布。最后的独立分支为 **2.8.x**。

---

## 6. 性能特征

### 时间复杂度汇总

| 操作 | 复杂度 | 备注 |
|------|--------|------|
| BF.ADD / BF.EXISTS | O(k) | k = 哈希函数数（1% 错误率时为 7） |
| BF.EXISTS（N 个子 filter） | O(k * N) | 随扩展退化 |
| CF.ADD | O(k + i) | i = 碰撞时的 maxIterations |
| CF.EXISTS / CF.DEL | O(k) | k = 子 filter 数量 |
| CMS.INCRBY / CMS.QUERY | O(n) | n = 命令中的元素数 |
| TOPK.ADD | O(n * k) | k = top-K 参数 |
| TDIGEST.QUANTILE | O(1) | 查找非常快 |
| TDIGEST.ADD | O(N) | N = 添加的观测值数 |

### 内存使用基准（社区数据）

| 容量 | 已插入数 | 内存使用 | 插入 RPS | 平均延迟 |
|------|---------|---------|---------|---------|
| 7M | ~6.77M | ~17.2 MB | ~16,331 | 1.22 ms |
| 15M | ~10M | ~36.85 MB | ~23,064 | 1.73 ms |
| 30M | ~16.6M | ~73.7 MB | — | — |

**每元素内存由错误率决定**: ~9.6 bits/item @ 1%, ~14.4 bits/item @ 0.1%, ~19.2 bits/item @ 0.01%。

### 扩展性能影响

- expansion=1, 初始容量 100K，达到 10M 元素需 **100 个子 filter**（99 次扩展）
- expansion=2, 初始容量 100K，达到 ~12.7M 元素只需 **7 个子 filter**
- 超过 ~100 个子 filter 会导致明显的性能退化

---

## 7. 源码结构

**仓库**: [github.com/RedisBloom/RedisBloom](https://github.com/RedisBloom/RedisBloom)

### 核心源文件（`src/`）

| 文件 | 用途 |
|------|------|
| `rebloom.c` | 主模块入口。注册所有 BF.* 和 CF.* 的 Redis 命令 |
| `sb.c` / `sb.h` | **Scalable Bloom Filter** 核心: `SBChain` 结构、子filter创建、位数组操作、哈希计算 |
| `cf.c` / `cf.h` | **Cuckoo Filter** 核心: bucket 管理、fingerprint、cuckoo 驱逐 |
| `rm_cms.c` / `rm_cms.h` | **Count-Min Sketch** Redis 命令封装 |
| `rm_topk.c` / `rm_topk.h` | **Top-K** Redis 命令封装 |
| `topk.c` | Top-K 算法核心（HeavyKeeper） |
| `rm_tdigest.c` / `rm_tdigest.h` | **t-digest** Redis 命令封装 |
| `config.h` / `config.c` | 模块配置 |

### 依赖（`deps/`）

| 依赖 | 用途 |
|------|------|
| `murmur2/` | MurmurHash2 哈希实现 |
| `t-digest-c/` | t-digest C 库 |
| `RedisModulesSDK/` | Redis Module API SDK |

---

## 8. 已知限制与问题

1. **Bloom Filter 不支持删除**: 元素无法从 Bloom Filter 中移除。如需删除功能，使用 Cuckoo Filter
2. **False positive 是固有的**: Bloom 和 Cuckoo Filter 始终有非零的 false positive 率
3. **子 filter 堆叠降低性能**: 初始容量不足时创建子 filter，每个额外子 filter 增加查找延迟。超过 ~100 个子 filter 会显著退化
4. **大容量 BF.RESERVE 的 OOM 风险**: 极大容量值（如 10^18）可能导致 Redis OOM 崩溃，无内置最大内存限制（[GitHub #699](https://github.com/RedisBloom/RedisBloom/issues/699)）
5. **CF.DEL 损坏风险**: 删除未添加过的元素会损坏 filter 并导致 false negative
6. **Cuckoo Filter 内存报告不准确**: 非常大的 Cuckoo Filter（如容量 2^32）的 `MEMORY USAGE` 可能不准确（[GitHub #609](https://github.com/RedisBloom/RedisBloom/issues/609)）
7. **TOPK.COUNT 已废弃**: 返回的计数是概率性的，不可靠
8. **TDIGEST.ADD 不自动创建**: 必须先调用 `TDIGEST.CREATE`
9. **RDB 安全漏洞**: 多个与加载畸形 RDB 数据相关的 CVE（崩溃、内存泄漏、任意内存访问），已在 v2.8.5+ 和 Redis 8.0.6 中修复
10. **Cuckoo Filter 错误率不可直接配置**: 不像 Bloom Filter 可设置精确的 error_rate，Cuckoo Filter 的错误率是 bucket size 的函数（bucket size 为 1 时最低约 0.78%）

---

## 9. C++ 重写注意事项

基于以上调研，C++ 重写时需要关注的关键点:

1. **哈希函数**: 原实现使用 MurmurHash2，可考虑使用 xxHash 或保持 MurmurHash2 以保证兼容性
2. **内存管理**: 原实现使用 Redis 的内存分配器（`RedisModule_Alloc/Free`），C++ 重写需设计自己的内存管理策略
3. **可扩展性**: Scalable Bloom Filter 的子 filter 链式结构是核心，需正确实现收紧比率和扩展因子
4. **序列化**: 如需与 Redis 兼容，需实现 SCANDUMP/LOADCHUNK 的二进制格式
5. **线程安全**: 原模块依赖 Redis 的单线程模型，C++ 重写若需多线程访问需额外考虑并发控制
6. **模板化设计**: C++ 可利用模板为不同的概率数据结构提供统一接口

---

## 参考资料

- [RedisBloom GitHub 仓库](https://github.com/RedisBloom/RedisBloom)
- [Redis Bloom Filter 文档](https://redis.io/docs/latest/develop/data-types/probabilistic/bloom-filter/)
- [Redis Cuckoo Filter 文档](https://redis.io/docs/latest/develop/data-types/probabilistic/cuckoo-filter/)
- [Redis Count-Min Sketch 文档](https://redis.io/docs/latest/develop/data-types/probabilistic/count-min-sketch/)
- [Redis Top-K 文档](https://redis.io/docs/latest/develop/data-types/probabilistic/top-k/)
- [Redis t-digest 文档](https://redis.io/docs/latest/develop/data-types/probabilistic/t-digest/)
- [Redis 概率数据结构配置参数](https://redis.io/docs/latest/develop/data-types/probabilistic/configuration/)
- [Scalable Bloom Filters 论文 (Almeida et al., 2007)](https://gsd.di.uminho.pt/members/cbm/ps/dbloom.pdf)
- [HeavyKeeper 论文](https://www.usenix.org/system/files/conference/atc18/atc18-gong.pdf)
