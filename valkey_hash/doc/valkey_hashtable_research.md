# Valkey Hashtable 设计调研

## 1. 概述

Valkey 是 Redis 的社区分支，在核心数据结构上做了大量现代化改造。其中最重要的变化之一是用全新的 **cache-line-aware hashtable** 替代了 Redis 传统的 **dict**（基于链表的哈希表）。本文档对两者的设计思想进行深入分析和对比。

---

## 2. Redis Dict 实现分析

### 2.1 核心数据结构

```c
struct dict {
    dictType *type;
    dictEntry **ht_table[2];       // 两张指针数组表
    unsigned long ht_used[2];      // 每张表的条目数
    long rehashidx;                // -1 表示未在 rehash
    unsigned pauserehash;          // >0 表示 rehash 暂停
    signed char ht_size_exp[2];    // 指数: size = 1 << exp
    int16_t pauseAutoResize;
    void *metadata[];              // 柔性数组，用于用户元数据
};
```

```c
struct dictEntry {
    struct dictEntry *next;   // 链表链接指针 (8 字节)
    void *key;                // 键指针 (8 字节)
    union {
        void *val;
        uint64_t u64;
        int64_t s64;
        double d;
    } v;                      // 值联合体 (8 字节)
};
// 总计: 每个 entry 24 字节 (64位系统)
```

另外还有 `dictEntryNoValue`（16 字节），用于 set 模式（`no_value=1`）的字典，省略了 value 联合体。

### 2.2 表布局

表本身是一个 `dictEntry*` 指针的平坦数组。每个 bucket 是一个指针（8 字节），指向一个单链表的头节点。表大小始终是 2 的幂。

```
ht_table[0] -> [ptr|ptr|ptr|ptr|...]
                 |       |
                 v       v
               dictEntry  dictEntry
                 |
                 v
               dictEntry (通过 ->next 链接)
```

### 2.3 冲突解决：链地址法

使用**单链表的分离链接法**。每个 bucket 指向一个 `dictEntry` 链表。新条目插入到链表头部。查找时需要沿 `->next` 指针遍历，每次跳转都可能是一次缓存未命中（cache miss）。

### 2.4 哈希函数

使用 SipHash，带 128 位种子（`dict_hash_function_seed[16]`）。同时提供大小写不敏感的变体 `siphash_nocase`。

### 2.5 增量 Rehash

- 使用**两张表**（`ht_table[0]` 和 `ht_table[1]`）
- Rehash 启动时，分配新表并将 `rehashidx` 设为 0
- `dictRehash(d, n)` 每次最多迁移 `n` 个 bucket，最多访问 `n*10` 个空 bucket 以限制工作量
- 每次操作（查找、添加、删除）通过 `_dictRehashStepIfNeeded` 触发一次 rehash 步骤
  - 优化：如果当前访问的 bucket 索引 >= `rehashidx` 且非空，优先 rehash 该 bucket（缓存友好）
  - 否则回退到 `dictRehash(d, 1)`
- `dictRehashMicroseconds` 以 100 步为一批，在时间限制内执行 rehash
- 缩容时，新索引通过与较小表的掩码按位与得到（无需重新计算哈希）
- 扩容时，必须为每个 entry 重新计算哈希

### 2.6 指针标记优化 (no_value 字典)

对于 set 模式字典（`no_value=1`），Redis 使用指针最低 3 位做标记：

| 标记位 | 含义 |
|--------|------|
| `000` (ENTRY_PTR_NORMAL) | 指向 dictEntry 结构体 |
| `XX1` (ENTRY_PTR_IS_ODD_KEY) | 直接存放奇数地址的 key 指针 |
| `010` (ENTRY_PTR_IS_EVEN_KEY) | 直接存放偶数地址的 key 指针（带标记） |

当 bucket 只含单个 key 且无冲突链时，key 可直接存储在 bucket 槽位中，避免 `dictEntry` 分配。

### 2.7 每条目内存开销

| 模式 | 开销 |
|------|------|
| Key-Value 字典 | 8 字节 (bucket 指针) + 24 字节 (dictEntry) = **~32 字节/条目** |
| no_value 优化后 | 直接 key 时仅 8 字节/条目；链接时 8 + 16 字节 |

考虑到负载因子（bucket 使用率 ~50%），实际开销约 **~40 字节/条目**。

### 2.8 迭代器设计

```c
typedef struct dictIterator {
    dict *d;
    long index;
    int table, safe;
    dictEntry *entry, *nextEntry;
    unsigned long long fingerprint;
} dictIterator;
```

- **Safe 迭代器**: 暂停 rehash
- **Unsafe 迭代器**: 使用 fingerprint 检测误用
- 迭代器在 yield 之前保存 `nextEntry`，使调用者可以安全删除当前 entry

---

## 3. Valkey Hashtable 实现分析

### 3.1 设计哲学

Valkey 的 hashtable 借鉴了 **Swiss Table**（Google 的高性能哈希表）的核心思想，围绕以下理念设计：

1. **Cache-line 对齐**: 每个 bucket 恰好 64 字节，等于一个 CPU 缓存行
2. **SIMD 加速查找**: 利用 SSE2/NEON 指令并行比较
3. **消除间接层**: 不再有 `dictEntry` 中间结构，直接存储 entry 指针
4. **哈希元数据内联**: 每个槽位存储 1 字节哈希指纹，快速过滤

### 3.2 核心数据结构

```c
struct hashtable {
    hashtableType *type;
    ssize_t rehash_idx;            // -1 = 未在 rehash
    bucket *tables[2];             // 连续的 bucket 数组
    size_t used[2];                // 每张表的条目数
    int8_t bucket_exp[2];          // bucket 数量的指数
    int16_t pause_rehash;
    int16_t pause_auto_shrink;
    size_t child_buckets[2];       // 已分配的子 bucket 数量
    iter *safe_iterators;          // safe 迭代器链表
    void *metadata[];
};
```

```c
typedef struct hashtableBucket {
    BUCKET_BITS_TYPE chained : 1;           // 1 位: 是否有子 bucket
    BUCKET_BITS_TYPE presence : ENTRIES_PER_BUCKET;  // 7 位: 哪些槽位已填充
    uint8_t hashes[ENTRIES_PER_BUCKET];     // 7 字节: 每槽位一个哈希字节
    void *entries[ENTRIES_PER_BUCKET];      // 7 个指针: 56 字节
} bucket;
// 总计: 恰好 64 字节 = 一个缓存行
```

通过 `static_assert` 确保 `sizeof(bucket) == 64`。

### 3.3 Bucket 内存布局 (64位系统)

```
+--------------------------------------------------------------------+
|  元数据 (8B)  |  entry0  entry1  entry2  entry3  entry4  entry5  entry6  |
+--------------------------------------------------------------------+
  1B metadata     7 x 8B = 56B 指针
  7B hash fingers

元数据字节分解:
+----------------------------------------------+
| c ppppppp | h0 | h1 | h2 | h3 | h4 | h5 | h6 |
+----------------------------------------------+
 1b  7b       7 x 1 字节哈希指纹
```

各字段含义：

| 字段 | 大小 | 说明 |
|------|------|------|
| `chained` | 1 bit | 是否有子 bucket（溢出链） |
| `presence` | 7 bits | 位掩码，标记哪些槽位包含有效 entry |
| `hashes[]` | 7 bytes | 每个 entry 的哈希高字节（h2），用于快速过滤 |
| `entries[]` | 7 x 8 bytes | 直接存储用户 entry 的指针 |

在 32 位系统上，每个 bucket 可容纳 12 个 entry（指针 4 字节），仍然保持 64 字节总大小。

### 3.4 冲突解决：Bucket 链式法

与 Redis 逐 entry 链接不同，Valkey 以**整个 bucket 为单位**进行链接：

1. 当一个 bucket 的 7 个槽位全部占满时
2. 分配一个新的子 bucket
3. 将满 bucket 的最后一个 entry 移到子 bucket
4. 设置 `chained` 位，将 `entries[6]` 改为指向子 bucket 的指针
5. 此时链接后的 bucket 有效容量从 7 降为 6

```
  主 bucket (64B)                子 bucket (64B)
+------------------+           +------------------+
| metadata         |           | metadata         |
| entry0 ... entry5|           | entry0 ... entry6|
| -> child_bucket -|---------->|                  |
+------------------+           +------------------+
```

好处：
- 前 7 个冲突条目**零额外分配**
- Bucket 链只在极高冲突率下才形成
- 删除时自动压缩：从子 bucket 移动 entry 填充空洞，释放空子 bucket

### 3.5 哈希函数与索引计算

与 Redis 相同，使用 SipHash。64 位哈希值的使用方式：

- **低位**: 用于 bucket 索引选择（`hash & mask`）
- **高字节** (`hash >> 56`): 存储在 bucket 元数据中作为 `h2` 指纹

h2 指纹的作用：在进行完整 key 比较之前，先比较 1 字节指纹。不匹配的概率为 255/256 ≈ 99.6%，大幅减少不必要的 key 比较。

### 3.6 SIMD 加速查找

`findBucket` 函数在可用时使用 SIMD 指令：

**SSE2 (x86-64)**:

```c
__m128i hash_vector = _mm_loadu_si128((__m128i *)b->hashes);
__m128i h2_vector = _mm_set1_epi8(h2);
__m128i result = _mm_cmpeq_epi8(hash_vector, h2_vector);
BUCKET_BITS_TYPE newmask = _mm_movemask_epi8(result);
newmask &= presence_mask;
```

一条 SSE2 指令同时比较 7 个哈希字节与目标 h2，生成候选位掩码。只有匹配的候选项才进行完整 key 比较。

**NEON (ARM)**:

```c
// 使用 vld1_u8, vdup_n_u8, vceq_u8 实现相同的并行比较
```

**标量回退**: 在不支持 SIMD 的平台上使用简单循环。

### 3.7 增量 Rehash

与 Redis 一样使用两张表，但以 bucket 为粒度：

**扩容路径** (`rehashStepExpand`):

1. 从旧表中批量提取最多 4 个 bucket 链的 entry 到缓冲区
2. 在紧凑循环中提取所有 key（无循环携带依赖 -- 利于 CPU 流水线）
3. 计算哈希并插入新表
4. 清理旧 bucket 链

**缩容路径** (`rehashStepShrink`):

- 避免重新计算哈希 -- 利用 bucket 索引作为哈希值（缩容只是掩码变短）
- 每步最多跳过 10 个空 bucket

**中止缩容** (独有特性):

如果缩容过程中目标表将超过 `MAX_FILL_PERCENT_HARD`，Valkey 会**中止缩容**：交换两张表，将操作转换为扩容。防止快速插入期间的病态情况。

**读写触发策略**:

| 场景 | 策略 | 原因 |
|------|------|------|
| 读操作 | 仅在 resize policy = ALLOW 时触发 | 减少 fork 时的 CoW 影响 |
| 写操作 | 仅在 resize policy = AVOID 时触发 | 确保 rehash 持续推进 |

### 3.8 填充因子管理

| | Soft (ALLOW) | Hard (AVOID) |
|---|---|---|
| 最大填充率 | 100% | 500% |
| 最小填充率 | 13% | 3% |

Bucket 数量计算使用整数运算：`num_buckets = ceil(num_entries * 5 / 32)`，resize 后最大填充率保证 <= ~91.43%。避免了昂贵的除法操作。

### 3.9 每条目内存开销

Hashtable 中的 entry 仅是**指针**（8 字节），直接存储在 bucket 中，无需像 `dictEntry` 那样单独分配。

| 指标 | 值 |
|------|-----|
| 每个 bucket | 64 字节 / 7 个槽位 |
| 每条目分摊开销 | 64 / 7 ≈ **9.14 字节** (满载) |
| 实际开销 (91% 填充率) | 约 **~10 字节/条目** |
| 额外分配 | 无 (对比 Redis 的 dictEntry) |

### 3.10 迭代器设计

```c
struct iter {
    hashtable *hashtable;
    bucket *bucket;
    long index;
    uint16_t pos_in_bucket;
    uint8_t table;
    uint8_t flags;
    union {
        uint64_t fingerprint;          // unsafe 迭代器
        uint64_t last_seen_size;       // safe 迭代器
    };
    iter *next_safe_iter;
};
```

关键特性：

- **预取 (Prefetching)**: 迭代到新 bucket 时，预取下一个 bucket 的 entry 和下下个 bucket 的元数据
- **值预取**: 可选的 `HASHTABLE_ITER_PREFETCH_VALUES` 标志，调用 `entryPrefetchValue` 回调预取 entry 关联的值对象
- **Bucket 链压缩**: safe 迭代器在遍历完一个 bucket 索引后，如有删除则自动压缩链
- **不透明类型**: 迭代器对外暴露为 `uint64_t hashtableIterator[6]` -- 栈上分配，无需堆分配

### 3.11 增量查找 (Incremental Find)

Valkey 独有的特性：`hashtableIncrementalFindInit` / `hashtableIncrementalFindStep` / `hashtableIncrementalFindGetResult` 允许将一次查找拆分为多个步骤。步骤之间 CPU 可以处理其他查找，实现多个 key 查找的流水线化。

状态机：`NEXT_BUCKET -> NEXT_ENTRY -> CHECK_ENTRY -> FOUND/NOT_FOUND`

这是为了利用**内存级并行性 (Memory-Level Parallelism, MLP)** -- 在等待一次内存访问完成期间启动另一次，从而隐藏内存延迟。

---

## 4. 核心差异对比

### 4.1 总览表

| 特性 | Redis dict | Valkey hashtable |
|------|-----------|-----------------|
| Bucket 大小 | 8 字节 (指针) | 64 字节 (缓存行) |
| 每 Bucket 条目数 | 1 + 链表 | 7 (64位) / 12 (32位) |
| 冲突策略 | 单链表链接 | Bucket 链接 |
| 每条目开销 | ~32 字节 | ~10 字节 |
| SIMD 支持 | 无 | SSE2 + NEON |
| 哈希元数据 | 无 | 每 entry 1 字节 (h2 指纹) |
| 增量查找 | 无 | 有 (状态机) |
| 迭代器预取 | 无 | 有 (bucket + value) |
| 中止缩容 | 无 | 有 |
| 典型查找缓存未命中 | 2+ 次 | 1 次 |
| 内存节省 | 基准 | ~3x 更少开销 |

### 4.2 内存布局

**Redis**: 间接寻址模型

```
bucket_ptr -> dictEntry -> dictEntry -> dictEntry -> NULL
               (24B)        (24B)        (24B)
```

每次查找需要：加载 bucket 指针 (1 次 cache miss) -> 加载 dictEntry (1 次 cache miss) -> 沿链表遍历 (每节点 1 次 cache miss)。

**Valkey**: 缓存行内联模型

```
bucket (64B = 1 cache line):
[metadata | h0 h1 h2 h3 h4 h5 h6 | ptr0 ptr1 ptr2 ptr3 ptr4 ptr5 ptr6]
```

查找只需：加载 bucket (1 次 cache miss，获得所有 7 个 entry 的信息) -> SIMD 过滤 -> 直接比较匹配项。

### 4.3 性能特征差异

| 操作 | Redis dict | Valkey hashtable | 原因 |
|------|-----------|-----------------|------|
| 点查找 | O(1) 均摊，2+ 次 cache miss | O(1) 均摊，通常 1 次 cache miss | bucket 内所有信息在同一缓存行 |
| 插入 | 需分配 dictEntry | 直接写入 bucket 槽位 | 无间接结构分配 |
| 删除 | 需释放 dictEntry | 清除 presence 位 + 可选压缩 | 无需释放间接结构 |
| 遍历 | 随机内存访问模式 | 顺序内存访问 + 预取 | bucket 数组连续，且有预取机制 |
| 批量操作 | 无特殊优化 | 增量查找 + 流水线化 | MLP 利用 |

### 4.4 Rehash 策略差异

| 方面 | Redis dict | Valkey hashtable |
|------|-----------|-----------------|
| Rehash 单位 | 一个 bucket (链表) | 一个 bucket 链 (含所有子 bucket) |
| 扩容 rehash | 逐 entry 重新哈希 | 批量提取、紧凑循环哈希、批量插入 |
| 缩容 rehash | 掩码法 (无需重新哈希) | 掩码法 (无需重新哈希)，跳过空位 |
| 中止缩容 | 不支持 | 支持：交换表并转为扩容 |
| 批量处理 | 100 步 / 时间检查 | 128 步 / 时间检查 |
| 索引特定 rehash | 有 (优先 rehash 当前访问的 bucket) | 无 (始终从 rehash_idx 顺序推进) |

### 4.5 设计哲学对比

**Redis dict**:
- 经典的计算机科学教科书式哈希表
- 分离链接法 + 多年渐进优化
- `dictEntry` 作为间接层，管理 key/value 存储
- 针对特定场景的指针标记优化
- 保守的、向后兼容的演进路径

**Valkey hashtable**:
- 现代化的、缓存感知的设计，灵感来源于 Google Swiss Table
- 核心决策：64 字节缓存行对齐的 bucket + 内联元数据
- Entry 只是指针 -- 哈希表不管理 key/value 存储，只存储指向用户管理的 entry 对象的指针
- 激进地利用现代硬件特性：SIMD、预取、MLP
- 以内存效率和缓存友好性为首要目标

---

## 5. Valkey 独有特性详解

### 5.1 SIMD 加速查找

在 bucket 内查找时，利用 SIMD 指令一次性比较所有 7 个哈希指纹字节：

- **x86-64**: SSE2 的 `_mm_cmpeq_epi8` + `_mm_movemask_epi8`
- **ARM**: NEON 的 `vceq_u8`

这使得 bucket 内搜索在 CPU 周期上几乎是免费的。

### 5.2 增量查找 API

```
hashtableIncrementalFindInit()    -- 初始化查找
hashtableIncrementalFindStep()    -- 执行一步
hashtableIncrementalFindGetResult() -- 获取结果
```

允许交错多个查找操作，在等待一个内存访问完成的同时启动另一个。这对于批量命令处理（如 MGET）非常有价值。

### 5.3 迭代器预取

- 遍历到新 bucket 时，预取下一个 bucket 的 entry 指针
- 同时预取下下个 bucket 的元数据
- 可选的 `entryPrefetchValue` 回调预取值对象
- 显著提升全表扫描性能

### 5.4 中止缩容

如果在缩容过程中发现目标表将超载（超过 `MAX_FILL_PERCENT_HARD`），会：
1. 交换 `tables[0]` 和 `tables[1]`
2. 将缩容操作转换为扩容操作
3. 防止快速插入场景下的性能退化

### 5.5 Bucket 链压缩

删除操作后自动执行：
1. 检查子 bucket 中是否有 entry 可以填补父 bucket 的空洞
2. 移动 entry 以保持紧凑
3. 释放完全空的子 bucket
4. 确保内存不会因为删除操作而碎片化

---

## 6. 对 C++ 重写的启示

基于以上调研，在用 C++ 重写时应注意以下设计要点：

1. **保持 64 字节 bucket 对齐**: 这是性能的基石，使用 `alignas(64)` 确保缓存行对齐
2. **利用 C++ 模板**: 可将 entry 类型参数化，避免 `void*` 的类型不安全
3. **SIMD 抽象层**: 设计统一的 SIMD 接口，自动适配 SSE2/AVX2/NEON
4. **RAII 管理 bucket 生命周期**: 利用 C++ 的析构语义自动管理子 bucket 链
5. **保留增量 rehash**: 这是面向数据库场景的关键特性，不能丢失
6. **考虑模板化哈希函数**: 允许用户注入自定义哈希函数，同时默认使用 SipHash
7. **迭代器符合 C++ 规范**: 实现标准的 InputIterator/ForwardIterator 接口
8. **增量查找可封装为 coroutine 或 future 模式**: 利用 C++20 协程简化流水线化查找

---

## 参考资源

- Valkey 源码: `src/hashtable.h`, `src/hashtable.c` ([GitHub](https://github.com/valkey-io/valkey))
- Redis 源码: `src/dict.h`, `src/dict.c` ([GitHub](https://github.com/redis/redis))
- Swiss Table 论文: Abseil (Google) 的 flat_hash_map 设计
- [CppCon 2017: Matt Kulukundis "Designing a Fast, Efficient, Cache-friendly Hash Table, Step by Step"](https://www.youtube.com/watch?v=ncHmEUmJZf4)
