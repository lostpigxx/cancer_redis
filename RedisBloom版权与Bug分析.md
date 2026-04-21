## 1. 任务一：版权风险分析

以下是工程合规分析，不替代执业律师意见。

### 1.1 RedisBloom 许可证背景

Redis 官方当前的法律页面明确写到：从 **Redis 8** 开始，Redis Open Source 及其内置模块可在 **RSALv2 / SSPLv1 / AGPLv3** 三个许可证中任选其一；并且 **RedisBloom 已作为 Redis Open Source 的组成部分纳入同一 tri-license**。官方 GitHub README 也写明：**Starting with Redis 8, RedisBloom is licensed under your choice of RSALv2 / SSPLv1 / AGPLv3; prior versions remain subject to (i) and (ii)**。([Redis][1])

更关键的是，Redis 官方法律页还给出了 **RedisBloom 历史版本表**：

* **RedisBloom < 2.0：AGPLv3**
* **RedisBloom 2.0 ～ 2.2：RSALv1**
* **RedisBloom 2.4 ～ 2.8：RSALv2 或 SSPLv1**。([Redis][1])

这里有一个必须明确说出来的点：**官方表格跳过了 2.3.x**。因此，针对 2.3.x 的精确许可证过渡点，我不确定，不能编造。保守做法是：**不能假设所有“旧 RedisBloom”都天然可按今天的 AGPLv3 选项处理**；你实际参考的代码版本，决定你必须遵守哪套条款。([Redis][1])

你仓库里的 `doc/redis_bloom_survey.md` 也写了许可证演进，但它把早期阶段概括成“2018-2022 RSALv1”，这与 Redis 官方法律页给出的 RedisBloom 历史表并不一致。这个文档可以作为调研记录，但**不能作为合规依据**。 ([Redis][1])

前面搞清楚了许可证背景，但还有一个更关键的问题：**你的实现到底和 RedisBloom 像到什么程度，像的是“算法思想”，还是“受保护的代码表达”？**

### 1.2 逐维度代码相似性对比

先给出结构映射图，便于后面的逐项判断：

```text
你的实现                              RedisBloom
---------------------------------------------------------------
ScalingBloomFilter                    SBChain
  layers_          ---------------->    filters
  totalItems_      ---------------->    size
  numLayers_       ---------------->    nfilters
  flags_           ---------------->    options
  expansionFactor_ ---------------->    growth

FilterLayer                           SBLink
  BloomLayer bloom   -------------->    struct bloom inner
  itemCount          -------------->    size

BloomLayer                            struct bloom
  hashCount_        ---------------->    hashes
  log2Bits_         ---------------->    n2
  capacity_         ---------------->    entries
  fpRate_           ---------------->    error
  bitsPerEntry_     ---------------->    bpe
  bitArray_         ---------------->    bf
  dataSize_         ---------------->    bytes
  totalBits_        ---------------->    bits
```

#### 1.2.1 数据结构定义

**相似度评级：高风险**

你的 `BloomLayer` 与 RedisBloom 的 `struct bloom` 基本是一一映射；你的 `FilterLayer` 与 RedisBloom 的 `SBLink` 也是一一映射；你的 `ScalingBloomFilter` 与 RedisBloom 的 `SBChain` 则是在 C++ 包装下的直接对应。你的代码只是把 C 结构换成了 C++ RAII 类和 `std::optional/std::span` 接口，但核心状态字段、层级关系、职责划分非常接近。   

这不是“都实现了 Bloom Filter，所以看起来像”这么简单。更关键的是：**你选择保存哪些字段、如何分层、哪些字段归单层、哪些字段归整链**，这些“选择与编排”都高度贴近 RedisBloom。这里已经超出了纯算法层面。

#### 1.2.2 内存布局与标志位语义

**相似度评级：高风险**

你的 `BloomFlags` 取值是：

```cpp
NoRound = 1
RawBits = 2
Use64Bit = 4
FixedSize = 8
```

RedisBloom 的 `bloom.h` 取值是：

```c
BLOOM_OPT_NOROUND = 1
BLOOM_OPT_ENTS_IS_BITS = 2
BLOOM_OPT_FORCE64 = 4
BLOOM_OPT_NO_SCALING = 8
```

不仅语义对应，而且 **bit 值完全对齐**。你自己的注释还写了 “values fixed for RDB wire compatibility”。这说明这不是巧合，而是有意绑定到 RedisBloom 的现有表达。 

如果只看版权，**“兼容所必需的字段/位布局”本身未必一定构成侵权**；但从证据强度看，这会显著增强“你的实现直接以 RedisBloom 为蓝本”的判断。

#### 1.2.3 哈希策略

**相似度评级：中风险**

你的 `Hash32Policy::Compute` / `Hash64Policy::Compute` 使用：

* 首哈希 seed：`0x9747b28c` / `0xc6a4a7935bd1e995ULL`
* 次哈希 seed：`h1`
* 位置公式：`primary + i * secondary`

RedisBloom 的 `bloom_calc_hash` / `bloom_calc_hash64` 与 `CHECK_ADD_FUNC` 使用的也是同一组种子和同一 double hashing 方式。 

这一项我给“中风险”而不是“高风险”，原因有两个：

1. **MurmurHash2 本身是公共领域/宽松来源实现**。你的 `murmur2.cc` 也明确标了 public domain。
2. **Kirsch–Mitzenmacher 风格的 double hashing** 是 Bloom Filter 领域经典公开算法思想，不受版权保护。

所以，这一项更像是“相同算法路线 + 相同兼容实现选择”，不是最强的版权锚点。

#### 1.2.4 扩容/收紧策略

**相似度评级：中风险**

你的 `ScalingBloomFilter`：

* 首层错误率：`errorRate * 0.5`
* 满了以后：`nextCap = currentCap * expansionFactor`
* 下一层错误率：`nextRate = currentFpRate * 0.5`
* 没有 shrink 路径，只会 append 新层。

RedisBloom 的 `SB_NewChain` / `SBChain_Add`：

* tightening ratio：`0.5`
* 新层容量：`entries * growth`
* 新层错误率：`current.error * 0.5`
* 也是只增长，不收缩。

这一项相似度很高，但它同时也明显来源于 **Scalable Bloom Filters 论文** 的公开思想。因此我给“中风险”：**技术路线完全同源，但思想本身不受版权保护；真正有风险的是你在其他地方连表达层也一起贴近了。**

#### 1.2.5 序列化 / 反序列化格式

**相似度评级：高风险**

这是最强证据之一。

你的模块类型名直接使用 **`"MBbloom--"`**，当前编码版本设为 **4**；注释明确说这是为了兼容现有 Redis 数据。

RedisBloom 也是创建 `MBbloom--`，并使用到 4 这一代 Bloom 编码版本。

你的 RDB 保存顺序：

```text
totalItems
numLayers
flags
expansionFactor
for each layer:
  capacity
  fpRate
  hashCount
  bitsPerEntry
  totalBits
  log2Bits
  bitArray
  itemCount
```

RedisBloom 的 `BFRdbSave` 保存顺序语义上是：

```text
size
nfilters
options
growth
for each layer:
  entries
  error
  hashes
  bpe
  bits
  n2
  bf
  size
```

字段名不同，但顺序与语义几乎是直接对应。 

你的 `WireFilterHeader` / `WireLayerMeta` 与 RedisBloom 的 `dumpedChainHeader` / `dumpedChainLink` 也是同一组字段语义。  

这一项不是“API 相同”，而是**内部持久化表达也高度对齐**。如果将来发生争议，这会是很重的证据。

#### 1.2.6 API 接口设计

**相似度评级：中风险**

你的模块直接暴露：

* `BF.RESERVE`
* `BF.ADD`
* `BF.MADD`
* `BF.INSERT`
* `BF.EXISTS`
* `BF.MEXISTS`
* `BF.INFO`
* `BF.CARD`
* `BF.SCANDUMP`
* `BF.LOADCHUNK`。

RedisBloom 的 Bloom 子集也是这组命令，并且参数文法高度一致。

单看版权，**命令名/API 兼容本身通常不是最核心的版权风险点**；但这里的问题是：你不是“只兼容 API”，而是 **API + 类型名 + 标志位 + RDB 格式 + 层级结构** 一起贴近，所以 API 这一项不能孤立看。

#### 1.2.7 错误处理模式

**相似度评级：低风险**

你和 RedisBloom 的错误语义相近：

* key already exists
* wrongtype
* non-scaling/full
* bad capacity/error_rate/expansion

但具体文案并非逐字复制，控制流也不是直接照抄。比如你写的是 `"ERR filter is full and non-scaling"`，RedisBloom 常见的是 `"ERR non scaling filter is full"`。 

因此这一项本身风险不高。它更像“实现了同一命令族必然会有类似错误语义”。

#### 1.2.8 哪些部分属于公共领域 / 论文算法，不构成版权问题

这里要单独划出来，避免把“思想”误判成“表达”：

1. **MurmurHash2 / MurmurHash64A 的经典实现**
   你的 `src/murmur2.cc` 明确写了 public domain 来源；这部分如果真的是 Austin Appleby 的公开实现或忠实移植，本身不构成 RedisBloom 版权问题。

2. **Bloom Filter 基本公式**
   `bits per entry = -ln(p)/(ln2^2)`，`k = ceil(ln2 * bpe)`，这是公开数学公式，不受版权保护。你的 `BloomLayer::Create` 和 RedisBloom `calc_bpe` / `bloom_init` 都使用这套公式，这是正常现象。 

3. **Scalable Bloom Filter 的 0.5 tightening ratio、按 expansion 因子叠层**
   这是论文公开思想。你和 RedisBloom 都用了，不足以单独构成版权问题。 

前面搞懂了“哪些是思想、哪些是表达”，但还有一个最终要落地的问题：**这份代码在法律上更接近哪一种：照抄、实质性相似、仅 API 兼容，还是完全独立？**

### 1.3 许可证合规判定

#### (a) 逐字复制 verbatim copy

**结论：未发现核心 Bloom/SBF C++ 代码存在明确的逐字复制证据。**

你的主体代码不是把 `sb.c` 或 `bloom.c` 直接翻成 `.cc` 的逐行转写；类设计、文件拆分、RAII 包装、`std::optional/std::span` 等都说明它不是机械复制。 

但要排除一类例外：**公共领域的 MurmurHash2 文件**。这一块即使是直接拿来的，也不是 RedisBloom 版权风险核心。

#### (b) 实质性相似 substantial similarity

**结论：是，且这是我对你仓库的主要判定。**

原因不是某一个点像，而是 **多项受保护表达同时贴近**：

* 单层/链式两级对象模型贴近
* 状态字段选择与归属贴近
* 标志位语义与 bit 值贴近
* `MBbloom--` 类型名与版本锚点贴近
* RDB / SCANDUMP 头格式与字段顺序贴近
* BF.* 命令子集与参数文法贴近。    

如果将来进入争议，这更像“以 RedisBloom Bloom 子系统为蓝本的 C++ 重写”，而不是“只参考了论文然后独立长出来的另一份实现”。

#### (c) 独立实现但 API 兼容

**结论：只能部分成立，不足以描述整个仓库。**

如果只看 BF.* 命令面和 Bloom 数学公式，可以说“API 兼容”。
但你的仓库并不是只兼容 API，它还兼容了：

* 类型名
* 标志位
* RDB 持久化布局
* SCANDUMP/LOADCHUNK 头语义
* 层级对象模型。  

所以，把它整体描述成“独立实现但 API 兼容”，证据不够。

#### (d) 完全独立实现

**结论：不成立。**

如果是完全独立实现，通常会看到：

* 不同的内部状态组织
* 不同的持久化格式
* 不同的标志位编码
* 不同的模块类型名
* 至少在某些关键决策上明显偏离上游。

你的代码没有呈现出这种“明显偏离”。

#### 法律层面的风险说明

这里最关键的一句话是：

**风险不取决于“今天 RedisBloom 能否按 AGPL 使用”，而取决于你实际参考/衍生的源码版本。** ([Redis][1])

具体说：

1. **如果你的实际来源是 Redis 8 / 当前 master 代码**
   那它现在是 tri-license。理论上你可以选择按 AGPLv3 合规，但前提是你接受 AGPL 的网络 copyleft 义务；若选择 RSAL/SSPL，则要满足它们各自限制。([Redis][1])

2. **如果你的实际来源是 RedisBloom 2.4～2.8 线**
   官方历史表给的是 RSALv2 / SSPLv1，并没有 AGPL 选项。那你不能事后把它当成“反正 Redis 8 已经 AGPL 了，所以我这里也安全”。([Redis][1])

3. **如果你的实际来源是 2.0～2.2 或更早**
   则分别要落到 RSALv1 或 AGPLv3 上。([Redis][1])

4. **如果你拿不出 clean-room 证据，也说不清参考的是哪条版本线**
   合规上最稳妥的做法是：**按“最不利但合理”的来源版本评估**，而不是按最宽松版本自我解释。

#### 建议的规避措施

有两条路线，必须二选一，不能夹在中间：

**路线 A：承认衍生关系，严格按来源许可证合规。**
适合你确实就是参考 RedisBloom 重写，而且也接受相应开源/源可用义务。

**路线 B：做真正的去衍生化重构。**
适合你希望把风险降到低。那就不能只改命名，必须把几个“强锚点”拆掉，见 1.5。

### 1.4 总体版权风险评级与结论

**总体版权风险评级：高**

不是“极高”，因为我没有看到核心实现存在明显逐字复制；而且 Murmur、Bloom 公式、Scalable Bloom Filter 论文思想这些部分，本来就不该算进侵权判断。 

但我仍然给“高”，原因是：
**你的仓库不是仅仅“实现了同一种算法”，而是同时复现了 RedisBloom 的对象分层、标志位编码、模块类型名、命令面、RDB/SCANDUMP 表达。** 这会让“实质性相似”判断变得很难回避。    

总结性意见只有两句：

第一，这份代码**更像“RedisBloom Bloom 子系统的 C++ 重写”**，而不是“仅从论文独立实现的另一个 Bloom 模块”。
第二，如果你想把风险真正压低，**不能只改注释和文件名，必须把兼容性锚点从内部表达层剥离出去。**

### 1.5 修改建议清单

下面这些建议，都是精确到文件/代码段的。

1. **`src/redis_bloom_module.cc:RedisModule_OnLoad`**
   如果你不需要与 RedisBloom RDB 完全互通，停止使用 **`"MBbloom--"`** 和同一编码版本锚点；改成你自己的模块类型名和版本。
   这是最直接、最有效的降风险动作之一。

2. **`src/bloom_filter.h:BloomFlags`**
   如果不需要 wire compatibility，停止沿用 `1/2/4/8` 这一组与 RedisBloom 对齐的 bit 语义；改为你自己的内部 flags，再在兼容适配层做转换。

3. **`src/bloom_rdb.h/.cc` 与 `src/sb_chain.h/.cc`**
   把“RedisBloom 兼容持久化”隔离成单独的 `compat/redisbloom_codec.*`。
   核心 Bloom 实现内部使用你自己的序列化格式；只有 import/export 边界层才处理 RedisBloom 兼容编码。   
   这样可以把“兼容需求”与“核心表达”切开。

4. **`src/sb_chain.h/.cc`**
   重新设计 `WireFilterHeader` / `WireLayerMeta` 的内部表达，不要继续和 RedisBloom 的 dumped header/link 形成一一映射；如果必须兼容，就只在 compat 层保留这一份格式。 

5. **`src/bloom_commands.cc`**
   如果你的产品并不要求 drop-in RedisBloom 命令兼容，停止暴露整套 `BF.*` 命令名。
   如果必须兼容，至少把“内部实现和对外兼容层”拆开，避免整个工程都看起来是 RedisBloom 的同构替身。

6. **仓库级别**
   增加 `NOTICE` / `THIRD_PARTY_LICENSES`：

   * MurmurHash2 的来源与 public-domain 说明
   * Redis Modules SDK 头文件来源
   * 若保留 RedisBloom 兼容层，写明兼容目标与实现方式。
     这不能消灭衍生风险，但能显著改善合规姿态。

7. **流程级别**
   如果你想主张“独立实现”，后续必须建立 **clean-room 证据链**：设计说明、来源隔离、对照文档、审查记录。没有过程证据，只靠结果代码很难把“高度相似”解释成巧合。

---

## 2. 任务二：Bug 审查报告

先说审查边界：这份代码里**没有显式多线程/锁/原子变量**，因此没有发现已触发的并发类 Bug；问题主要集中在 **内存安全、对象生命周期、数值边界、兼容性**。

### 2.1 确认的 Bug 列表（按严重程度排序）

#### Bug #1

严重程度: **P0(致命)**
类别: **内存安全 / 数值问题**
位置: **`src/bloom_filter.cc:BloomLayer::Create`，`src/bloom_filter.cc:BloomLayer::Test`，`src/bloom_filter.cc:BloomLayer::Insert`**

问题描述:
`NoRound` 路径下，代码把 `totalBits_` 直接截断成整数位数，然后用

```cpp
dataSize_ = totalBits_ / 8;
```

分配字节数；但访问时又按 `pos < totalBits_` 计算 bit 位置，并用 `bitArray_[pos >> 3]` 取字节。
只要 `totalBits_ % 8 != 0`，最后那几个 bit 会落到 **未分配的下一字节**，形成越界读写。

这不是冷门分支。`AllocFilter()` 默认就把 `BloomFlags::NoRound` 打开，因此这是**生产主路径**。

触发条件:
例如 `BF.RESERVE key 0.01 200`。
此时 `rawBits ≈ 200 * 9.585 = 1917`，`dataSize = 1917 / 8 = 239` 字节，只覆盖 1912 bit；而 `Test/Insert` 仍可能访问到 bit 1912~1916，对应 `bitArray_[239]`，越界。
RedisBloom 上游不会出现这个问题，因为它会把 bytes 向上对齐，并把 `bits = bytes * 8`。

修复建议:
必须把“可寻址 bit 数”和“已分配 byte 数”统一：

```cpp
layer.dataSize_ = (layer.totalBits_ + 7) / 8;
layer.totalBits_ = layer.dataSize_ * 8;
```

如果你想保留“原始目标 bit 数”，应单独存一个 `requestedBits_`，而不是让访问逻辑继续用未对齐的 `totalBits_`。

---

#### Bug #2

严重程度: **P0(致命)**
类别: **可移植性与未定义行为 / 内存安全**
位置: **`src/sb_chain.cc:ScalingBloomFilter::AppendLayer`，`src/sb_chain.cc:ScalingBloomFilter::FromRdbShell`，`src/sb_chain.cc:ScalingBloomFilter::SetLayer`，`src/sb_chain.cc:ScalingBloomFilter::operator=(ScalingBloomFilter&&)`**

问题描述:
这份代码把 **非平凡 C++ 对象** 当成 C POD 在管理，违反对象生命周期规则。

核心问题有三处：

1. `AppendLayer()` 对 `FilterLayer*` 使用 `RMRealloc` 扩容。
   但 `FilterLayer` 里含有 `BloomLayer`，而 `BloomLayer` 有自定义析构、move ctor、move assign，因此**不是可被 `realloc` 原样搬移的平凡类型**。`realloc` 的字节搬移对这类对象是未定义行为。

2. `FromRdbShell()` 用 `RMCalloc` 分配 `FilterLayer` 数组，然后 `SetLayer()` 直接赋值到这些槽位。
   这些槽位并没有通过 placement-new 构造出 `FilterLayer/BloomLayer` 对象；对“未构造对象”做赋值，同样是未定义行为。

3. `ScalingBloomFilter::operator=(ScalingBloomFilter&&)` 直接 `this->~ScalingBloomFilter();`，然后在同一对象上继续给成员赋值。
   显式析构后对象生命周期已经结束；不经 placement-new 直接继续使用该对象，也属于未定义行为。

这类问题在 C 版本 RedisBloom 里不存在，因为上游 `SBLink` / `struct bloom` 是 C POD，`realloc` 在那里是合法策略；你这里把它移植成带析构/移动语义的 C++ 类之后，原管理方式就不成立了。 

触发条件:

* Bloom 层数增长，触发 `AppendLayer()` 重新分配
* 任意 RDB / SCANDUMP 反序列化路径
* 对 `ScalingBloomFilter` 做 move assignment

修复建议:
不要再用 `realloc/calloc + 手工赋值` 管理 `FilterLayer`。

可行方案有两个：

**方案 A：改成真正的 C++ 容器**

```cpp
std::vector<FilterLayer, RedisAllocator<FilterLayer>> layers_;
```

**方案 B：继续手写，但必须遵守生命周期**

* 原始内存用 `::operator new` / 自定义 allocator 申请
* 扩容时对旧元素逐个 placement-move-construct 到新地址
* 再逐个显式析构旧元素
* 最后释放旧原始内存

`operator=(ScalingBloomFilter&&)` 也不要手调析构函数；改成正常的资源交换或显式释放成员资源后赋值。

---

#### Bug #3

严重程度: **P1(严重)**
类别: **逻辑正确性 / API 健壮性**
位置: **`src/bloom_commands.cc:CmdLoadchunk`**

问题描述:
`BF.LOADCHUNK` 非 header 路径对 chunk 长度不做严格校验，而是：

```cpp
size_t copyLen = std::min(dataLen, layer.bloom.GetDataSize());
std::memcpy(layer.bloom.GetBitArray(), data, copyLen);
```

这意味着：

* chunk **过短**：静默接受，剩余 bit 保持旧值/零值
* chunk **过长**：静默截断，仍然返回 `OK`

结果是：**损坏的 filter 被当作成功加载**。这会直接造成 membership 结果不可信。

触发条件:

* 用户手工调用 `BF.LOADCHUNK` 传错长度
* AOF/迁移数据被截断
* 上游/异构实现导出的 chunk 长度与本实现不匹配

修复建议:
必须要求严格相等：

```cpp
if (dataLen != static_cast<size_t>(layer.bloom.GetDataSize())) {
    return RedisModule_ReplyWithError(ctx, "ERR chunk size mismatch");
}
std::memcpy(...);
```

---

#### Bug #4

严重程度: **P1(严重)**
类别: **逻辑正确性 / API 健壮性**
位置: **`src/bloom_rdb.cc:RdbLoadBloom`**

问题描述:
RDB 加载路径也在做同样的“容忍式截断”：

```cpp
std::memcpy(params.bitArray, buf,
            std::min(bufLen, static_cast<size_t>(params.dataSize)));
```

并且没有对以下关键约束做完整校验：

* `bufLen == dataSize`
* `hashCount > 0`
* `log2Bits == 0` 时与 `totalBits` 的关系
* `log2Bits != 0` 时 `totalBits` 是否真的是 `2^log2Bits`
* `itemCount <= capacity`
* flags 是否在已知集合内。

这会导致 **畸形 RDB 被静默接受为“合法 filter”**。
RedisBloom 自己最近几年修过多次“加载无效 RDB 崩溃/泄漏”的问题；你的实现这里没有建立完整的防线。([GitHub][2])

触发条件:

* 损坏或恶意构造的 RDB
* 与本实现期望不一致的旧格式/异构格式

修复建议:
在真正分配和拷贝前做强校验；一旦任一字段不自洽，直接失败返回 `nullptr`。
建议增加一个 `ValidateLoadedLayer(params, bufLen)`。

---

#### Bug #5

严重程度: **P1(严重)**
类别: **数值问题 / API 健壮性**
位置: **`src/bloom_commands.cc:CmdReserve`，`src/bloom_commands.cc:CmdInsert`，`src/bloom_filter.cc:BloomLayer::Create`，`src/sb_chain.cc:ScalingBloomFilter::Put`**

问题描述:
容量和扩容路径缺少上界与溢出检查。

具体包括：

1. `CmdReserve` / `CmdInsert` 只要求 `cap > 0`，没有上限。
2. `BloomLayer::Create` 用 `double rawBits = cap * bitsPerEntry`，随后直接 cast 到 `uint64_t`，没有 `isfinite` / 上界检查。
3. `ScalingBloomFilter::Put` 扩容时做：

   ```cpp
   uint64_t nextCap = top.bloom.GetCapacity() * expansionFactor_;
   ```

   这里也没有乘法溢出检查。

结果可能是：

* `rawBits` 溢出/变成无穷大/转换成错误整数
* `nextCap` wrap-around
* 申请异常巨大的内存，导致 OOM 或不稳定行为

上游 RedisBloom 会用配置范围限制这些值；你的实现没有。

触发条件:

* 极大容量，如 `BF.RESERVE key 0.01 9223372036854775807`
* 多层扩容叠加导致乘法溢出

修复建议:

* 在命令层设置明确上限
* `Create()` 里用 `long double` 计算，并检查 `std::isfinite(rawBits)`
* 扩容前做 checked multiply：

```cpp
if (top.bloom.GetCapacity() > UINT64_MAX / expansionFactor_) {
    return std::nullopt;
}
```

---

#### Bug #6

严重程度: **P2(中等)**
类别: **逻辑正确性**
位置: **`src/bloom_rdb.cc:RdbLoadBloom`**

问题描述:
`RdbLoadBloom` 表面上接受 `encver <= 4`：

```cpp
if (encver > kCurrentEncVer) return nullptr;
```

并且还对 flags / expansion 做了旧版本兼容分支。
但每层数据解析时，它**始终**读取：

```text
capacity, fpRate, hashCount, bitsPerEntry, totalBits, log2Bits, bitArray, itemCount
```

而 RedisBloom 的旧 `encver == 0` 加载路径并不是这样；它不会从 RDB 里读取 `bits/n2` 这两个字段。也就是说，你的加载器实际上**并不真的兼容最老版本格式**，只是“名义上放行”。 

触发条件:
尝试加载很老的 RedisBloom Bloom RDB 数据。

修复建议:
两种选一个：

* 要么明确拒绝 `encver == 0`
* 要么补齐和 RedisBloom 同步的旧版本解析分支

---

#### Bug #7

严重程度: **P2(中等)**
类别: **逻辑正确性 / API 健壮性**
位置: **`src/bloom_commands.cc:CmdScandump`，`src/bloom_commands.cc:CmdLoadchunk`**

问题描述:
你的 `BF.SCANDUMP/BF.LOADCHUNK` **cursor 语义** 与 RedisBloom 不同。

RedisBloom 的 Bloom `SCANDUMP` 是基于**字节流偏移**的 chunk 迭代器；返回的 cursor 与 chunk 的结束偏移绑定，`LOADCHUNK` 再用 `iter - bufLen` 反推写入位置。 

你的实现则是：

* `0 -> (1, header)`
* `1 -> (2, layer0 bits)`
* `2 -> (3, layer1 bits)`
* `LOADCHUNK` 里 `cursor==1` 表示 header，否则 `idx = cursor - 2`

也就是把 cursor 变成了**层编号协议**。

这会导致：
**虽然 header 结构长得像 RedisBloom，但 SCANDUMP/LOADCHUNK 命令协议并不互通。**

触发条件:

* 试图和 RedisBloom 做 SCANDUMP/LOADCHUNK 级别互操作
* 使用依赖 RedisBloom cursor 语义的外部工具

修复建议:
如果目标是兼容 RedisBloom，就必须按上游 byte-offset 语义实现。
如果不追求兼容，就应该在文档和命令命名上明确声明这是自定义协议。

---

#### Bug #8

严重程度: **P2(中等)**
类别: **数值问题 / API 健壮性**
位置: **`src/bloom_filter.cc:Hash32Policy::Compute`，`src/bloom_filter.cc:Hash64Policy::Compute`**

问题描述:
两处都把 `std::span::size()` 窄化成了 `int`：

```cpp
auto len = static_cast<int>(data.size());
```

而 Murmur 接口也接受 `int len`。 

这意味着当输入 item 长度超过 `INT_MAX` 时，会发生截断甚至变负，哈希结果错误，行为不可靠。

触发条件:
超大输入值，长度 > 2,147,483,647 字节。

修复建议:

* 在命令层或哈希层显式拒绝超大 item
* 或把 Murmur 包装层改成接受 `size_t`，在内部做安全分块/安全检查

---

#### Bug #9

严重程度: **P3(轻微)**
类别: **可移植性与未定义行为**
位置: **`src/bloom_commands.cc`，`src/bloom_config.cc`**

问题描述:
这两个文件都调用了 `strncasecmp`，但只包含了 `<cstring>`，没有包含 POSIX 的 `<strings.h>`。 

在某些非 POSIX 或更严格的编译环境下，这会直接编译失败。

触发条件:

* 非 GNU / 非 POSIX 平台
* 更严格的头文件检查配置

修复建议:
显式包含：

```cpp
#include <strings.h>
```

或者自己写一个平台无关的大小写无关比较函数。

---

#### Bug #10

严重程度: **P3(轻微)**
类别: **逻辑正确性**
位置: **`src/sb_chain.cc:ScalingBloomFilter::BytesUsed`，`src/bloom_rdb.cc:BloomMemUsage`**

问题描述:
`BytesUsed()` 把层数组内存按 `numLayers_ * sizeof(FilterLayer)` 计算，而不是按**实际分配容量** `layerCapacity_` 计算。
当发生扩容后，`layerCapacity_ > numLayers_`，这里会低报内存占用。`BloomMemUsage()` 继续沿用了这个结果。 

触发条件:
层数组有富余 capacity 时。

修复建议:
改成：

```cpp
size_t base = sizeof(ScalingBloomFilter) + layerCapacity_ * sizeof(FilterLayer);
```

如果以后改成 `std::vector`，则应以 `capacity()` 为准。

### 2.2 待确认问题列表

#### 待确认问题 A

位置: **`src/bloom_commands.cc:CmdMadd`，`src/bloom_commands.cc:CmdInsert`，`src/bloom_commands.cc:CmdMexists`**

疑问:
这些批量命令在某些错误场景下会先 `ReplyWithArray`，然后在数组元素里夹杂 `ReplyWithError`。
这在 Redis Modules 层面未必非法，但是否符合你想要的 RESP 语义、是否符合你想对齐的 RedisBloom/Redis 客户端预期，需要再确认。

#### 待确认问题 B

位置: **`src/sb_chain.h:WireFilterHeader/WireLayerMeta`，`src/sb_chain.cc:SerializeHeader/DeserializeHeader`**

疑问:
SCANDUMP header 直接写 packed struct，里面有原生整数和 `double`。
如果你的目标包含**跨字节序、跨 ABI** 迁移，这就是可移植性问题；如果目标仅是同架构/同实现之间迁移，则可能是可接受设计。 

#### 待确认问题 C

位置: **`src/bloom_config.cc:BloomConfigLoad`**

疑问:
这里用 `strncasecmp(arg, "ERROR_RATE", len)` 之类的写法，没有同时校验长度相等，因此前缀也可能被接受；并且未知参数默认静默忽略。
这到底是你想要的“宽松启动参数”，还是解析疏漏，取决于设计意图。

### 2.3 Bug 汇总表

| 编号 | 严重程度 | 类别             | 一句话描述                                          |
| -- | ---- | -------------- | ---------------------------------------------- |
| 1  | P0   | 内存安全 / 数值问题    | `NoRound` 路径按 `totalBits/8` 分配，导致 bit 访问可能越界   |
| 2  | P0   | 未定义行为 / 内存安全   | 用 `realloc/calloc` 管理非平凡 C++ 对象，违反对象生命周期       |
| 3  | P1   | 逻辑正确性 / API健壮性 | `BF.LOADCHUNK` 对错误 chunk 长度静默截断并返回成功           |
| 4  | P1   | 逻辑正确性 / API健壮性 | RDB 加载对畸形数据缺少严格校验，损坏 filter 可被接受               |
| 5  | P1   | 数值问题 / API健壮性  | 容量、bit 数、扩容乘法都缺少上界与溢出检查                        |
| 6  | P2   | 逻辑正确性          | `RdbLoadBloom` 名义支持旧 encver，实际不能正确解析最老格式       |
| 7  | P2   | 逻辑正确性 / API健壮性 | `SCANDUMP/LOADCHUNK` cursor 语义与 RedisBloom 不兼容 |
| 8  | P2   | 数值问题 / API健壮性  | 哈希层把 `size_t` 窄化成 `int`，超大输入会截断                |
| 9  | P3   | 可移植性           | `strncasecmp` 缺少 `<strings.h>` 依赖              |
| 10 | P3   | 逻辑正确性          | 内存占用统计低报                                       |

---

## 3. 附录：审查覆盖率声明

### 已审查的项目文件与函数

#### 业务源码 `src/`

* **`src/bloom_filter.h`**
  `BloomFlags`，`HashPair`，`Hash32Policy`，`Hash64Policy`，`AsBytes`，`BloomLayer` 声明与字段布局。
* **`src/bloom_filter.cc`**
  `Hash32Policy::Compute`，`Hash64Policy::Compute`，`BloomLayer::~BloomLayer`，move ctor，move assign，`Create`，`FromRdb`，`Test`，`Insert`。
* **`src/sb_chain.h`**
  `FilterLayer`，`ScalingBloomFilter` 声明，`WireLayerMeta`，`WireFilterHeader`，`ComputeHeaderSize`，`SerializeHeader`，`DeserializeHeader` 声明。
* **`src/sb_chain.cc`**
  `ScalingBloomFilter` ctor/dtor，move ctor，move assign，`AppendLayer`，`ComputeHash`，`Put`，`Contains`，`TotalCapacity`，`BytesUsed`，`FromRdbShell`，`SetLayer`，`ComputeHeaderSize`，`SerializeHeader`，`DeserializeHeader`。
* **`src/bloom_commands.h`**
  `RegisterBloomCommands` 声明。
* **`src/bloom_commands.cc`**
  `GetFilter`，`AllocFilter`，`AllocDefaultFilter`，`OpenOrCreate`，`MatchArg`，`CmdReserve`，`CmdAdd`，`CmdMadd`，`CmdInsert`，`CmdExists`，`CmdMexists`，`CmdInfo`，`CmdCard`，`CmdScandump`，`CmdLoadchunk`，`RegisterBloomCommands`。
* **`src/bloom_rdb.h`**
  `BloomType`，编码版本常量，RDB/AOF/free/mem_usage 声明。
* **`src/bloom_rdb.cc`**
  `RdbLoadBloom`，`RdbSaveBloom`，`AofRewriteBloom`，`FreeBloom`，`BloomMemUsage`。
* **`src/bloom_config.h` / `src/bloom_config.cc`**
  `BloomConfig`，`g_bloomConfig`，`BloomConfigLoad`。 
* **`src/murmur2.h` / `src/murmur2.cc`**
  `MurmurHash2`，`MurmurHash64A`。 
* **`src/redis_bloom_module.cc`**
  `RedisModule_OnLoad`。
* **`src/rm_alloc.h`**
  `RMAlloc/RMCalloc/RMRealloc/RMFree` 包装。

#### 测试与构建

* **`test/bloom_filter_test.cc`** 全部测试用例。
* **`test/sb_chain_test.cc`** 全部测试用例。
* **`CMakeLists.txt`** 编译目标与测试清单，用于确认项目自有源码覆盖范围。
* **`README.md`** 构建/运行说明。
* **`doc/redis_bloom_survey.md`** 仅用于来源/许可证背景辅助判断，不作为权威合规依据。

#### 对比用的 RedisBloom 上游文件

* **`src/sb.c` / `src/sb.h`**
* **`deps/bloom/bloom.c` / `deps/bloom/bloom.h`**
* **`src/rebloom.c`**。    

### 信息不足或审查限制

1. **`include/redismodule.h`** 是 vendored 的 Redis Modules SDK 头文件，不属于你这份 Bloom 逻辑的自研核心；我没有把它当成你的业务实现去做逐函数安全审计。

2. 我使用的是仓库文件抓取视图，**没有稳定暴露精确行号**；因此本报告里的位置采用了用户要求允许的另一种形式：**文件名 + 函数名**。

3. 我没有在容器里实际编译/跑 ASan/UBSan，只做了静态代码审查。因此像 Bug #2 这种“标准层面的未定义行为”，我把它列为**确认问题**，依据是 C++ 对象生命周期规则，而不是运行时崩溃复现。

4. 许可证历史部分，我以 Redis 官方法律页面为主；对 **RedisBloom 2.3.x** 的精确许可证过渡点，我不确定，已明确标注。([Redis][1])

[1]: https://redis.io/legal/licenses/ "Licenses | Redis"
[2]: https://github.com/RedisBloom/RedisBloom "GitHub - RedisBloom/RedisBloom: Probabilistic Datatypes Module for Redis · GitHub"

