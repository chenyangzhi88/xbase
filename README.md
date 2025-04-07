# XBase 基于 LSM 树和 Hash 表构建双索引存储引擎
1）基于一致性 Hash 和 Swiss Table 来构建 HashTable，相比开链式
策略其优点在于可以对 Hash 分块后存储在磁盘中。   
2）优化 MemTable 结构，不再使用 SkipList 低效的数据结构。  
3）优化了读放大，使得读的效率大大提高。之前读取一个 Key，需
要冗长的路径，从 MemTable 再到 Immutable MemTable 在到分层
的 Sstable，即使所在路径上的索引和数据都在内存中，检索路径也
过长。有了 Hash 索引之后，检索路径大大缩短。

