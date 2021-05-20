# 数据库学习的相关资料
 - PingCAP数据库学习资料汇总: [awesome-database-learning](https://github.com/pingcap/awesome-database-learning)
 - UC Berkeley CS186: [旧CS186 fall2013](https://sites.google.com/site/cs186fall2013/) [新CS W186](https://cs186berkeley.net/)
 - MIT 6.824: [MIT 6.824](https://pdos.csail.mit.edu/6.824/) [如何才能更好的学习 MIT 6.824 ？](https://zhuanlan.zhihu.com/p/110168818)
 - 参考书: [数据库系统概念（中文第六版）](.//数据库系统概念（中文第六版）.pdf)


# Project 1 Records, Files & Buffers
  [Project 1 Records, Files & Buffers](https://sites.google.com/site/cs186fall2013/homeworks/project-1)

## Fields and Tuples
  字段
  - [Field](./src/java/simpledb/Field.java) 字段, interface class, IntField和StringField是它的实现
    * [Type](./src/java/simpledb/Type.java) 字段类型, 只有两种: integer和string
    * [IntField](./src/java/simpledb/IntField.java) int类型字段
    * [StringField](./src/java/simpledb/StringField.java) string类型字段, 定长

  记录
  - [Tuple](./src/java/simpledb/Tuple.java) 元组(表的存储记录, select查询的返回结果或者中间结果), 包含tupleDesc RecordId fieldList
    * [RecordId](./src/java/simpledb/RecordId.java) Tuple的ID, HeapPage的ID + tuple在page内的ID
    * [TupleDesc](./src/java/simpledb/TupleDesc.java) 元组的描述(表的定义, select查询的返回结果或者中间结果的定义)
      * [TDItem](./src/java/simpledb/TupleDesc.java) 一个Field对应的定义, 包含type和name

## Catalog
  目录: 数据库中所有表, 表别名, 表ID, 表对应的文件
  - [Catalog](./src/java/simpledb/Catalog.java) 目录, load table的定义并存储

## BufferPool
  缓冲区: 内存中用于存储磁盘块的拷贝的那一部分. 每个块总有一个拷贝存放在磁盘上, 但是在磁盘上的拷贝可能比在缓冲区的拷贝旧.
  - [BufferPool](./src/java/simpledb/BufferPool.java) 缓存池, 读写page, Catalog

## HeapFile access method
  堆文件组织: 一条记录可以放在文件中的任何地方, 只要那个地方有空间存放这条记录. 记录是没有顺序的. 通常每个关系使用一个单独的文件.
  计算page中有多少tuple:
    `tupsPerPage = floor((BufferPool.PAGE_SIZE * 8) / (tuple size * 8 + 1))`
  计算page中header大小:
    `headerBytes = ceiling(tupsPerPage/8)`

  - [Page](./src/java/simpledb/Page.java) 存储的页
    * [HeapPage](./src/java/simpledb/HeapPage.java) HeapPage主要包括HeapPageId TupleDesc header tuples
      * [HeapPageId](./src/java/simpledb/HeapPageId.java) HeapPage的ID tableID(table file path hash code) + pageNo
  - [File](./src/java/simpledb/File.java) 文件
    * [HeapFile](./src/java/simpledb/HeapFile.java)

## Operators
  - [SeqScan](./src/java/simpledb/SeqScan.java) 顺序扫描