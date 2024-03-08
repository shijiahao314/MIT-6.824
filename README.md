# [MIT 6.824 (6.5840) 分布式系统课程](http://nil.csail.mit.edu/6.5840/2023/index.html)

> **What is 6.5840 about?**
> 6.5840 is a core 12-unit graduate subject with lectures, readings, programming labs, an optional project, a mid-term exam, and a final exam. It will present abstractions and implementation techniques for engineering distributed systems. Major topics include fault tolerance, replication, and consistency. Much of the class consists of studying and discussing case studies of distributed systems.

MIT 6.824 (6.5840) 是麻省理工大学一门非常著名的分布式系统入门课程，该课程实验一共包含四个实验：

1. 实验一：实现一个 MapReduce 框架；
2. 实验二：实现 Raft 共识算法的 leader 选举、日志复制、日志快照、持久化功能；
3. 实验三：基于实验二实现的 Raft 共识算法，实现一个分布式的 KV 存储；
4. 实验四：基于实验三的分布式 KV 存储，添加存储分片的功能；

该门课程主要使用 Golang 语言实现，涉及到 Go 的并发通信、分布式存储原理、RPC 通信等，具有一定挑战性。

## Lab1: MapReduce

[实验指导](http://nil.csail.mit.edu/6.5840/2023/labs/lab-mr.html)

相关文件夹：`mapreduce`

## Lab2: Raft

[实验指导](http://nil.csail.mit.edu/6.5840/2023/labs/lab-raft.html)

相关文件夹：`raft`

## Lab3: Fault-tolerant KV

[实验指导](http://nil.csail.mit.edu/6.5840/2023/labs/lab-kvraft.html)

相关文件夹：`kvraft`

## Lab4: Shared KV

[实验指导](http://nil.csail.mit.edu/6.5840/2023/labs/lab-shard.html)

相关文件夹：`shardctrler`、`shardkv`

## 其他文件夹

- `debugutils`: 自定义 Debug 工具文件夹，主要提供日志输出功能；
- `labgob`: 项目框架提供的序列化工具；
- `labrpc`: 项目框架提供的 RPC 调用架构；
- `models`: 项目框架提供的 KV 存储模型定义；
- `porcupine`: 项目框架提供的验证数据一致性和可视化分析工具；

## 最后

为什么要将多种类型（如：Put/Append/PutAppend）的 RPC 参数放在一个结构体，[解释](https://zhuanlan.zhihu.com/p/463146544)如下：

```text
此外可能有同学觉得将不同的 rpc 参数都塞到一个结构体中并不是一个好做法，
这虽然简化了客户端和服务端的逻辑，但也导致多传输了许多无用数据。
对于 6.824 来说，的确是这样，
但对于成熟的 rpc 框架例如 gRPC，thrift 等，其字段都可以设置成 optional，
在实际传输中，只需要一个 bit 就能够区分是否含有对应字段，这不会是一个影响性能的大问题。
因此在我看来，多传输几个 bit 数据带来的弊端远不如简化代码逻辑的收益大。
```
