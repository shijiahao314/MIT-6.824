# MIT 6.824 (6.5840) 分布式系统课程
课程主页：
http://nil.csail.mit.edu/6.5840/2023/index.html

## Lab1: MapReduce
http://nil.csail.mit.edu/6.5840/2023/labs/lab-mr.html
相关文件夹：

## Lab2: Raft
http://nil.csail.mit.edu/6.5840/2023/labs/lab-raft.html
相关文件夹：
- raft

## Lab3: Fault-tolerant KV
http://nil.csail.mit.edu/6.5840/2023/labs/lab-kvraft.html
相关文件夹：
- kvraft


## Lab4: Shared KV
http://nil.csail.mit.edu/6.5840/2023/labs/lab-shard.html
相关文件夹：
- shardctrler
- shardkv

## 其他文件夹
- debugutils: 自定义Debug工具文件夹
- labgob: 
- labrpc
- models: 
- porcupine: 

```text
此外可能有同学觉得将不同的 rpc 参数都塞到一个结构体中并不是一个好做法，
这虽然简化了客户端和服务端的逻辑，但也导致多传输了许多无用数据。
对于 6.824 来说，的确是这样，
但对于成熟的 rpc 框架例如 gRPC，thrift 等，其字段都可以设置成 optional，
在实际传输中，只需要一个 bit 就能够区分是否含有对应字段，这不会是一个影响性能的大问题。
因此在我看来，多传输几个 bit 数据带来的弊端远不如简化代码逻辑的收益大。
```
from: https://zhuanlan.zhihu.com/p/463146544