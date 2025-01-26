# ShardKV

本项目基于MIT-6.5840（2024版本）的[最后一个lab](https://pdos.csail.mit.edu/6.824/labs/lab-shard.html)

> 一个键值存储系统，通过一组副本组对键进行分片或分区。一个分片是键/值对的一个子集；例如，所有以“a”开头的键可能是一个分片，所有以“b”开头的键可能是另一个分片，依此类推。分片的原因是性能。每个副本组仅处理几个分片的puts和gets，而且这些组是并行操作的；因此，总系统吞吐量（每单位时间的puts和gets）与组数成正比。

简单来说就是一个基于[Raft协议](https://raft.github.io/)实现的分布式的键值存储系统，支持分片，副本，故障恢复等功能。

[English doc](README.md)

## 项目结构

- `cmd/`：包含了一些简单的CLI实现，可以用来启动一个分片键值存储服务器
- `raft/`：Raft协议的实现
- `shardkv/`：分片键值存储系统的实现
- `shardctrler/`：分片控制器的实现

## 如何使用

根据本仓库中的代码可以构建一个自己的键值存储系统，在`cmd/`中，有一些简单的CLI实现，可以用来启动一个分片键值存储服务器，不过这个CLI的实现不怎么友好，可以根据需要修改。

### 构建项目

通过`make`构建项目

如果使用`make tmux`，可以根据**默认配置**在本地启动一个分片键值存储服务器集群，包括一个分片控制器和两个分片键值存储服务器，所有的进程都会在tmux中启动。

### CLI参数

```
shardkv-server:
    -c string
        配置文件路径，必须指定
    -g int
        组ID，必须指定
    -i int
        组内服务器ID，必须指定
    -m int
        触发快照的最大内存大小（单位：字节）

shardkv-client:
    -c string
            配置文件路径，必须指定
    -i      启用交互模式
    -v      输出详细信息

ctrler-server:
    -c string
        配置文件路径，必须指定
    -i int
        服务器ID，必须指定
    -m int
        触发快照的最大内存大小（单位：字节）

ctrler-client:
    -c string
            配置文件路径，必须指定
    -i      启用交互模式
    -v      输出详细信息
```

### 配置文件

仓库内的bin/目录下有一个默认的配置文件`config.json`，可以根据需要修改。

### 命令行客户端

通过命令与客户端交互

**shardkv-client支持的命令：**

- `get <key>`：获取键值
- `put <key> <value>`：设置键值
- `append <key> <value>`：追加值

**ctrler-client支持的命令：**

交互模式可能有些奇怪，要求先输入命令，然后再输入参数，这样做更方便解析

- `join`：

    加入组，后续输入组ID和服务器地址，直到输入一个空行为止，例如：
    ```
    ctrler> join
    <format>: gid server1 server2 ... end with empty line
    0 localhost:5001 localhost:5002 localhost:5003
    1 localhost:5004 localhost:5005 localhost:5006 localhost:5007

    ctrler>
    ```

- `leave`：

    离开（删除）组，后续输入组ID，直到输入一个空行为止，例如：
    ```
    ctrler> leave
    <format>: gid1 gid2 ... end with empty line
    0 1

    ctrler>
    ```
- `move`: 暂不支持，因为没有必要

- `query`：

    查询指定的下标的配置，后续输入组ID。当输入为-1时返回最新的配置，例如：
    ```
    ctrler> query
    Num: -1
    {1 [0 1 0 1 0 1 0 1 0 1] map[0:[localhost:5000 localhost:5001 localhost:5002 localhost:5003 localhost:5004] 1:[localhost:5005 localhost:5006 localhost:5007]]}
    ctrler>
    ```