# ShardKV 

This project is based on the [final lab](https://pdos.csail.mit.edu/6.824/labs/lab-shard.html) of MIT 6.5840 (2024 edition).

> In this lab you'll build a key/value storage system that "shards," or partitions, the keys over a set of replica groups. A shard is a subset of the key/value pairs; for example, all the keys starting with "a" might be one shard, all the keys starting with "b" another, etc. The reason for sharding is performance. Each replica group handles puts and gets for just a few of the shards, and the groups operate in parallel; thus total system throughput (puts and gets per unit time) increases in proportion to the number of groups.

In simple terms, this is a distributed key-value storage system built on the [Raft protocol](https://raft.github.io/). It supports sharding, replication, fault recovery, and other features.

[简体中文](README-zh_cn.md)

## Project Structure

- `cmd/`: Contains simple CLI implementations for starting a shard-based key-value storage server.
- `raft/`: Implementation of the Raft protocol.
- `shardkv/`: Implementation of the sharded key-value storage system.
- `shardctrler/`: Implementation of the shard controller.

## How to Use

Using the code in this repository, you can build your own key-value storage system. The `cmd/` directory contains simple CLI implementations for starting a sharded key-value storage server. These implementations are basic and can be modified as needed.

### Building the Project

Build the project using `make`.

If you use `make tmux`, you can start a local cluster of sharded key-value storage servers, including a shard controller and two sharded key-value storage servers, based on the **default configuration**. All processes will be started in tmux.

### CLI Parameters

```plaintext
shardkv-server:
    -c string
        Path to the configuration file (required).
    -g int
        Group ID (required).
    -i int
        Server ID within the group (required).
    -m int
        Max Raft state size(bytes) (default 8192)

shardkv-client:
    -c string
        Path to the configuration file (required).
    -i      Enable interactive mode.
    -v      Enable verbose output.

ctrler-server:
    -c string
        Path to the configuration file (required).
    -i int
        Server ID (required).
    -m int
        Max Raft state size(bytes) (default 8192)

ctrler-client:
    -c string
        Path to the configuration file (required).
    -i      Enable interactive mode.
    -v      Enable verbose output.
```

### Configuration File

A default configuration file, `config.json`, is provided in the `bin/` directory. You can modify it as needed to suit your requirements.

### Commands

Interact with the system using the following commands:

**Commands supported by `shardkv-client`:**

- `get <key>`: Get the value for the specified key.
- `put <key> <value>`: Set the value for the specified key.
- `append <key> <value>`: Append the value for the specified key.

**Commands supported by `ctrler-client`:**

The interactive mode requires entering the command first, followed by parameters, which simplifies parsing.

- `join`:

    Add a group by providing the group ID and server addresses. End input with an empty line. For example:
    ```
    ctrler> join
    <format>: gid server1 server2 ... end with empty line
    0 localhost:5001 localhost:5002 localhost:5003
    1 localhost:5004 localhost:5005 localhost:5006 localhost:5007

    ctrler>
    ```

- `leave`:

    Remove (leave) a group by providing group IDs. End input with an empty line. For example:
    ```
    ctrler> leave
    <format>: gid1 gid2 ... end with empty line
    0 1

    ctrler>
    ```

- `query`:

    Query the configuration for a specific index. Input the group ID. If the input is -1, the latest configuration is returned. For example:
    ```
    ctrler> query
    Num: -1
    {1 [0 1 0 1 0 1 0 1 0 1] map[0:[localhost:5000 localhost:5001 localhost:5002 localhost:5003 localhost:5004] 1:[localhost:5005 localhost:5006 localhost:5007]]}
    ctrler>
    ```