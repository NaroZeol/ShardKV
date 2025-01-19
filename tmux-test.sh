#!/bin/bash
cd bin || exit

tmux new-session -d -s shardkv

tmux set -g base-index 1
tmux setw -g pane-base-index 1
tmux set -g renumber-windows on

tmux new-window -t 1 -k
tmux kill-window -a -t 1
tmux select-window -t 1
tmux rename-window -t 1 "ctrler"

# ShardControllers
tmux split-window -v
tmux split-window -v
tmux split-window -v
tmux select-layout main-horizontal

tmux send-keys -t 1 "./ctrler/ctrler-client -c config.json" C-m
for i in {2..4}; do
  tmux send-keys -t "$i" "./ctrler/ctrler-server -c config.json -i $((i - 2))" C-m
done

# ShardServers group0
tmux new-window -n group0

tmux split-window -h
tmux split-window -v
tmux split-window -h
tmux split-window -v
tmux select-layout even-vertical

for i in {1..5}; do
  tmux send-keys -t "$i" "./shardkv/shardkv-server -c config.json -g 0 -i $((i - 1))" C-m
done

# ShardServers group1
tmux new-window -n group1

tmux split-window -v
tmux split-window -v
tmux select-layout even-vertical

for i in {1..3}; do
  tmux send-keys -t "$i" "./shardkv/shardkv-server -c config.json -g 1 -i $((i - 1))" C-m
done

# Join the shard servers
tmux select-window -t 1
tmux send-keys -t 1 "join" C-m
tmux send-keys -t 1 "0 localhost:5000 localhost:5001 localhost:5002 localhost:5003 localhost:5004" C-m
tmux send-keys -t 1 "1 localhost:5005 localhost:5006 localhost:5007" C-m C-m

# ShardClients
tmux new-window -n client
tmux send-keys -t 1 "./shardkv/shardkv-client -c config.json" C-m

tmux attach -t shardkv