GO := $(shell which go)

CTRLER_CLIENT_BIN = bin/ctrler/ctrler-client
CTRLER_SERVER_BIN = bin/ctrler/ctrler-server
SHARDKV_CLIENT_BIN = bin/shardkv/shardkv-client
SHARDKV_SERVER_BIN = bin/shardkv/shardkv-server

CTRLER_CLIENT_SRC = cmd/ctrler/client/ctrler-client.go cmd/common/* shardctrler/*
CTRLER_SERVER_SRC = cmd/ctrler/server/ctrler-server.go cmd/common/*  shardctrler/* raft/*

SHARDKV_CLIENT_SRC = cmd/shardkv/client/shardkv-client.go cmd/common/* shardctrler/* shardkv/*
SHARDKV_SERVER_SRC = cmd/shardkv/server/shardkv-server.go cmd/common/* shardkv/* raft/*

GRPC_INPUT = rpcwrapper/grpc/raft/raft.proto
GRPC_OUTPUT = rpcwrapper/grpc/raft/raft.pb.go rpcwrapper/grpc/raft/raft_grpc.pb.go

all: $(CTRLER_CLIENT_BIN) $(CTRLER_SERVER_BIN) $(SHARDKV_CLIENT_BIN) $(SHARDKV_SERVER_BIN)

$(CTRLER_CLIENT_BIN): $(CTRLER_CLIENT_SRC) $(GRPC_OUTPUT)
	@echo "\033[1;32mBuilding ctrler-client...\033[0m"
	@mkdir -p $(dir $@)
	$(GO) build -o $@ $<

$(CTRLER_SERVER_BIN): $(CTRLER_SERVER_SRC) $(GRPC_OUTPUT)
	@echo "\033[1;32mBuilding ctrler-server...\033[0m"
	@mkdir -p $(dir $@)
	$(GO) build -o $@ $<

$(SHARDKV_CLIENT_BIN): $(SHARDKV_CLIENT_SRC) $(GRPC_OUTPUT)
	@echo "\033[1;32mBuilding shardkv-client...\033[0m"
	@mkdir -p $(dir $@)
	$(GO) build -o $@ $<

$(SHARDKV_SERVER_BIN): $(SHARDKV_SERVER_SRC) $(GRPC_OUTPUT)
	@echo "\033[1;32mBuilding shardkv-server...\033[0m"
	@mkdir -p $(dir $@)
	$(GO) build -o $@ $<

$(GRPC_OUTPUT): $(GRPC_INPUT)
	@echo "\033[1;32mGenerating gRPC code...\033[0m"
	protoc --go_out=. --go_opt=paths=source_relative \
    --go-grpc_out=. --go-grpc_opt=paths=source_relative \
    rpcwrapper/grpc/raft/raft.proto

tmux: $(CTRLER_CLIENT_BIN) $(CTRLER_SERVER_BIN) $(SHARDKV_CLIENT_BIN) $(SHARDKV_SERVER_BIN) $(GRPC_OUTPUT)
	./tmux-test.sh

clean:
	@echo "\033[1;32mCleaning up...\033[0m"
	rm -rf $(CTRLER_CLIENT_BIN)
	rm -rf $(CTRLER_SERVER_BIN)
	rm -rf $(SHARDKV_CLIENT_BIN)
	rm -rf $(SHARDKV_SERVER_BIN)

	rm -rf bin/raftstate
	rm -rf bin/snapshot

.PHONY: all clean help
