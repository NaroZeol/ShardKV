GO := $(shell which go)

CTRLER_CLIENT_BIN = bin/ctrler/ctrler-client
CTRLER_SERVER_BIN = bin/ctrler/ctrler-server
SHARDKV_CLIENT_BIN = bin/shardkv/shardkv-client
SHARDKV_SERVER_BIN = bin/shardkv/shardkv-server

CTRLER_CLIENT_SRC = cmd/ctrler/client/ctrler-client.go
CTRLER_SERVER_SRC = cmd/ctrler/server/ctrler-server.go
SHARDKV_CLIENT_SRC = cmd/shardkv/client/shardkv-client.go
SHARDKV_SERVER_SRC = cmd/shardkv/server/shardkv-server.go

all: $(CTRLER_CLIENT_BIN) $(CTRLER_SERVER_BIN) $(SHARDKV_CLIENT_BIN) $(SHARDKV_SERVER_BIN)

$(CTRLER_CLIENT_BIN): $(CTRLER_CLIENT_SRC)
	@echo "Building ctrler-client..."
	mkdir -p $(dir $@)
	$(GO) build -o $@ $<

$(CTRLER_SERVER_BIN): $(CTRLER_SERVER_SRC)
	@echo "Building ctrler-server..."
	mkdir -p $(dir $@)
	$(GO) build -o $@ $<

$(SHARDKV_CLIENT_BIN): $(SHARDKV_CLIENT_SRC)
	@echo "Building shardkv-client..."
	mkdir -p $(dir $@)
	$(GO) build -o $@ $<

$(SHARDKV_SERVER_BIN): $(SHARDKV_SERVER_SRC)
	@echo "Building shardkv-server..."
	mkdir -p $(dir $@)
	$(GO) build -o $@ $<

clean:
	@echo "Cleaning up..."
	rm -rf $(CTRLER_CLIENT_BIN)
	rm -rf $(CTRLER_SERVER_BIN)
	rm -rf $(SHARDKV_CLIENT_BIN)
	rm -rf $(SHARDKV_SERVER_BIN)

	rm -f bin/raftstate/*
	rm -f bin/snapshot/*

.PHONY: all clean help
