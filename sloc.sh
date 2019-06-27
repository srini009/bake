#!/bin/bash
CLIENT=(include/bake-client.h include/bake.h src/bake-client.c src/bake-mkpool.c src/util.c)
SERVER=(include/bake-server.h src/bake-rpc.h src/bake-server.c src/bake-timing.h)
echo "************** CLIENT ***************"
sloccount "${CLIENT[@]}"

echo "************** SERVER ***************"
sloccount "${SERVER[@]}"
