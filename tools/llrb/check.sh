#!/usr/bin/env bash

go build

# llrb load -n 8000000 -klen 64,128 -vlen 0,0 -nodearena 96,256,6000000000,1000000 -stats 1000

# arena {minblock, maxblock, arena-capacity, pool-capacity}
#llrb check -repeat 100000 -nodearena 96,256,3000000000,1048576 -valarena 96,2048,3000000000,1048576
llrb check -mvcc 100 -repeat 100000 -nodearena 96,256,3000000000,1048576 -valarena 96,2048,3000000000,1048576
rm llrb
