#!/usr/bin/env bash

go build

# arena {minblock, maxblock, arena-capacity, pool-capacity}
#llrb check -repeat 100000 -nodearena 96,256,3000000000,1048576 -valarena 96,2048,3000000000,1048576
llrb check -mvcc -repeat 100000 -nodearena 96,256,3000000000,1048576 -valarena 96,2048,3000000000,1048576
