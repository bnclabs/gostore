#!/usr/bin/env bash

go build

# arena {minblock, maxblock, arena-capacity, pool-capacity}
llrb validate -repeat 100000 -nodearena 96,256,3000000000,1048576 -valarena 96,2048,3000000000,1048576
