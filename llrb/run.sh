#!/usr/bin/env bash

export GOMAXPROCS=8

go build -tags dict

doload() {
    llrb load -n 5000000 -par 1 -klen 64,128 -nodearena 96,256,6000000000,1000000 $*
}

# arena {minblock, maxblock, arena-capacity, pool-capacity}
doverify() {
    llrb verify -repeat 100000 -nodearena 96,256,3000000000,1048576 -valarena 96,2048,3000000000,1048576
}

doverifymvcc() {
    llrb verify -mvcc 100 -repeat 100000 -nodearena 96,256,3000000000,1048576 -valarena 96,2048,3000000000,1048576
}

if [ -z "$1" ]; then
    doload -vlen 0,0

elif [ $1 == "load" ]; then
    echo "doload -batchsize 100 -vlen 0,0 ........."
    doload -batchsize 100 -vlen 0,0
    echo
    echo "doload -mvcc 1 -batchsize 100 -vlen 0,0 ........."
    doload -mvcc 1 -batchsize 100 -vlen 0,0
    echo

elif [ $1 == "verify" ]; then
    doverify

elif [ $1 == "verifymvcc" ]; then
    doverifymvcc
fi

rm llrb
