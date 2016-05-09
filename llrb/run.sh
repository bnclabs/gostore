#!/usr/bin/env bash

export GOMAXPROCS=8

go build -tags dict

doload() {
    params="load -n 5000000 -par 1 -klen 64,128 -nodearena 96,256,6000000000,1000000 $*"
    echo "$params ..."
    llrb $params
    echo
}

# arena {minblock, maxblock, arena-capacity, pool-capacity}
doverify() {
    params="verify -repeat 100000 -nodearena 32,512,3000000000,1048576 -valarena 32,3072,3000000000,1048576 $*"
    echo "$params ..."
    llrb $params
    echo
}

doverifymvcc() {
    params="verify -mvcc 100 -repeat 100000 -nodearena 32,512,3000000000,1048576 -valarena 32,3072,3000000000,1048576 $*"
    echo "$params ..."
    llrb $params
    echo
}

if [ -z "$1" ]; then
    doload -vlen 0,0

elif [ $1 == "load" ]; then
    doload -batchsize 100 -vlen 0,0
    doload -mvcc 1 -batchsize 100 -vlen 0,0

elif [ $1 == "verify" ]; then
    doverify

elif [ $1 == "verifymvcc" ]; then
    doverifymvcc
fi

rm llrb
