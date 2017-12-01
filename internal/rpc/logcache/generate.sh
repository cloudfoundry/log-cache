#!/bin/bash

dir_resolve()
{
    cd "$1" 2>/dev/null || return $?  # cd to desired directory; if fail, quell any error messages but return exit status
    echo "`pwd -P`" # output full, link-resolved path
}

set -e

TARGET=`dirname $0`
TARGET=`dir_resolve $TARGET`
cd $TARGET

go get github.com/golang/protobuf/{proto,protoc-gen-go}


tmp_dir=$(mktemp -d)
mkdir -p $tmp_dir/log-cache

cp $GOPATH/src/code.cloudfoundry.org/log-cache/api/*proto $tmp_dir/log-cache

protoc \
    $tmp_dir/log-cache/*.proto \
    --go_out=plugins=grpc,Mv2/envelope.proto=code.cloudfoundry.org/go-loggregator/rpc/loggregator_v2:. \
    --proto_path=$tmp_dir/log-cache \
    -I=$tmp_dir/log-cache \
    -I=$GOPATH/src/code.cloudfoundry.org/loggregator-api/.


rm -r $tmp_dir
