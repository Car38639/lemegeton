#!/bin/bash

set -e

usage() {
  echo "Usage: $0 -p <proto_file>"
  echo
  echo "Options:"
  echo "  -p    proto file"
  echo "  -h    Show this help message"
  echo
  echo "Example:"
  echo "  $0 -p template/template.proto"
}

if [ $# -eq 0 ]; then
  usage
  exit 1
fi

while getopts "p:h" opt; do
  case $opt in
    p) PROTO_FILE=$OPTARG ;;
    h)
      usage
      exit 0
      ;;
    *)
      usage
      exit 1
      ;;
  esac
done

if [ -z "$PROTO_FILE" ]; then
  echo "❌ Error: -p is required"
  echo
  usage
  exit 1
fi


if [ ! -f "$PROTO_FILE" ]; then
  echo "❌ Error: proto file not found: $PROTO_FILE"
  exit 1
fi


cd "$WORKSPACE/libs/protocol/src"

python -m grpc_tools.protoc \
  -I. \
  --python_out=. \
  --grpc_python_out=. \
  "lemegeton/msg/$PROTO_FILE"

echo "✅ Generated $PROTO_FILE protobuf grpc python files."
