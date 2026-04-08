#!/bin/bash

set -e

usage() {
  echo "Usage: $0 -p <proto_file>"
  echo
  echo "Options:"
  echo "  -p    proto path"
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
    p) PROTO_PATH=$OPTARG ;;
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

if [ -z "$PROTO_PATH" ]; then
  echo "❌ Error: -p is required"
  echo
  usage
  exit 1
fi


if [ ! -f "$PROTO_PATH" ]; then
  echo "❌ Error: cannot find $PROTO_PATH"
  exit 1
fi

# # ===== 新增資料夾邏輯 =====
# INIT_FILE="$PROTO_PATH/__init__.py"
# if [ ! -f "$INIT_FILE" ]; then
#   touch "$INIT_FILE"
# fi


cd "$WORKSPACE/libs/protocol/src"
protoc -I=. --python_out=. "solproto/msg/$PROTO_PATH"

echo "✅ Generated $PROTO_PATH python message."
