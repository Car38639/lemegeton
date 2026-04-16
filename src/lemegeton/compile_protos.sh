#!/bin/bash

PROTO_SRC="msg"

# 透過 python 指令查詢該模組的檔案位置並去除非必要的檔名
LEMEGETON_PATH=$(python3 -c "import lemegeton, os; print(os.path.dirname(lemegeton.__file__))" 2>/dev/null)

# 檢查是否成功取得路徑
if [ -z "$LEMEGETON_PATH" ]; then
    echo "錯誤: 找不到 lemegeton 的安裝路徑。請確保已執行 pip install ."
    exit 1
fi

if [ -z "$PROTO_SRC" ]; then
    echo "錯誤: 找不到 proto 檔案來源。請確保其位於msg/底下。"
    exit 1
fi


# 2. 尋找所有 .proto 檔案
find "$PROTO_SRC" -name "*.proto" | while read -r proto_file; do

    protoc -I$PROTO_SRC \
    -I$(dirname "$LEMEGETON_PATH") \
    --python_out="$LEMEGETON_PATH/msg" \
    $proto_file

    if [ $? -ne 0 ]; then
        echo "❌ 錯誤: $proto_file 編譯失敗！請檢查路徑或語法。"
        exit 1  # 立即終止腳本
    fi

    touch "$LEMEGETON_PATH/$(dirname "$proto_file")/__init__.py"

    echo "$proto_file 編譯完成！"
done
