"""`lemegeton create` — 在專案 package 內建立 msg/ 骨架。

從當前目錄往上爬找到專案根 (見 lemegeton.project), 取得 package 後於其中建立:
    <pkg>/msg/__init__.py
    <pkg>/msg/template/template.proto
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from lemegeton import project

# template/template.proto 的預設內容 (與 repo 現有 template 一致)
TEMPLATE_PROTO = """syntax = "proto3";

package template;

service Service {
  rpc REQUEST (MESSAGE) returns (RESPONSE);
}

message MESSAGE {
  string name = 1;
  int32 age = 2;
}

message RESPONSE {
  string message = 1;
}
"""


def run_create(args: argparse.Namespace) -> int:
    """執行 create; 回傳 exit code。"""
    start = Path(args.path).resolve() if getattr(args, "path", None) else Path.cwd()
    info = project.detect(start)
    if info is None:
        print(
            "❌ 找不到專案 (往上爬找不到 pyproject.toml / CMakeLists.txt, 或無法解析 name)。",
            file=sys.stderr,
        )
        return 1

    if info.source == "cmake":
        print(
            "ℹ️  由 CMakeLists.txt 取得名稱 "
            "(C++ 專案的 msg 佈局可能與 Python 不同, __init__.py 為 Python 專用)。"
        )
    print(f"專案根: {info.root}")
    print(f"package name: {info.name}  (Python package: {info.package})")

    # 定位 / 建立 package 目錄
    pkgdir = info.package_dir
    if not pkgdir.is_dir():
        pkgdir.mkdir(parents=True, exist_ok=True)
        (pkgdir / "__init__.py").touch(exist_ok=True)
        print(f"📁 建立 package 目錄: {pkgdir.relative_to(info.root)}")

    # 建立 msg/ 骨架
    msgdir = pkgdir / "msg"
    (msgdir / "template").mkdir(parents=True, exist_ok=True)

    init_file = msgdir / "__init__.py"
    if not init_file.exists():
        init_file.touch()
        print(f"✅ 建立 {init_file.relative_to(info.root)}")
    else:
        print(f"⏭️  已存在, 略過 {init_file.relative_to(info.root)}")

    proto_file = msgdir / "template" / "template.proto"
    if not proto_file.exists():
        proto_file.write_text(TEMPLATE_PROTO, encoding="utf-8")
        print(f"✅ 建立 {proto_file.relative_to(info.root)}")
    else:
        print(f"⏭️  已存在, 略過 {proto_file.relative_to(info.root)}")

    print("🎉 完成! 之後可用 'lemegeton compile' 編譯 msg/ 下的 .proto。")
    return 0


def add_create_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """把 create 相關的參數掛到指定 parser 上。"""
    parser.add_argument(
        "path",
        nargs="?",
        default=None,
        help="起始目錄 (預設: 當前目錄; 會往上爬尋找專案根)。",
    )
    return parser
