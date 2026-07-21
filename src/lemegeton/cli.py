"""lemegeton 統一 CLI (子指令架構)。

用法:
    lemegeton create [PATH]      在專案 package 內建立 msg/ 骨架
    lemegeton compile [...]      編譯 <pkg>/msg/ 下的 protobuf

各子指令的參數由對應模組提供:
    create  → lemegeton.create
    compile → lemegeton.compile_protos
"""

from __future__ import annotations

import argparse
from typing import List, Optional

from lemegeton import compile_protos as _compile
from lemegeton import create as _create


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="lemegeton",
        description="lemegeton 專案工具 CLI。",
    )
    sub = parser.add_subparsers(dest="command", metavar="<command>", required=True)

    p_create = sub.add_parser(
        "create",
        help="在專案 package 內建立 msg/ 骨架 (__init__.py + template/template.proto)。",
        description="在專案 package 內建立 msg/ 骨架。",
    )
    _create.add_create_arguments(p_create)
    p_create.set_defaults(_handler=_create.run_create)

    p_compile = sub.add_parser(
        "compile",
        help="編譯 <pkg>/msg/ 下的 protobuf (仿 ROS 機制)。",
        description="編譯任意套件 <pkg>/msg/ 底下的 protobuf 訊息。",
    )
    _compile.add_compile_arguments(p_compile)
    p_compile.set_defaults(_handler=_compile.run_compile)

    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    return args._handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
