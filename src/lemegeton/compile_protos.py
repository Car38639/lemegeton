"""通用 protobuf 編譯工具 — 適用於任意 Python 套件 (仿 ROS 的訊息編譯機制)。

把任意套件 ``mypkg`` (其 ``mypkg/msg/`` 底下放 ``.proto``) 編譯成
可用 ``import mypkg.msg.<group>.<name>_pb2`` 直接引用的模組。

核心規則
--------
protoc 生成的 Python import 路徑, **完全由 .proto 相對於 include root (-I)
的路徑決定** (與 .proto 內的 ``package`` 宣告、與執行目錄都無關)。因此本工具固定:

    include root  = 目標套件的「上一層」目錄
    --python_out  = 同一層

於是位於 ``mypkg/msg/foo/bar.proto`` 的檔案 → 生成 ``mypkg/msg/foo/bar_pb2.py``,
且內部跨檔引用一律為 ``from mypkg.msg.foo import ..._pb2``。

前提: 每支 .proto 內的 import 也必須以 include root 起算的完整路徑書寫,
例如 ``import "mypkg/msg/common/std.proto";``。

定位目標套件
------------
* 預設: 從當前目錄往上爬偵測所在專案 (見 lemegeton.project), 作用在當前專案
  而非工具的安裝位置 —— 因此一般 (非 editable) 安裝也能正常運作。
* 或用 ``--pkg-dir`` 明確指定任一套件目錄 (可跨專案 / 出樹使用)。

開箱即用
--------
優先使用 pip 套件 ``grpcio-tools`` 內建的 protoc (``python -m grpc_tools.protoc``,
同時內建 ``google/protobuf/*`` well-known types); 若不存在則退回系統的 ``protoc``。

CLI 用法
--------
    compile_protos                          # 編譯目標套件的全部 proto
    compile_protos --pkg-dir /path/to/mypkg # 指定任意套件目錄
    compile_protos --msg-dir messages       # msg 子目錄改用其它名稱
    compile_protos -p humanoid              # 只編某個 group (msg 下第一層資料夾)
    compile_protos --proto humanoid/state.proto   # 只編單一檔案
    compile_protos --list                   # 列出發現的 group / proto
    compile_protos --clean                  # 清除生成的 *_pb2.py
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

from lemegeton import project


def _protoc_prefix() -> Optional[List[str]]:
    """回傳呼叫 protoc 的指令前綴; 找不到任何可用 protoc 時回傳 None。"""
    try:
        import grpc_tools.protoc  # noqa: F401  (僅確認可 import)

        # `python -m grpc_tools.protoc` 會自動附上內建的 well-known types include
        return [sys.executable, "-m", "grpc_tools.protoc"]
    except ImportError:
        system_protoc = shutil.which("protoc")
        if system_protoc:
            return [system_protoc]
    return None


class ProtoCompiler:
    """把某個套件 ``<package_dir>/<msg_subdir>/`` 底下的 .proto 編譯成可 import 的模組。

    :param package_dir: 目標 Python 套件目錄 (例如 ``.../mypkg``)。
    :param msg_subdir:  存放 .proto 的子目錄名稱 (預設 ``msg``)。
    """

    def __init__(self, package_dir: Path, msg_subdir: str = "msg"):
        self.package_dir = Path(package_dir).resolve()
        self.package_name = self.package_dir.name
        # protoc 的 -I 與 --python_out 都設在套件的上一層, 才能得到 <pkg>.<msg>.* 的 import
        self.include_root = self.package_dir.parent
        self.msg_dir = self.package_dir / msg_subdir

    # -- 顯示輔助 ---------------------------------------------------------
    def _rel(self, path: Path) -> str:
        try:
            return str(path.relative_to(Path.cwd()))
        except ValueError:
            return str(path)

    # -- 發現 -------------------------------------------------------------
    def discover_protos(self, group: Optional[str] = None) -> List[Path]:
        """遞迴找出所有 .proto; 指定 ``group`` 則只找該子群組下的檔案。"""
        root = self.msg_dir / group if group else self.msg_dir
        if not root.exists():
            return []
        return sorted(root.rglob("*.proto"))

    def list_groups(self) -> List[str]:
        """列出所有 group (``msg/`` 下含有 .proto 的第一層資料夾)。"""
        groups = set()
        for proto in self.discover_protos():
            rel = proto.relative_to(self.msg_dir)
            if len(rel.parts) > 1:
                groups.add(rel.parts[0])
        return sorted(groups)

    def resolve_single(self, proto: str) -> Optional[Path]:
        """把使用者輸入的 ``--proto`` 參數解析成實際的 .proto 路徑 (接受多種寫法)。"""
        given = Path(proto)
        candidates = [
            given,                        # 絕對路徑或相對當前目錄
            self.msg_dir / proto,         # 相對 msg/
            self.include_root / proto,    # 相對 include root (mypkg/msg/...)
            Path.cwd() / proto,
        ]
        for cand in candidates:
            if cand.is_file() and cand.suffix == ".proto":
                return cand.resolve()
        return None

    # -- 生成後處理 -------------------------------------------------------
    def ensure_init_files(self) -> None:
        """為套件、msg 目錄及其所有子目錄補上 ``__init__.py`` (讓 package 可 import)。"""
        dirs = [self.package_dir, self.msg_dir]
        dirs += [p for p in self.msg_dir.rglob("*") if p.is_dir()]
        for directory in dirs:
            init_file = directory / "__init__.py"
            if not init_file.exists():
                init_file.touch()

    # -- 動作 -------------------------------------------------------------
    def compile(self, protos: List[Path]) -> int:
        """呼叫 protoc 編譯指定的 .proto 清單。回傳 exit code。"""
        prefix = _protoc_prefix()
        if prefix is None:
            print(
                "❌ 錯誤: 找不到 protoc。請安裝 `pip install grpcio-tools`,"
                " 或在系統上安裝 protobuf-compiler。",
                file=sys.stderr,
            )
            return 1

        cmd = [
            *prefix,
            f"-I{self.include_root}",
            f"--python_out={self.include_root}",
            *[str(p) for p in protos],
        ]
        result = subprocess.run(cmd)
        if result.returncode != 0:
            print("❌ 編譯失敗, 請檢查 .proto 語法或 import 路徑。", file=sys.stderr)
            return result.returncode

        self.ensure_init_files()
        return 0

    def clean(self) -> int:
        """移除 ``msg/`` 底下所有生成的 ``*_pb2.py`` / ``*_pb2_grpc.py``。"""
        removed = 0
        for pattern in ("*_pb2.py", "*_pb2_grpc.py"):
            for generated in self.msg_dir.rglob(pattern):
                generated.unlink()
                removed += 1
        print(f"🧹 已清除 {removed} 個生成檔。")
        return 0

    def print_discovered(self) -> int:
        groups = self.list_groups()
        # 也涵蓋直接放在 msg/ 底下 (無 group) 的 proto
        loose = [
            p for p in self.discover_protos()
            if len(p.relative_to(self.msg_dir).parts) == 1
        ]
        if not groups and not loose:
            print(f"(在 {self._rel(self.msg_dir)} 底下找不到任何 .proto)")
            return 0
        print(f"套件: {self.package_name}  (include root: {self._rel(self.include_root)})")
        for grp in groups:
            print(f"📦 {grp}")
            for proto in self.discover_protos(grp):
                print(f"   - {proto.relative_to(self.msg_dir)}")
        for proto in loose:
            print(f"📄 {proto.relative_to(self.msg_dir)}")
        return 0


def add_compile_arguments(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """把 compile 相關的參數掛到指定 parser 上 (供獨立 CLI 與 lemegeton 子指令共用)。"""
    parser.add_argument(
        "--pkg-dir",
        metavar="PATH",
        default=None,
        help="目標套件目錄 (預設: 從當前目錄往上爬偵測的專案套件)。",
    )
    parser.add_argument(
        "--msg-dir",
        metavar="NAME",
        default="msg",
        help="存放 .proto 的子目錄名稱 (預設: msg)。",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "-p",
        "--package",
        dest="group",
        metavar="NAME",
        help="只編譯指定 group (msg 下第一層資料夾, 例如 humanoid、teleop)。",
    )
    group.add_argument(
        "--proto",
        metavar="PATH",
        help="只編譯單一 .proto 檔。",
    )
    group.add_argument(
        "--list",
        action="store_true",
        help="列出所有發現的 group 與 .proto。",
    )
    group.add_argument(
        "--clean",
        action="store_true",
        help="移除所有生成的 *_pb2.py。",
    )
    return parser


def run_compile(args: argparse.Namespace) -> int:
    """依已解析的參數執行編譯; 回傳 exit code。"""
    if args.pkg_dir:
        package_dir = Path(args.pkg_dir).resolve()
    else:
        # 預設: 從當前目錄往上爬偵測專案 (與安裝模式無關, 作用在當前專案)
        info = project.detect()
        if info is None:
            print(
                "❌ 錯誤: 找不到專案 (往上爬找不到 pyproject.toml / CMakeLists.txt)。"
                " 請在專案目錄內執行, 或用 --pkg-dir 明確指定套件目錄。",
                file=sys.stderr,
            )
            return 1
        package_dir = info.package_dir

    compiler = ProtoCompiler(package_dir, msg_subdir=args.msg_dir)

    if not compiler.msg_dir.exists():
        print(
            f"❌ 錯誤: 找不到訊息目錄 {compiler._rel(compiler.msg_dir)}。"
            f" 請確認 --pkg-dir / --msg-dir 設定正確 (或先執行 `lemegeton create`)。",
            file=sys.stderr,
        )
        return 1

    if args.list:
        return compiler.print_discovered()

    if args.clean:
        return compiler.clean()

    if args.proto:
        target = compiler.resolve_single(args.proto)
        if target is None:
            print(f"❌ 錯誤: 找不到 .proto 檔: {args.proto}", file=sys.stderr)
            return 1
        protos = [target]
    else:
        protos = compiler.discover_protos(args.group)
        if args.group and not protos:
            print(
                f"❌ 錯誤: group '{args.group}' 底下找不到 .proto。"
                f" 可用的 group: {', '.join(compiler.list_groups()) or '(無)'}",
                file=sys.stderr,
            )
            return 1

    if not protos:
        print(f"(在 {compiler._rel(compiler.msg_dir)} 底下找不到任何 .proto, 無事可做)")
        return 0

    print(
        f"🔨 編譯 {len(protos)} 個 .proto"
        f" (套件: {compiler.package_name}, include root: {compiler._rel(compiler.include_root)})"
    )
    for proto in protos:
        print(f"   • {proto.relative_to(compiler.msg_dir)}")

    rc = compiler.compile(protos)
    if rc == 0:
        print("✅ 全部編譯完成!")
    return rc


def main(argv: Optional[List[str]] = None) -> int:
    """獨立 `compile_protos` 進入點。"""
    parser = argparse.ArgumentParser(
        prog="compile_protos",
        description="編譯任意套件 <pkg>/msg/ 底下的 protobuf 訊息 (仿 ROS 機制)。",
    )
    add_compile_arguments(parser)
    return run_compile(parser.parse_args(argv))


# 向後相容: 舊的進入點名稱
def run_bash() -> None:
    raise SystemExit(main())


if __name__ == "__main__":
    raise SystemExit(main())
