"""專案偵測共用邏輯 (仿 ROS 的 walk-up)。

供 ``lemegeton create`` 與 ``lemegeton compile`` 共用: 從當前目錄往上爬,
找到專案標記檔並定位「要操作的 Python package 目錄」。因此不論工具是
editable 或一般安裝, 指令都作用在「當前所在的專案」, 而非工具的安裝位置。
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import NamedTuple, Optional


class ProjectInfo(NamedTuple):
    root: Path          # 專案根 (含 pyproject.toml / CMakeLists.txt 的目錄)
    name: str           # 原始 package name
    source: str         # 名稱來源: "pyproject" | "cmake"
    package: str        # Python import 用的 package 名 (dash/dot → 底線)
    package_dir: Path   # package 目錄 (src/<pkg> 或 <pkg>)


def find_project_root(start: Optional[Path] = None) -> Optional[Path]:
    """從 ``start`` 往上爬, 找到含 pyproject.toml 或 CMakeLists.txt 的目錄。"""
    base = (start or Path.cwd()).resolve()
    for cand in [base, *base.parents]:
        if (cand / "pyproject.toml").is_file() or (cand / "CMakeLists.txt").is_file():
            return cand
    return None


def _name_from_pyproject(path: Path) -> Optional[str]:
    """解析 pyproject.toml 的專案名稱 (優先用 tomllib/tomli, 退回 regex)。"""
    text = path.read_text(encoding="utf-8")

    data = None
    try:
        import tomllib  # Python 3.11+

        data = tomllib.loads(text)
    except ModuleNotFoundError:
        try:
            import tomli  # 第三方 backport (3.9 / 3.10)

            data = tomli.loads(text)
        except ModuleNotFoundError:
            data = None

    if data is not None:
        name = data.get("project", {}).get("name")
        if name:
            return name
        return data.get("tool", {}).get("poetry", {}).get("name")  # 相容 Poetry

    # 無 toml 解析器 → 簡易 regex fallback (只找 [project] 區段的 name)
    section = None
    for line in text.splitlines():
        stripped = line.strip()
        header = re.match(r"^\[([^\]]+)\]", stripped)
        if header:
            section = header.group(1).strip()
            continue
        if section == "project":
            m = re.match(r"""^name\s*=\s*["']([^"']+)["']""", stripped)
            if m:
                return m.group(1)
    return None


def _name_from_cmake(path: Path) -> Optional[str]:
    """解析 CMakeLists.txt 的 ``project(<name> ...)``。"""
    m = re.search(r"project\s*\(\s*([A-Za-z0-9_.\-]+)", path.read_text(encoding="utf-8"))
    return m.group(1) if m else None


def resolve_package_dir(root: Path, pkg: str) -> Path:
    """定位 package 目錄 (src-layout 優先)。"""
    if (root / "src" / pkg).is_dir():
        return root / "src" / pkg
    if (root / pkg).is_dir():
        return root / pkg
    if (root / "src").is_dir():
        return root / "src" / pkg   # 有 src/ 但套件目錄尚未建
    return root / pkg


def detect(start: Optional[Path] = None) -> Optional[ProjectInfo]:
    """偵測當前專案; 成功回傳 ProjectInfo, 失敗回傳 None。"""
    root = find_project_root(start)
    if root is None:
        return None

    name: Optional[str] = None
    source = ""
    pyproject = root / "pyproject.toml"
    cmake = root / "CMakeLists.txt"
    if pyproject.is_file():
        name = _name_from_pyproject(pyproject)
        if name:
            source = "pyproject"
    if not name and cmake.is_file():
        name = _name_from_cmake(cmake)
        if name:
            source = "cmake"
    if not name:
        return None

    pkg = re.sub(r"[-.]", "_", name)
    return ProjectInfo(
        root=root,
        name=name,
        source=source,
        package=pkg,
        package_dir=resolve_package_dir(root, pkg),
    )
