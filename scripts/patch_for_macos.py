#!/usr/bin/env python3
"""
macOS 打包前临时 patch：让 Windows 专属代码在 macOS 上不崩溃。

工作原理：
  - GitHub Actions 在全新 VM 里跑，每个步骤的修改都不会提交回仓库
  - 这个脚本只在 CI 临时副本里运行，本地开发不受影响

补丁点：
  1. 顶层 `import winreg` → `try/except`
  2. `if __name__` 块内 `import msvcrt` → `try/except + fcntl`
  3. 文件锁 → msvcrt (Windows) / fcntl (Unix) 双平台
"""
import sys
from pathlib import Path

APP_PY = Path("app.py")


def patch(content: str, old: str, new: str, name: str) -> tuple[str, bool]:
    """替换文本，返回 (新内容, 是否成功)"""
    if old in content:
        content = content.replace(old, new)
        print(f"[PATCH {name}] OK")
        return content, True
    print(f"[PATCH {name}] SKIP (pattern not found)")
    return content, False


def main() -> int:
    if not APP_PY.exists():
        print(f"ERROR: {APP_PY} not found", file=sys.stderr)
        return 1

    content = APP_PY.read_text(encoding="utf-8")
    print(f"Read {APP_PY} ({len(content)} chars)")

    # ── Patch 1: 顶层 import winreg → try/except ──
    content, _ = patch(
        content,
        old="import winreg\nimport subprocess",
        new="import subprocess\ntry:\n    import winreg\nexcept ImportError:\n    winreg = None  # macOS CI patch",
        name="1 winreg",
    )

    # ── Patch 2: __main__ 中 import msvcrt → try/except + fcntl ──
    content, _ = patch(
        content,
        old="    import msvcrt\n",
        new="    try:\n        import msvcrt\n    except ImportError:\n        import fcntl\n        msvcrt = None\n",
        name="2 msvcrt",
    )

    # ── Patch 3: 文件锁 → 跨平台（msvcrt / fcntl） ──
    old_lock = (
        "    LOCK_FILE = str(DATA_DIR / \".gateway.lock\")\n"
        "    try:\n"
        "        _lock_fd = open(LOCK_FILE, \"w\")\n"
        "        msvcrt.locking(_lock_fd.fileno(), msvcrt.LK_NBLCK, 1)\n"
        "    except (OSError, IOError):\n"
        "        import ctypes\n"
        "        ctypes.windll.user32.MessageBoxW(\n"
        "            0, \"网关客户端已在运行中，请勿重复启动。\", \"提示\", 0x30\n"
        "        )\n"
        "        sys.exit(0)"
    )
    new_lock = (
        "    LOCK_FILE = str(DATA_DIR / \".gateway.lock\")\n"
        "    try:\n"
        "        _lock_fd = open(LOCK_FILE, \"w\")\n"
        "        if msvcrt is not None:\n"
        "            msvcrt.locking(_lock_fd.fileno(), msvcrt.LK_NBLCK, 1)\n"
        "        else:\n"
        "            fcntl.flock(_lock_fd.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)\n"
        "    except (OSError, IOError, AttributeError):\n"
        "        if msvcrt is not None:\n"
        "            import ctypes\n"
        "            ctypes.windll.user32.MessageBoxW(\n"
        "                0, \"网关客户端已在运行中，请勿重复启动。\", \"提示\", 0x30\n"
        "            )\n"
        "        else:\n"
        "            print(\"网关客户端已在运行中，请勿重复启动。\", file=sys.stderr)\n"
        "        sys.exit(0)"
    )
    content, _ = patch(content, old_lock, new_lock, "3 lock")

    APP_PY.write_text(content, encoding="utf-8")
    print(f"Wrote patched {APP_PY}")
    print("=" * 50)
    print("Done. macOS patches applied (not committed back to repo).")
    return 0


if __name__ == "__main__":
    sys.exit(main())