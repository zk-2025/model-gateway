# -*- mode: python ; coding: utf-8 -*-
import re

# 从 app.py 读取版本号，自动生成带版本号的 exe 名（如 v1.5.0-网关客户端）
with open('app.py', encoding='utf-8') as _f:
    _ver = re.search(r'APP_VERSION\s*=\s*"([^"]+)"', _f.read()).group(1)
EXE_NAME = f'v{_ver}-网关客户端'


a = Analysis(
    ['app.py'],
    pathex=[],
    binaries=[],
    datas=[('templates', 'templates'), ('models_meta.json', '.')],
    hiddenimports=['pystray._win32', 'PIL', 'PIL._tkinter_finder'],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name=EXE_NAME,
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
