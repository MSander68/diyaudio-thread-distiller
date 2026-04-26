# -*- mode: python ; coding: utf-8 -*-
"""PyInstaller spec for building DIYAudio Thread Distiller on Linux.

Build from the project root with:
    .venv/bin/pyinstaller --clean --noconfirm distiller_linux.spec
"""

from pathlib import Path

from PyInstaller.config import CONF


try:
    project_root = Path(SPECPATH).resolve()
except NameError:
    project_root = Path(__file__).resolve().parent
CONF["upx_dir"] = str(project_root)

a = Analysis(
    ["main.py"],
    pathex=[str(project_root)],
    binaries=[],
    datas=[],
    hiddenimports=[],
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
    name="diyaudio-thread-distiller",
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
