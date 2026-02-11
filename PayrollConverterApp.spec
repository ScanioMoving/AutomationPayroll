# -*- mode: python ; coding: utf-8 -*-


a = Analysis(
    ['payroll_mac_app.py'],
    pathex=[],
    binaries=[],
    datas=[('payroll_roster.json', '.'), ('payroll_workspace_ui.html', '.'), ('/Users/orlandocantoni/Downloads/AutomationPayroll/.build_assets/default_template.xlsx', '.')],
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
    [],
    exclude_binaries=True,
    name='PayrollConverterApp',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)
coll = COLLECT(
    exe,
    a.binaries,
    a.datas,
    strip=False,
    upx=True,
    upx_exclude=[],
    name='PayrollConverterApp',
)
app = BUNDLE(
    coll,
    name='PayrollConverterApp.app',
    icon=None,
    bundle_identifier=None,
)
