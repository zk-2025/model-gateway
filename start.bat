@echo off
chcp 65001 >nul
cd /d "%~dp0"

echo ============================================
echo   无限额度监控网关 - 启动脚本
echo ============================================
echo.

:: 检查虚拟环境是否存在
if not exist "venv\Scripts\python.exe" (
    echo [初始化] 未检测到虚拟环境，正在创建...
    python -m venv venv
    if errorlevel 1 (
        echo [错误] 创建虚拟环境失败，请确保已安装 Python。
        pause
        exit /b 1
    )
    echo [初始化] 安装依赖到虚拟环境...
    call venv\Scripts\pip install -r requirements.txt -q
    echo.
)

echo [启动] 使用虚拟环境运行项目...
echo.
venv\Scripts\python app.py

