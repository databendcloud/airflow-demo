#!/usr/bin/env bash
# 启动 Airflow（standalone 模式，含 web UI + scheduler）。
# 首次启动会自动建 admin 账号，账号密码打印在终端日志里。
set -euo pipefail

cd "$(dirname "$0")"

export AIRFLOW_HOME="$(pwd)/airflow_home"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

# standalone 会 spawn 子进程，按名字 `airflow` 从 PATH 查找。
# 必须把 venv 的 bin 放到 PATH 最前，否则会命中 Homebrew 全局的旧 airflow，
# 触发 `ImportError: cannot import name 'escape' from 'jinja2'`。
export VIRTUAL_ENV="$(pwd)/.venv"
export PATH="$(pwd)/.venv/bin:$PATH"

exec .venv/bin/airflow standalone
