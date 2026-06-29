# CSV/NDJSON → S3 → Databend Cloud (Airflow 方案 A)

用 Apache Airflow 全程编排：本地文件先传到 S3 暂存，成功后对 Databend Cloud 执行 `COPY INTO` 入表。单一调度源，带依赖和重试，`COPY INTO` 自带文件去重，重跑安全。

## 架构

```
本地 CSV/NDJSON
      │  Task1: LocalFilesystemToS3Operator
      ▼
   AWS S3 (暂存)
      │  Task2: PythonOperator -> COPY INTO
      ▼
 Databend Cloud
```

Task1 成功才触发 Task2。不需要 Databend 端再起 TASK 轮询。

## 环境（已装好）

依赖已安装在本地 venv `.venv/`：

- `apache-airflow==2.9.3`（Python 3.11，用官方 constraints 锁版本）
- `apache-airflow-providers-amazon`
- `databend-driver`

Airflow 元数据库已初始化在 `airflow_home/`，DAG 已校验可正常加载、无 import 错误。

## 启动

```bash
./start_airflow.sh
```

standalone 模式会同时拉起 web UI（默认 http://localhost:8080）和 scheduler。
首次启动会自动创建 admin 账号，用户名密码打印在终端日志，也存于
`airflow_home/standalone_admin_password.txt`。

## Airflow 配置

启动后在 Airflow UI 配置以下项。

Connection：
- `aws_default` —— S3 上传用的 AWS 凭证（Admin -> Connections）

Variable（Admin -> Variables）：
- `s3_bucket` —— 暂存桶名，如 `my-ingest-bucket`
- `databend_dsn` —— Databend Cloud 连接串
- `aws_access_key_id` / `aws_secret_access_key` —— 供 `COPY INTO` 读 S3 用

`databend_dsn` 从 Databend Cloud 控制台 Connect 页面获取，格式：

```
databend://<user>:<password>@<host>:443/<database>?sslmode=enable&warehouse=<wh>
```

## 调整点

`dags/csv_ndjson_to_databend.py` 顶部常量：
- `LOCAL_FILE_PATH` 待导入文件
- `S3_KEY` S3 暂存路径
- `TARGET_TABLE` 目标表
- `FILE_FORMAT` `CSV` 或 `NDJSON`
- DAG 的 `schedule` 默认 `@hourly`，按需改 cron

## 生产建议

- 凭证改用 Airflow Secrets Backend（如 AWS Secrets Manager），不要明文存 Variable。
- `COPY INTO` 的 S3 鉴权建议在 Databend Cloud 侧预建 `CONNECTION` 对象，DAG 里只引用名字，避免把 AWS key 传进 SQL。
- 若文件名固定且会被覆盖，确认 `COPY INTO` 去重行为符合预期；建议每批用带时间戳的唯一 key。
