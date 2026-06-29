"""
将本地 CSV / NDJSON 文件经 Airflow 导入 Databend Cloud。

流程（Airflow 全程编排）：
  1. upload_to_s3   —— 把本地文件上传到 S3 暂存路径
  2. copy_into_databend —— 对 Databend Cloud 执行 COPY INTO，从 S3 拉取并入表

依赖关系：第 1 步成功后才触发第 2 步。COPY INTO 自带文件去重，
重跑同一文件不会重复导入，因此整个 DAG 是幂等的。

需要在 Airflow 中预先配置：
  - Connection: aws_default        （S3 上传用的 AWS 凭证）
  - Variable:   databend_dsn       （Databend Cloud 连接串，见下方说明）
  - Variable:   s3_bucket          （暂存桶名）

databend_dsn 格式（从 Databend Cloud 控制台 -> Connect 获取）：
  databend://<user>:<password>@<host>:443/<database>?sslmode=enable&warehouse=<wh>
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)

# ---- 可按需调整的参数 -----------------------------------------------------
LOCAL_FILE_PATH = "/Users/hanshanjie/databend/airflow/data/sample.ndjson"   # 待导入的本地文件
S3_KEY = "ingest/sample.ndjson"                       # S3 暂存路径（桶内 key）
TARGET_TABLE = "airflow_demo"                         # Databend 目标表
FILE_FORMAT = "NDJSON"                                # 或 CSV
# --------------------------------------------------------------------------


def copy_into_databend(**context) -> None:
    """对 Databend Cloud 执行 COPY INTO，从 S3 拉取暂存文件入表。"""
    from databend_driver import BlockingDatabendClient

    dsn = Variable.get("databend_dsn")
    bucket = Variable.get("s3_bucket")

    # 走 presign / connection 方式让 Databend 直接读 S3。
    # AUTH 通过 Databend Cloud 端配置好的 CONNECTION 或临时凭证。
    # 这里用 AWS access key 显式传入；生产建议改用 Databend CONNECTION 对象。
    aws_key = Variable.get("aws_access_key_id")
    aws_secret = Variable.get("aws_secret_access_key")

    if FILE_FORMAT.upper() == "CSV":
        file_format_clause = (
            "FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_DELIMITER = ',')"
        )
    else:
        file_format_clause = "FILE_FORMAT = (TYPE = NDJSON)"

    copy_sql = f"""
        COPY INTO {TARGET_TABLE}
        FROM 's3://{bucket}/{S3_KEY}'
        CONNECTION = (
            AWS_KEY_ID = '{aws_key}'
            AWS_SECRET_KEY = '{aws_secret}'
        )
        {file_format_clause}
        PURGE = FALSE
        ON_ERROR = ABORT
    """

    client = BlockingDatabendClient(dsn)
    conn = client.get_conn()
    rows = conn.exec(copy_sql)
    print(f"COPY INTO 完成，受影响行数返回：{rows}")


with DAG(
    dag_id="csv_ndjson_to_databend",
    schedule="@hourly",                       # 定时批量；按需改 cron
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["databend", "s3", "ingest"],
) as dag:

    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename=LOCAL_FILE_PATH,
        dest_key=S3_KEY,
        dest_bucket="{{ var.value.s3_bucket }}",
        aws_conn_id="aws_default",
        replace=True,
    )

    copy_task = PythonOperator(
        task_id="copy_into_databend",
        python_callable=copy_into_databend,
    )

    upload_to_s3 >> copy_task
