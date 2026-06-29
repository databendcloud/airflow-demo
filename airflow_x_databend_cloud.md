# 从本地文件到云端数仓：用 Airflow 编排 Databend Cloud 数据入仓流水线

## 前言

数据进仓这件事，听起来简单，做起来琐碎。连续的 CSV 或 NDJSON 文件，想让它出现在云端数据仓库的表里，中间往往要经过：上传到对象存储、触发导入、处理失败重试、保证不重复导入……手工跑一次没问题，但要每小时跑一次、跑一年，还要在出错时自动恢复，就需要一个像样的调度系统。

这篇文章用一个最小可跑通的例子，演示如何用 **Apache Airflow** 编排整条链路，把本地文件经 **AWS S3** 暂存后，导入 **Databend Cloud**。全程单一调度源、带依赖与重试，重跑安全。

读完你会得到：

- 对 Airflow 和 Databend Cloud 各自定位的清晰认识
- 一个能直接照着跑通的 DAG，以及配套的启动脚本
- 从启动服务、配置连接、到看见数据落表的完整操作过程

> 本文用到的全部代码——完整的启动脚本、DAG、样例数据和依赖清单——都已经放在 GitHub 上：[**databendcloud/airflow-demo**](https://github.com/databendcloud/airflow-demo)。下文会贴出关键片段并逐段讲解，想直接上手或看完整文件，clone 这个仓库即可。

---

## 两个组件

### Apache Airflow：用代码定义的工作流调度器

[Apache Airflow](https://airflow.apache.org/) 是一个用 Python 代码定义、调度和监控工作流的开源平台，由 Airbnb 开源、现为 Apache 顶级项目，是数据工程领域事实上的编排标准。

它的核心概念只有几个：

- **DAG（有向无环图）**：一个工作流就是一张 DAG，描述"有哪些任务、谁依赖谁"。因为是有向无环的，所以任务间的依赖关系永远不会成环。
- **Task（任务）**：DAG 里的一个节点，是实际干活的最小单元。
- **Operator（算子）**：任务的模板。Airflow 内置和社区提供了大量 Operator，比如执行 Python 函数的 `PythonOperator`、把本地文件传到 S3 的 `LocalFilesystemToS3Operator`。
- **Scheduler（调度器）**：按 DAG 定义的时间表（`@hourly`、cron 表达式等）触发任务，并根据依赖关系决定执行顺序。
- **Connection / Variable**：Airflow 管理凭证和配置的两种机制。Connection 存连接信息（如 AWS 凭证），Variable 存键值对配置（如桶名、连接串）。

Airflow 最大的价值在于：**工作流即代码**。依赖关系、重试策略、调度周期全部写在 Python 里，可版本化、可审查、可复用。任务失败了会按策略自动重试，整个流程的运行状态在 Web UI 里一目了然。

### Databend Cloud：云原生数据仓库

[Databend](https://www.databend.com/) 是一个用 Rust 编写的云原生数据仓库，主打**存算分离**架构——数据存在对象存储（S3 等）上，计算资源（Warehouse）按需弹性伸缩，存储和计算各自独立扩展、独立计费。**Databend Cloud** 则是其全托管的云服务版本，开箱即用，无需自己运维集群。

对这篇文章来说，Databend 有一个特性特别关键——`COPY INTO` 命令：

```sql
COPY INTO my_table
FROM 's3://my-bucket/path/to/file.ndjson'
CONNECTION = (AWS_KEY_ID = '...' AWS_SECRET_KEY = '...')
FILE_FORMAT = (TYPE = NDJSON)
```

`COPY INTO` 让 Databend **直接从对象存储拉取文件入表**，支持 CSV、NDJSON、Parquet 等多种格式。更重要的是它**自带文件去重**：同一个文件导入过一次后，再次执行不会重复入库。这个特性是我们整条链路"重跑安全"的基石。

---

## 整体架构

这个方案的链路非常清晰，只有两步：

```
本地 CSV/NDJSON
      │  Task1: LocalFilesystemToS3Operator（上传暂存）
      ▼
   AWS S3 (暂存)
      │  Task2: PythonOperator -> COPY INTO（拉取入表）
      ▼
  Databend Cloud
```

设计要点：

- **单一调度源**：整条链路由 Airflow 一家编排，不需要在 Databend 端再起一个 TASK 去轮询 S3。出了问题只需要看 Airflow 一个地方。
- **依赖明确**：Task1（上传）成功后，才会触发 Task2（导入）。上传失败就不会执行导入，不会出现"导了半截"的脏数据。

为什么要先过一道 S3，而不是让 Airflow 直接把数据写进 Databend？因为 Databend 的 `COPY INTO` 就是为对象存储设计的批量导入路径，吞吐高、去重强；S3 同时也是一份天然的原始数据暂存层，便于回溯和重导。

---

## 准备工作

### 环境依赖

整套环境跑在一个 Python 3.11 的虚拟环境（`.venv/`）里，依赖很精简：

```text
apache-airflow>=2.7.0
apache-airflow-providers-amazon>=8.0.0
databend-driver>=0.20.0
```

- `apache-airflow`：调度核心。本例锁定 `2.9.3`，并用官方 constraints 文件锁住整套依赖版本，避免依赖地狱。
- `apache-airflow-providers-amazon`：提供 `LocalFilesystemToS3Operator` 等 AWS 相关算子。
- `databend-driver`：Databend 官方 Python 驱动，DAG 里用它的 `BlockingDatabendClient` 执行 `COPY INTO`。

安装命令（用官方 constraints 锁版本）：

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install "apache-airflow==2.9.3" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"
pip install apache-airflow-providers-amazon databend-driver
```

### 准备一份样例数据

`data/sample.ndjson`，三行 NDJSON（每行一个 JSON 对象）：

```json
{"id": 1, "name": "alice", "amount": 10.5}
{"id": 2, "name": "bob", "amount": 20.0}
{"id": 3, "name": "carol", "amount": 33.3}
```

### 在 Databend Cloud 建好目标表

登录 Databend Cloud 控制台，在 Worksheet 里建一张和数据结构对应的表：

```sql
CREATE TABLE IF NOT EXISTS airflow_demo (
    id     INT,
    name   VARCHAR,
    amount DOUBLE
);
```

---

## 启动脚本解析

启动 Airflow 的脚本 `start_airflow.sh` 只有十几行，但每一行都有讲究：

```bash
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
```

几个关键点：

- `AIRFLOW_HOME` 指向项目内的 `airflow_home/`，Airflow 的元数据库、配置、日志都落在这里，不污染系统目录。
- `AIRFLOW__CORE__DAGS_FOLDER` 指向 `dags/`，告诉 Airflow 去哪里加载 DAG。Airflow 的约定是：**任何形如 `AIRFLOW__<节>__<键>` 的环境变量都会覆盖 `airflow.cfg` 里对应的配置项**，比写配置文件更省事。
- `LOAD_EXAMPLES=False` 关掉官方示例 DAG，UI 里只剩我们自己的 DAG，干净。
- **PATH 顺序**那段注释是踩坑后的总结：`airflow standalone` 会派生子进程，子进程按名字 `airflow` 从 `PATH` 里找可执行文件。如果系统里装过 Homebrew 的全局 `airflow`，就可能命中旧版本，报 `jinja2` 的导入错误。把 venv 的 `bin` 放到 `PATH` 最前面，确保始终用对版本。

`standalone` 模式会在一个进程里同时拉起 Web Server、Scheduler 和触发器，最适合本地开发和演示，省去分别启动多个组件的麻烦。

### 跑起来

```bash
./start_airflow.sh
```

启动后终端会刷出一大片日志。首次启动 Airflow 会自动创建 admin 账号，用户名密码打印在终端日志里，也写到了 `airflow_home/standalone_admin_password.txt`。

![启动 Airflow](https://p.ipic.vip/9t953y.png)

看到日志稳定下来后，浏览器打开 http://localhost:8080，用 admin 账号登录即可进入 Web UI。

---

## DAG 代码详解

完整的 DAG 在 `dags/csv_ndjson_to_databend.py`。下面拆开讲。

![DAG 代码](https://p.ipic.vip/2sl9zl.png)

### 顶部可调参数

```python
LOCAL_FILE_PATH = "/Users/hanshanjie/databend/airflow/data/sample.ndjson"  # 待导入的本地文件
S3_KEY = "ingest/sample.ndjson"   # S3 暂存路径（桶内 key）
TARGET_TABLE = "airflow_demo"     # Databend 目标表
FILE_FORMAT = "NDJSON"            # 或 CSV
```

把所有需要按场景调整的东西集中在文件顶部，换数据源、换表、换格式都只改这几行。

### 第二个任务：执行 COPY INTO

核心逻辑在 `copy_into_databend` 函数里：

```python
def copy_into_databend(**context) -> None:
    """对 Databend Cloud 执行 COPY INTO，从 S3 拉取暂存文件入表。"""
    from databend_driver import BlockingDatabendClient

    dsn = Variable.get("databend_dsn")
    bucket = Variable.get("s3_bucket")
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
```

几个细节：

- 所有凭证和配置都从 `Variable.get(...)` 读取，**不写死在代码里**。
- 根据 `FILE_FORMAT` 动态拼接 `FILE_FORMAT` 子句：CSV 需要跳过表头、指定分隔符，NDJSON 则直接按行解析。
- `PURGE = FALSE`：导入后**不删除** S3 上的源文件，保留暂存数据便于回溯。
- `ON_ERROR = ABORT`：遇到坏数据立即中止，不静默吞掉错误。
- `databend-driver` 的 import 放在函数内部，是 Airflow 的常见技巧——避免在 DAG 解析阶段就加载重依赖，减轻 Scheduler 负担。

### 组装 DAG

```python
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
```

- `schedule="@hourly"`：每小时跑一次。换成 cron 表达式（如 `"0 2 * * *"`）就能改成每天凌晨 2 点跑。
- `catchup=False`：不补跑历史时间窗口，只从当前开始调度。
- `max_active_runs=1`：同一时刻只允许一个 DAG 实例在跑，避免并发导入冲突。
- `dest_bucket="{{ var.value.s3_bucket }}"`：用 **Jinja 模板**在运行时从 Variable 读桶名，而不是写死。
- `upload_to_s3 >> copy_task`：这行 `>>` 就是 Airflow 定义依赖的语法糖，意思是"先上传，成功后再导入"。这一条线就把两个任务串成了 DAG。

---

## 在 Airflow 里配置连接和变量

DAG 跑起来前，需要在 Web UI 里把凭证和配置填好。

### Connection

`Admin -> Connections` 新建一条：

- `aws_default` —— S3 上传用的 AWS 凭证（Access Key / Secret Key）。`LocalFilesystemToS3Operator` 就靠它把本地文件传上 S3。

### Variable

`Admin -> Variables` 新建以下几条：

- `s3_bucket` —— 暂存桶名，如 `my-ingest-bucket`
- `databend_dsn` —— Databend Cloud 连接串
- `aws_access_key_id` / `aws_secret_access_key` —— 供 `COPY INTO` 在 Databend 端读 S3 用

其中 `databend_dsn` 从 Databend Cloud 控制台的 **Connect** 页面获取，格式：

```text
databend://<user>:<password>@<host>:443/<database>?sslmode=enable&warehouse=<wh>
```

配置完后，Variables 列表大致长这样：

![Airflow Variables](https://p.ipic.vip/2nnnx1.png)

---

## 跑通验证

一切就绪后，在 Airflow UI 的 DAG 列表里找到 `csv_ndjson_to_databend`：

1. 打开 DAG 右上角的开关（Unpause），让它进入可调度状态。
2. 点击右侧的 ▶ 手动触发一次（Trigger DAG），不用等到整点。
3. 进入 Grid / Graph 视图，看着 `upload_to_s3` 先变绿（成功），随后 `copy_into_databend` 被触发、也变绿。
4. 点开 `copy_into_databend` 的日志，能看到 `COPY INTO 完成，受影响行数返回：...` 的打印。

回到 Databend Cloud 的 Worksheet，查一下目标表：

```sql
SELECT * FROM airflow_demo;
```

三行数据如期落表：

![Databend Cloud 查询结果](https://p.ipic.vip/ntai5b.png)

到这里，整条链路就跑通了：本地文件 → S3 暂存 → Databend Cloud 入表，全程由 Airflow 一手编排。

---

## 生产环境建议

这个例子为了演示，把凭证直接放进了 Variable、把 AWS key 拼进了 SQL。真正上生产前，建议做这几处加固：

- **凭证别明文存 Variable**。改用 Airflow 的 [Secrets Backend](https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/secrets-backend/index.html)（如 AWS Secrets Manager、HashiCorp Vault），凭证集中托管、自动轮转。
- **别把 AWS key 拼进 `COPY INTO` 的 SQL**。在 Databend Cloud 侧预先创建 [`CONNECTION` 对象](https://docs.databend.com/sql/sql-commands/ddl/connection/)，DAG 里只引用连接名，凭证不出 Databend。
- **用唯一的 S3 key**。如果文件名固定、内容会被覆盖，要确认 `COPY INTO` 的去重行为符合预期。更稳妥的做法是每批数据用带时间戳的唯一 key（如 `ingest/2026-06-29/sample.ndjson`），既保证幂等，又留下清晰的数据血缘。
- **加重试和告警**。给 Task 配置 `retries`、`retry_delay`，再接上失败回调（邮件、Slack），让失败能被及时发现。

---

## 小结

这篇文章用一个最小例子，串起了 Airflow 和 Databend Cloud 两个工具的配合：

- **Airflow** 负责"什么时候做、按什么顺序做、失败了怎么办"——用 Python 代码把工作流定义清楚，调度和监控都交给它。
- **Databend Cloud** 负责"把数据高效地存进来、查出去"——`COPY INTO` 的批量导入和自带去重，让数据入仓既快又稳。
- 中间的 **S3** 既是导入的高速通道，也是原始数据的暂存层。

三者配合，得到一条**单一调度源、依赖明确、重跑安全**的数据进仓流水线。从这个骨架出发，换数据源、改调度周期、接更多下游处理，都只是顺势扩展的事。

详细完整的启动脚本和 DAG 代码，可参考 GitHub 仓库 [**databendcloud/airflow-demo**](https://github.com/databendcloud/airflow-demo)，clone 下来按 README 配好凭证即可跑通。

