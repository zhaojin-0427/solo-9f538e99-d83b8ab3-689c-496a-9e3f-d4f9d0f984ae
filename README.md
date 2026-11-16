# 土壤湿度分区内容寻址存储 API

遥感建模数据工程师的「土壤湿度旧分区回收 + 训练作业固定输入」服务。
按日期分区上传 CSV（站点、湿度），整份替换当前分区；相同字节按 SHA-256
共用数据卷文件；作业可固定某一已提交状态的全部分区取得**快照号**按号读取，
释放后拒绝新读取；回收只删除既不被当前分区也不被未释放快照引用的文件。

技术栈：**FastAPI + SQLite（WAL）+ 内容寻址文件卷**，单容器、单进程，
`docker compose` 一键启动。

---

## 1. 快速开始

```bash
docker compose up --build -d
curl http://localhost:8000/health
# 交互式 API 文档： http://localhost:8000/docs
```

数据持久化在命名卷 `moisture-data`（容器内 `/data`），含 SQLite 数据库、
`blobs/` 数据卷与 `tmp/` 临时目录。

本地无 Docker 时也可直接运行：

```bash
pip install -r requirements.txt
DATA_DIR=./data uvicorn app.main:app --host 0.0.0.0 --port 8000
```

---

## 2. CSV 约定

- UTF-8 编码（允许 BOM）；
- 表头必须含「站点」和「湿度」两列，兼容英文名
  `station/site` 与 `moisture/soil_moisture`（大小写、空格不敏感）；
- 湿度必须是有限数值，站点不能为空；容忍空行；
- 上传按字节计算 SHA-256，**相同字节（哪怕来自不同分区/日期）只存一份文件**。

---

## 3. API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/partitions/{partition_id}` | 上传 CSV，整份替换该日期分区（multipart 字段 `file`） |
| `GET`  | `/partitions` | 列出所有分区当前版本元数据 |
| `GET`  | `/partitions/{partition_id}` | 查看单个分区当前版本 |
| `GET`  | `/partitions/{partition_id}/download[?snapshot_id=N]` | 下载 CSV；带快照号读固定版本 |
| `POST` | `/snapshots` | 固定当前全部分区，返回快照号 |
| `GET`  | `/snapshots` | 列出快照及释放状态 |
| `POST` | `/snapshots/{id}/release` | 释放快照，之后拒绝新读取（409） |
| `POST` | `/gc` | 立即执行一轮回收 |
| `GET`  | `/gc/status` | 回收队列长度、活动下载数、数据卷文件数 |
| `GET`  | `/health` | 健康检查 |

### 典型演示

```bash
# 上传两个日期分区
curl -X POST localhost:8000/partitions/2026-11-14 -F "file=@a.csv"
curl -X POST localhost:8000/partitions/2026-11-15 -F "file=@b.csv"

# 训练作业固定当前全部分区，拿到快照号
SID=$(curl -s -X POST localhost:8000/snapshots | python3 -c 'import sys,json;print(json.load(sys.stdin)["snapshot_id"])')

# 作业按快照号读取，期间分区可被替换，但快照内容不变
curl "localhost:8000/partitions/2026-11-14/download?snapshot_id=$SID" -o job-input.csv

# 作业结束释放，之后拒绝新读取
curl -X POST localhost:8000/snapshots/$SID/release
curl -o /dev/null -w "%{http_code}\n" "localhost:8000/partitions/2026-11-14/download?snapshot_id=$SID"  # 409

curl -X POST localhost:8000/gc          # 触发回收
curl localhost:8000/gc/status
```

---

## 4. 数据模型与存储布局

```
/data
├── moisture.db            # SQLite（WAL 模式，外键开启）
├── blobs/<aa>/<bb>/<sha>  # 内容寻址文件，路径由 SHA-256 分片得到
└── tmp/upload-*           # 上传临时文件，提交后原子发布，重启即清理
```

核心表：

- `blobs`：每个 SHA-256 一条登记（大小、创建时间）；
- `partitions` / `partition_entries`：分区当前版本及其解析后的行；
- `snapshots` / `snapshot_entries`：快照固定的（分区 → blob）映射，
  `released_at` 非空表示已释放；
- `download_leases`：进行中的下载租约（blob 级），带过期时间；
- `gc_queue`：待回收 blob 队列，记录尝试次数与最近错误，崩溃后可继续。

一个 blob 只要被以下任一位置引用就**不可回收**：
1. 某分区当前版本（`partitions.current_blob` / `partition_entries`）；
2. 任一未释放快照的条目（`snapshot_entries` 且快照 `released_at IS NULL`）。

---

## 5. 关键正确性保证

### 5.1 写盘或登记失败，不改原分区
上传先把字节完整流式落到 `tmp/` 并算出 SHA-256、解析校验 CSV，
这些步骤都在事务外、不触碰现有登记；随后在一个 `BEGIN IMMEDIATE` 事务里
登记 blob、替换分区。任一步失败整体回滚，原分区保持不变，临时文件被清理。
数据卷文件通过同目录 `os.replace` **原子发布**，读者任何时刻要么看不到、
要么看到完整文件。

### 5.2 更新分区不改变已固定快照
快照保存的是不可变 blob 的 SHA-256 引用而非数据副本。分区替换只改当前
版本指针，快照条目纹丝不动，按快照号永远读到固定时的字节。

### 5.3 回收交错安全（上传 / 释放 / 下载 / 回收）
每个候选 blob 在真正 `unlink` 前，于同一把进程内 `metadata_lock` 的**短
临界区**内重新校验「引用 + 下载租约」：

- 仍被当前分区/未释放快照引用 → 出队保留；
- 仍有未过期下载租约 → 留在队列等下一轮，**不阻塞其他文件回收**；
- 两者皆空 → 同临界区内删除文件并移除登记。

临界区只覆盖元数据判断与 `unlink`，**不包含网络流式 I/O**，因此慢下载
不会阻塞上传、释放或其他文件的回收。

### 5.4 已开始的下载完整结束
下载在打开文件前取得 blob 级租约并登记入库；回收临界区会跳过带租约的
blob。流式响应用 **async 生成器**实现（阻塞读放进 anyio 工作线程），
生成器被响应迭代器与服务集合双重强引用，`finally` 在响应结束（含客户端
断开）才释放租约。Linux 下即使文件被取消链接，已打开的句柄仍可读完。

> 说明：租约保护的是「下载任务的生命周期」。对很小的文件，服务端可能在
> 客户端还没读完 OS 套接字缓冲时就发完并结束任务；真实大数据卷文件在
> TCP 反压下服务端任务会随客户端节奏挂起，租约贯穿整个读取过程。
> `tests/test_concurrency.py` 用约 9 MiB 文件 + 慢客户端验证该反压场景。

### 5.5 崩溃 / 中断恢复
- 启动时清理 `tmp/` 残留临时文件；
- 数据卷对账：磁盘上无登记的文件自动入队；
- 处理 `gc_queue`：删除失败的文件保留在队列、记录错误，重启继续，
  单文件失败不影响其他文件；
- 进程重启清空遗留下载租约（进程已死，无活动下载）；运行期另有 TTL
  清扫兜底，防止死租约永久阻塞回收；
- 当前分区与未释放快照的引用由 SQLite 外键 + 事务保证，恢复后始终完整可读。

### 5.6 同内容重新上传仍可用
blob 被回收后若再次上传相同字节，会重新走「临时文件 → 原子发布」流程，
得到同一路径并正常下载；上传时若发现该 SHA 已在回收队列，会自动撤销回收。

---

## 6. 测试

```bash
pip install -r requirements.txt pytest httpx
python -m pytest tests/ -q
```

- `tests/test_api.py`：上传/替换、SHA-256 去重、快照固定/释放、回收规则、
  坏 CSV 不破坏原分区、同内容重新上传；
- `tests/test_concurrency.py`：**真实 uvicorn + httpx** 端到端，覆盖慢下载
  反压期间上传/释放/其他文件回收互不阻塞、同字节并发上传去重、并发读写
  快照始终可读、释放后 409；
- `tests/test_recovery.py`：写盘失败不改原分区、重启继续回收且引用完好、
  磁盘孤儿文件对账、WAL/外键开关验证。

---

## 7. 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `DATA_DIR` | `/data` | 数据库与数据卷根目录 |
| `LEASE_TTL_SECONDS` | `3600` | 下载租约兜底 TTL |
| `GC_MAX_PASSES` | `3` | 单次回收对「下载中」文件的重试轮数 |
| `MAX_UPLOAD_BYTES` | `67108864` | 单文件上传字节上限（64 MiB） |

## 8. 部署约束与取舍

- Compose 固定 **单 worker**：下载租约的进程内强引用/计数依赖单进程；
  SQLite 单库 + WAL 也适配单进程多协程并发。临界区均为短操作，流式 I/O
  不持锁，吞吐瓶颈在文件与网络而非锁。
- 若要水平扩展为多进程/多主机，需把进程内锁与租约计数下沉到数据库
  （`SELECT ... BEGIN IMMEDIATE` 串行化 + 租约表）或引入分布式锁，
  当前数据模型已为此预留 `download_leases` 表与 TTL 字段。
