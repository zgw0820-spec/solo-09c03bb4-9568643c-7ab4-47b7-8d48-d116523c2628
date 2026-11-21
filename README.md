# Prometheus Remote Write 1.0 双副本选主接收器

两台 Prometheus（副本 A / B）同时采集同一目标，各自携带 `cluster` 与 `replica`
外部标签，以 **Remote Write 1.0（snappy + protobuf 普通样本）** 推送到本服务。
接收器按 `cluster` 选出唯一供数副本，剥除 `replica` 标签后逐序列存入 SQLite，
并在供数副本停报时自动接替。

## 做成的功能

### 1. Remote Write 1.0 接收
- `POST /api/v1/write`，校验 `X-Prometheus-Remote-Write-Version: 1.0` 与
  `Content-Type: ... application/x-protobuf ... encoding=snappy`。
- 手写 protobuf 子集编解码（`receiver/app/remote_write.py`），接收器自身不依赖
  protobuf 运行时；snappy 压缩用 `cramjam`。
- 普通样本（`Sample{double value, int64 timestamp_ms}`），未知字段（如 2.x 的
  metadata）直接跳过，保持前向兼容。
- 畸形/截断载荷返回 400；空请求体按 204 noop 处理。

### 2. 按 cluster 选供数副本（`receiver/app/db.py`）
- 每个 `cluster` 在 `cluster_state` 中只记录一个 `active_replica`。
- 首个带**新点**的副本成为初始供数方（SELECTED 事件）。
- 供数方未陈旧时，另一副本的批次一律拒绝（角色 `standby`，HTTP 仍为 204，
  响应头 `X-Ingest-Accepted: 0`），不写任何样本。
- **停报接替**：供数副本超过 `STALE_AFTER_SECONDS`（默认 10 秒）没有写入新点
  即判陈旧；此时另一副本必须带来**晚于该序列尾点**的新点才能接替（TAKEOVER）。
  只重发旧点不允许接替。
- 陈旧计时只由“成功写入新点”刷新（`replica_liveness.last_write_ms`），
  重发、心跳、standby 批次都不会续期。

### 3. 逐序列存储与尾点规则
- 剥除 `replica` 后对剩余标签做规范化 JSON + SHA-256 指纹，因此 A/B 写入的是
  **同一条序列**；存库标签中不含 `replica`。
- 每条序列仅接收时间戳严格晚于当前尾点的样本；`ts <= tail` 计为 stale 丢弃，
  重发不入库、不续期。同批次重复时间戳去重，主键
  `(series_id, timestamp_ms)` 兜底幂等。
- SQLite 表：`series`、`samples`（WITHOUT ROWID 复合主键）、`cluster_state`、
  `replica_liveness`、`events`。

### 4. 切换与入库原子、重启沿用
- 一次 `BEGIN IMMEDIATE` 事务内完成“角色判定 + 样本插入 + 活跃副本切换 +
  事件记录”，中途任何异常整体回滚（已用注入故障的测试验证）。
- SQLite 开启 WAL；进程被 SIGKILL 后重启，已提交的 `cluster_state` 与样本
  完整保留（已用崩溃重启脚本验证），重启后继续沿用上次成功写入的状态与
  新鲜/陈旧判定。

### 5. 按 cluster 的活跃序列上限与 30 秒占额
- 网页“集群状态”表中可为每个 cluster 直接设置 `活跃序列上限`，也可先对尚未
  有数据的 cluster 预设。**未设置时照常无限接收**；`PUT
  /api/clusters/{cluster}/limit`（`{"max_series": N}` 或 `null` 取消）与写入
  都在 `BEGIN IMMEDIATE` 事务里读取上限，因此上限调整与写入严格按提交顺序生效。
- 每条序列在**最后一次成功写入新点**后占用一个名额 `LEASE_SECONDS`（默认
  30 秒），每次成功写新点都会把占额窗口重置到当前时间。
- 已占额的序列照常写新点（即使随后把上限调到低于当前占额数，也不驱逐它们）；
  同批其他没有占额的序列只在仍有空位时写入，满额则丢弃它们的新点。被丢弃的
  点在结果里以 `samples_limit_dropped` / `X-Ingest-Limit-Dropped` 体现。
- 重发旧点不会续期；因限额丢点既不会续期，也不会让另一个副本借此接替。
- 样本、`series_leases` 占额行与 `cluster_state` 在**同一个事务**提交；写入
  失败时沿用上次成功提交的状态与占额。
- 占额行存的是“最后一次成功写入的提交时间”（`last_write_ms`），是否占额只按
  `now - last_write_ms < LEASE_SECONDS` 判定。重启后直接从该索引时间窗计算，
  **无需扫描全部序列或历史样本**；旧库样本照常可查，升级前的写入不占额
  （启动时只记一条 `LIMIT_SCHEMA_UPGRADE` 说明事件，O(1) 完成）。

### 6. Docker Compose 一键演示
- `docker compose up --build` 启动 4 个服务：
  - `receiver`（本服务，Web 与 API 在 8000 端口，SQLite 存于命名卷）
  - `prom-a` / `prom-b`（prom/prometheus:v2.54.1，`protocol_version: 1.0`，
    外部标签 `cluster=demo, replica=A|B`，5 秒抓取）
  - `demo`（FastAPI 模拟目标，持续递增 `demo_requests_total` 计数器）
- 接收器挂载 `/var/run/docker.sock`，网页按钮通过 Docker SDK 对
  `rw-demo-prom-a` / `rw-demo-prom-b` 执行 `pause` / `unpause`。

### 7. 网页控制台（`http://localhost:8000/`）
- 各 cluster 当前供数副本、供数/陈旧状态、最近新点时间。
- 每个 cluster 一行：**活跃数 / 上限**、**限额丢弃累计**、上限保存/取消按钮。
- 本地混合批次演示卡（不依赖 Docker）：选 cluster/replica、已有序列数与新序列数，
  点一次即通过正常写入路径注入“已占额序列 + 争抢空位的新序列”混合批次，
  下方直接显示 `samples_stored`、`samples_limit_dropped`、`active_series`。
- 两个演示副本的容器状态与**暂停 / 恢复**按钮。
- 序列数、样本总数、切换事件数；逐序列 SVG 计数走势图（最近 240 点）。
- 最近 30 条 SELECTED / TAKEOVER / LIMIT_SCHEMA_UPGRADE 事件，页面 2 秒自动刷新。

## 目录结构

```
.
├── docker-compose.yml          # receiver + prom-a + prom-b + demo
├── prometheus/
│   ├── prometheus-a.yml        # external_labels replica=A, remote_write 1.0
│   └── prometheus-b.yml        # external_labels replica=B
├── receiver/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py             # FastAPI：/api/v1/write 与 Web/API
│       ├── remote_write.py     # 手写 RW1.0 protobuf + snappy 编解码
│       ├── db.py               # SQLite 存储与选主/接替/活跃序列上限状态机
│       ├── dockerctl.py        # Docker pause/unpause 封装
│       ├── config.py           # 环境变量配置
│       └── static/index.html   # 演示网页
├── demo/
│   ├── Dockerfile
│   └── app.py                  # 本地模拟 metrics 目标
├── tests/                      # 52 个 pytest 用例
└── scripts/                    # 实跑冒烟、限额冒烟与崩溃重启脚本
```

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DB_PATH` | `/data/receiver.db` | SQLite 文件路径 |
| `STALE_AFTER_SECONDS` | `10` | 副本判陈旧的无新点秒数 |
| `LEASE_SECONDS` | `30` | 成功写新点后的活跃序列占额时长 |
| `REQUIRE_REMOTE_WRITE_HEADER` | `1` | 是否强制 RW1.0 协议头 |
| `DOCKER_BASE_URL` | `unix:///var/run/docker.sock` | Docker 控制入口 |
| `REPLICA_CONTAINERS` | `A=rw-demo-prom-a,B=rw-demo-prom-b` | 副本与容器名映射 |

各 cluster 的活跃序列上限不放在环境变量里，而是通过网页或
`PUT /api/clusters/{cluster}/limit` 写入数据库，重启后保留。

## HTTP 接口

| 方法/路径 | 说明 |
| --- | --- |
| `POST /api/v1/write` | Remote Write 1.0 入口；响应头回传 `X-Ingest-Role`（initial/active/takeover/standby/rejected/noop）、`X-Ingest-Accepted`、`X-Ingest-Stored`、`X-Ingest-Stale`、`X-Ingest-Limit-Dropped`、`X-Ingest-Active-Series` |
| `GET /api/status` | 各 cluster 活跃副本、陈旧标记、两副本活跃时间、上限/活跃数/限额丢弃数 |
| `GET /api/limits` | 各 cluster 的上限、当前活跃序列数、限额丢弃累计、占额时长 |
| `PUT /api/clusters/{cluster}/limit` | 设置/取消（body `{"max_series": null}`）活跃序列上限 |
| `POST /api/demo/batch` | 本地混合批次演示：按 body 的 `existing_series`/`new_series` 通过正常写入路径注入一批新点，返回逐项结果 |
| `GET /api/series` | 序列清单、样本数、尾点 |
| `GET /api/series/{id}/samples?limit=240` | 逐序列样本（走势图数据源） |
| `GET /api/events` | 选主/接替/升级事件 |
| `GET /api/replicas` / `POST /api/replicas/{A|B}/{pause|resume}` | 演示副本容器控制 |
| `GET /healthz` | 健康检查 |

## 本地测试与实跑

```bash
# 单元 + HTTP 测试（52 个）
python3 -m pytest tests/ -q

# 真实 uvicorn 进程冒烟：首写 -> standby 拒绝 -> 旧批重发 -> 停报接替
# 先启动：DB_PATH=/tmp/smoke/receiver.db uvicorn app.main:app --port 8080 \
#         --app-dir receiver
SMOKE_BASE=http://127.0.0.1:8080 python3 -I scripts/smoke_live.py

# 活跃序列上限冒烟：无限接收 -> 设上限 -> 满额丢弃 -> 过期释放 -> 取消上限
# 先启动：DB_PATH=/tmp/smoke/receiver.db LEASE_SECONDS=3 \
#         uvicorn app.main:app --port 8078 --app-dir receiver
SMOKE_BASE=http://127.0.0.1:8078 SMOKE_LEASE_SECONDS=3 \
  python3 -I scripts/limit_smoke.py

# SIGKILL 崩溃 -> 同库重启 -> 状态沿用且 standby 规则仍生效
python3 -I scripts/crash_restart_test.py
```

## 实测结果（本仓库开发环境）

- `pytest`：**52 passed**，覆盖编解码往返、特殊浮点/UTF-8、坏载荷、
  首写选主、standby 拒绝、10 秒陈旧接替、“陈旧但无新点拒绝接替”、
  重发不续期、事务注入故障原子回滚、SIGKILL 后状态持久化、多 cluster
  独立、并发写不损坏、HTTP 协议头校验，以及活跃序列上限：未设置不限、
  满额丢弃、同批“占额者照常写 + 新序列争空位”、占额 30 秒到期释放、
  成功写新点续期、重发不续期、调低上限后已占额者继续写、限额丢点不触发
  接替、限额与样本原子提交、上限/占额跨重启保留、旧库升级不扫历史样本也
  不把旧写入计入占额、并发写不超上限。
- 真实 uvicorn 进程冒烟（`scripts/smoke_live.py`）：
  - A 首批 → `role=initial, stored=2`；
  - B 随即写入 → `role=standby, accepted=0`；
  - A 原样重发 → `stored=0, stale=2`，`last_write_ms` 不变；
  - 等待 10.5 秒后 B 带新点 → `role=takeover, stored=1`，
    库存序列仅 1 条、样本值 `[1.0, 2.0, 3.0]`，事件表含 SELECTED 与 TAKEOVER。
- 限额冒烟（`scripts/limit_smoke.py`，`LEASE_SECONDS=3`，真实 uvicorn）：
  - 未设上限：4 条新序列全部 `stored=4, dropped=0`；
  - 设 `max_series=2` 后，4 条已占额序列继续 `stored=4, dropped=0, active=4`；
  - 满额时 2 条新序列 `stored=0, dropped=2`，`/api/limits` 累计丢弃 2；
  - 3 秒后占额过期，3 条新序列争 2 个空位 → `stored=2, dropped=1`；
  - 重发旧点 `stored=0, stale=1` 不续期；取消上限后恢复 `stored=3, dropped=0`。
- 重启占额（真实 uvicorn，同一 DB）：`LEASE_SECONDS=5` 写入 1 条序列后强杀，
  以 `LEASE_SECONDS=60` 重启 → `active_series=1`；再以 `LEASE_SECONDS=5` 重启
  → 已按“提交写入时间 + 5 秒”过期，`active_series=0`，期间未扫描历史样本。
- 崩溃重启（`scripts/crash_restart_test.py`，120 秒窗口）：SIGKILL 后
  WAL 文件留存，重启即恢复；活跃副本仍为 A，窗口内 B 仍判 standby，
  样本数保持 1，标签中无 `replica`。

## 未完成 / 限制项

1. **本沙箱无 Docker 守护进程且无法访问 Docker Hub 镜像仓库**，
   `docker compose up --build` 与真实 Prometheus 对发未能在本机执行；
   已通过 YAML 校验、镜像标签/SKD 可用性核对，以及用字节兼容的
   RW1.0 编码器对真实 uvicorn 进程发请求完成等价验证。首次在有 Docker 的
   环境运行时需要联网拉取 `prom/prometheus:v2.54.1` 与 `python:3.11-slim`。
2. 仅实现 Remote Write **1.0 普通样本**；exemplars、原生直方图、
   RW2.0 的 metadata/created-timestamp 不在范围内（未知字段会被跳过而非报错）。
3. 接收器假定一个请求内所有序列属于同一 `(cluster, replica)`，混合请求
   返回 400；Prometheus 按 external_labels 发送时天然满足。
4. 演示网页的暂停/恢复依赖挂载 docker.socket 的 Compose 部署；脱离该部署时
   接口返回 503，不影响写入链路。
5. SQLite 面向单机演示量级，未做分片/保留策略（retention）；如需长期运行
   应增加样本过期清理任务。
6. 活跃序列上限的“占额”按接收器本机时钟（提交时的 wall clock）计算，多实例
   共享同一 SQLite 文件时才保持一致；跨机器部署需要先统一时钟或改为中心授时。
7. 占额行在每次写入该 cluster 时按时间窗惰性清理；长期无写入的 cluster 会保留
   少量已过期行，活跃数/限额判定仍正确（按 `last_write_ms` 过滤），但不主动
   删除，避免后台任务。
8. 上限对**被限额丢弃**的序列不设惩罚重试；若新序列持续争抢，谁先写入取决于
   请求到达顺序，不保证公平。
