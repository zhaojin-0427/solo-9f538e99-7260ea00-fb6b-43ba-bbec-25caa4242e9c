# 招标公告语料采集器（Tender Corpus Collector）

面向招标语料数据员的采集 API：提交多个官网公告 URL，后台按共享配额抓取，
提取 HTML 标题与正文，全部成功后提供完整 JSON 语料下载。

技术栈：FastAPI + httpx（异步）+ SQLite（持久化），Docker Compose 一键启动
API 与两个可模拟**双向跳转、限流（429/503 + Retry-After）、慢响应**的本地站点。

## 快速开始

```bash
docker compose up --build
```

然后打开 <http://localhost:8000/docs>（Swagger UI），即可**采集、取消、查询、下载**：

1. `POST /jobs` 提交 1–20 个公告 URL（示例见下），返回 `job_id`；
2. `GET /jobs/{job_id}` 轮询进度（每条公告的状态/跳数/重试次数/错误）；
3. `POST /jobs/{job_id}/cancel` 取消**运行中**的作业（误提交 URL 后止损）；
4. `GET /jobs/{job_id}/download` 在**全部成功**后下载完整 JSON 语料
   （有失败、未完成或已取消时返回 409，不提供半份结果）；
5. `GET /cooldowns` 查看当前生效的源冷却；`GET /jobs` 列出所有作业。

### 取消作业（POST /jobs/{job_id}/cancel）

- 仅运行中的作业可取消；已成功/失败的终态作业返回 409，结论与下载资格不变；
  对未知 `job_id` 返回 404。
- **重复取消幂等**：再次调用返回 200 与原终态（`status=cancelled`）。
- 取消登记是**单事务原子操作**（jobs 标记 + 未完成公告置 `cancelled`）；
  登记失败（如 SQLite 提交错误）时事务回滚、返回 500，作业按原样继续采集。
- 登记成功后：
  - **尚未发出的请求一律不发**——队列中待发任务、3xx 后的跳转请求、429/503/
    网络错误后的重试，在出队/重投点直接丢弃；冷却等待会被取消事件立即唤醒，
    不再等满 `Retry-After`；
  - **已在途的响应不能改写取消结论**——响应回来后一律丢弃，不写成功/失败、
    不发起后续请求；持久层对 `cancelled` 行的 UPDATE 也会被拒绝（双保险）；
  - 已采到的成功/失败条目**原样保留**（进度可查），但取消作业**不能下载语料**；
  - 取消状态持久化：**重启后已取消作业不会续采**，其他未完成作业仍按原配额与
    源冷却恢复运行；取消一个作业不影响其他作业。

两个模拟站点：宿主机上 `http://localhost:8001`（site_a）、`http://localhost:8002`
（site_b）可浏览；**提交给 API 的 URL 请使用容器网络名**
`http://site_a:8000/...`、`http://site_b:8000/...`（API 容器内解析）。

### 可试的 URL（在 /docs 的 POST /jobs 里提交）

| URL | 行为 |
|---|---|
| `http://site_a:8000/article/1` | 普通公告（提取标题+正文） |
| `http://site_a:8000/to-peer/2` | 302 跳到 site_b 的公告（跨源 1 跳） |
| `http://site_a:8000/chain/3` | 双向跳转链，恰好 3 跳成功 |
| `http://site_a:8000/chain/4` | 第 4 跳超出上限 → 失败 |
| `http://site_a:8000/flaky/1?fails=2&ra=3` | 前 2 次 429 + `Retry-After: 3`，第 3 次成功 |
| `http://site_b:8000/busy` | 始终 503 + `Retry-After: 1` → 重试 2 次后失败 |
| `http://site_a:8000/slow/1?delay=3` | 慢响应（3 秒） |
| `http://site_a:8000/redirect-loop` | 双向死循环跳转 → 跳数上限失败 |

## 需求实现对照

| 要求 | 实现 |
|---|---|
| 提交至多 20 个 URL | `POST /jobs`，pydantic 校验 1–20 条、http/https、去重 |
| 提取标题和正文 | `api/app/extract.py`：stdlib `html.parser` 提取 `<title>` 与 `<body>` 文本（剔除 script/style） |
| 全局最多 2 个在途请求 | 全局 `asyncio.Semaphore(2)`，所有作业共享一个引擎 |
| 同源最多 1 个 | 每个源（scheme://host:port）一条串行"pump"协程 |
| 跳转按目标源排队 | 不自动跟随重定向；302 目标作为新任务投入**目标源**的队列 |
| 每条公告最多 3 跳 | `hops >= 3` 再遇 3xx 即失败（`too many redirects`） |
| 429/503 + 整数 Retry-After → 同源所有作业未发请求延后 | 设置源级冷却（内存 + SQLite），该源 pump 发送前等待 |
| 等待不占全局名额 | 冷却 `sleep` 发生在获取全局信号量**之前** |
| 其他源仍能采集 | 冷却是 per-origin 的，其他源的 pump 不受影响 |
| 每个 URL 最多重试 2 次 | `attempts` 计数（429/503/网络错误），跳转后的新 URL 重新计数 |
| 可查进度 | `GET /jobs/{id}`（逐条状态）、`GET /jobs`、`GET /cooldowns` |
| 运行中作业可取消 | `POST /jobs/{id}/cancel`：原子登记；未发请求/跳转/重试不发出，在途响应丢弃，已采进度保留但不可下载 |
| 重复取消幂等 | 已取消作业再调用返回 200 与原终态；成功/失败终态作业返回 409 |
| 登记失败继续采集 | 取消事务回滚 + HTTP 500，作业状态与引擎均不改变 |
| 重启不续采已取消作业 | `jobs.cancelled` 与公告 `cancelled` 状态持久化；启动时仅恢复 pending/fetching，取消集在引擎中重建 |
| 重启保留冷却、继续未完成、成功不重取 | SQLite 持久化作业/公告/冷却；启动时恢复未完成的（pending/fetching），成功的不重取 |
| 全部成功才开放下载 | `GET /jobs/{id}/download`：非全成功（含已取消）返回 409 |
| /docs 可采集、取消、查询、下载 | Swagger UI 暴露全部端点；下载带 `Content-Disposition: attachment` |

## 目录结构

```
api/            采集 API（FastAPI）
  app/main.py   端点与生命周期（含取消端点；启动时恢复冷却与未完成任务，跳过已取消）
  app/engine.py 抓取引擎：全局/同源配额、冷却、跳转、重试、取消检查点
  app/db.py     SQLite 持久化（jobs / announcements / cooldowns，含取消列的惰性迁移）
  app/extract.py HTML 标题正文提取
sites/          模拟招标站点（同一镜像以环境变量区分 site_a/site_b）
tests/          端到端自测脚本（本地进程模拟 compose 拓扑）
docker-compose.yml
```

## 自测

```bash
python3 tests/e2e_test.py                          # 全量：提取/跳转/限流/配额/下载门控/重启持久化
python3 tests/cross_job_cooldown_test.py           # 专项：跨作业共享冷却
python3 tests/cancel_job_test.py                   # 专项：取消作业（未发请求不发/在途丢弃/幂等/重启不续采/作业隔离）
python3 tests/cancel_registration_failure_test.py  # 专项：取消登记失败继续采集 + 持久层原子性/迁移
```

## 已知限制

- "源"按 `scheme://host:port` 判定；同一服务器的不同端口/主机名视为不同源。
- 失败（重试耗尽）是终态，重启不会自动重试失败公告；需重新提交作业。
- 提取器面向常规 HTML 公告页，不执行 JavaScript。
