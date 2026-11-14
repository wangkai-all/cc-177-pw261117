# 入仓工作台 · 乱序批次关联 (Pointwise Delivery Intake Workbench)

订单、订单行、付款记录分批到达时的从零入仓处理台。先落**原始批次**，找不到
订单主记录的明细进入**等待区**，主记录到达后**自动补关联**；等待超时只进入
异常处理（数据不删除）；订单撤销后对已发布明细生成**有来源的撤销结果**。
全部数据为本地合成，不连接任何真实交易系统。

纯 Python 标准库 + SQLite + 原生 HTML/JS，**零第三方依赖、零构建步骤**。

## 快速开始

```bash
# 1. 启动网页工作台
python3 -I run.py                       # http://127.0.0.1:8000

# 2. 命令行端到端演示 (7 个场景, 含断言与血缘样例)
python3 -I scripts/demo.py

# 3. 单元测试 (20 个)
python3 -I -m unittest discover -s tests -t .

# 4. 重新生成合成批次 JSON
PYTHONPATH=. python3 -I -m intake_workbench.synthdata
```

`-I` 为隔离模式（忽略环境变量与当前目录隐式导入），`run.py` 已自行注入项目路径。

## 网页工作台能做什么

- **乱序发送批次**：一键按手工排列的乱序发送全部 15 个合成批次
- **查看等待原因**：等待区/异常面板展示原因码（主记录未到、撤销先到、
  已撤销、超时），每条可打开血缘抽屉
- **补送主记录**：等待行上一键补送订单主记录，等待/异常数据自动补关联
- **超时扫描（可快进）**：输入秒数模拟时钟快进，把到期等待转入异常处理
- **手工发送批次**：粘贴 JSON 批次，自行构造重复投递、同版本重投等情况
- **发布**：单事务原子发布，订单与其明细保持一致
- **发布查询**：发布流水、订单/行/付款快照、带"来源发布号"的撤销结果
- **血缘抽屉**：每条最终记录按时间线列出经过的批次与全部状态变化

## 领域规则与状态机

### 幂等边界（同一业务版本不能重复计入）

- **批次号相同** → 整批标记 `DUPLICATE` 忽略，原始载荷仍留痕（批次表 + 血缘）
- **`(实体类型, 实体 ID, 版本)` 已登记** → 跳过该条目，写入
  `VERSION_SKIPPED_DUPLICATE` 血缘，不产生第二条业务数据
- 撤销指令同版本重投同样只生效一次；重复撤销留事件痕迹但不重复生成撤销结果

### 等待区与补关联

| 情况 | 行为 |
|---|---|
| 订单行/付款到达，订单不存在 | 明细 `WAITING`，等待区原因 `ORDER_NOT_ARRIVED` |
| 订单主记录到达 | 同事务内排空该订单所有等待明细 → `LINKED` |
| 等待超过阈值（默认 300s） | 明细 `EXCEPTION`、等待行 `EXCEPTION`，**数据保留** |
| 超时后主记录补达 | 异常恢复 → `LINKED`，血缘保留"超时→恢复"全过程 |
| 撤销指令先于订单到达 | 等待区落 `CANCEL_MARKER`；即使超时转异常，订单创建时仍被承认 |
| 订单撤销后才到的明细 | 直接 `CANCELLED`，原因 `ORDER_CANCELLED_BEFORE_DETAIL` |

超时不是"数据不存在"：`EXCEPTION` 行与 `WAITING` 行一样会被主记录补达排空。

### 撤销与有来源的撤销结果

- 订单撤销：`orders.state = CANCELLED`，记录撤销批次与原因
- 已关联但**未发布**的明细 → `CANCELLED`，不产生撤销结果（从未对外）
- **已发布**的订单行/付款 → 每条生成一条 `reversals`：
  - `source_publish_id` 指向被回滚的那次发布（来源可解释）
  - `batch_id` 指向触发撤销的批次
  - 初始 `PENDING`，随下一次发布变为 `PUBLISHED`，记录自身发布号

### 发布一致性与可续跑

- 每次发布在**一个 SQLite 事务**内：订单快照 + 明细快照 + 撤销结果 + 发布流水
  同时成功或同时回滚，中断后重跑不会出现半发布
- 订单与明细只作为一个组发布；等待中的明细不可能在没有主记录时发布
- 订单主记录仅首次发布时快照一次；后续新明细增量发布，业务版本不重复计数
- 空发布是幂等 no-op

## 数据模型（SQLite）

| 表 | 作用 |
|---|---|
| `batches` | 原始批次信封与完整 JSON 载荷（含重复批次留痕） |
| `biz_versions` | 业务版本幂等表，主键 `(entity_type, entity_id, version)` |
| `orders` / `order_events` | 订单当前态 + 全部 CREATE/CANCEL 事件 |
| `order_lines` / `payments` | 明细当前态与来源批次 |
| `waiting` | 等待区：明细等待 + 先到撤销标记 |
| `reversals` | 有来源撤销结果（PENDING/PUBLISHED） |
| `publish_journal` / `*_snapshots` | 发布流水与不可变快照 |
| `lineage` | 只增事件流，每条状态变化都记录触发批次 |

## 合成验证场景（`intake_workbench/synthdata.py`）

| 场景 | 验证点 |
|---|---|
| A 乱序补关联 | 行+付款先等，主记录补达自动关联 |
| B 超时异常与恢复 | 超时不删数据，补达后异常恢复 |
| C 发布后撤销 | 3 条已发布明细各带来源撤销结果并发布 |
| D 重复投递幂等 | 同批次号整批忽略 + 同业务版本不重计 |
| E 先撤销后创建 | 撤销标记超时后仍生效，后到明细作废 |
| F 增量发布不重复 | 多批次明细分批发布，空发布 no-op |
| G 同批混合实体 | 同批次订单+明细直接关联 |

## 目录结构

```
intake_workbench/
  store.py       SQLite schema 与事务
  engine.py      入仓/补关联/超时/撤销/发布/血缘 核心引擎
  synthdata.py   本地合成批次与场景计划
  server.py      标准库 HTTP 服务 + REST API
web/             原生 HTML/CSS/JS 单页工作台
scripts/demo.py  可复现端到端演示（隔离库断言 + 全量回放）
tests/           20 个 unittest
run.py           服务入口
```

## 主要 HTTP API

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/ingest` | 发送批次 `{batch_id, items[]}` |
| POST | `/api/synth/send-shuffled` | 乱序发送全部合成批次 |
| POST | `/api/synth/master` | 补送订单主记录 |
| POST | `/api/sweep` | 超时扫描，支持 `fast_forward_seconds` |
| POST | `/api/publish` | 事务发布 |
| POST | `/api/reset` | 清空本地演示库 |
| GET | `/api/waiting`、`/api/orders`、`/api/lines`、`/api/payments` | 当前态 |
| GET | `/api/reversals`、`/api/published` | 撤销与发布查询 |
| GET | `/api/lineage?entity_type=&entity_id=` | 单条记录血缘 |
| GET | `/api/batches`、`/api/overview`、`/api/synth`、`/api/labels` | 批次/统计/合成数据/中文标签 |
