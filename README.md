# 湖仓工坊 · 联邦查询与多维自助分析平台

面向异构数据源联邦查询、语义模型构建、OLAP多维立方物化、聚合缓存预热、时序窗口分析、查询改写优化与数据集市发布的多维自助分析平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/WorkbenchView.vue|tsx  全部模块共享工作台
│   ├── src/data/modules.ts              18个业务模块配置
│   ├── src/api/              统一请求封装
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/resource.py  通用查询/导出/登记/动作接口
│   ├── app/services/<key>.py    每个模块独立领域逻辑
│   ├── app/services/base.py     共享服务骨架
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 数据源注册 | `source_registry` | 注册数据源 | 源编号、源类型、连接地址 |
| 语义模型 | `semantic_model` | 语义定义 | 模型编号、模型名称、源表映射 |
| 联邦查询 | `query_federation` | 联邦计划 | 计划编号、参与来源、关联键 |
| 多维立方 | `olap_cube` | OLAP立方 | 立方编号、立方名称、维度列表 |
| 指标工厂 | `metric_factory` | 计算指标 | 指标编号、指标名称、数据来源 |
| 维度树 | `dimension_tree` | 维度层级 | 层级编号、维度名称、层级路径 |
| 聚合缓存 | `aggregate_cache` | 缓存配置 | 配置编号、目标立方、缓存粒度 |
| 查询改写 | `query_rewrite` | 改写规则 | 规则编号、原始模式、替换模式 |
| 时序窗口 | `time_window` | 窗口定义 | 窗口编号、窗口类型、滑动宽度 |
| 数据切片 | `dataset_slice` | 切片配置 | 切片编号、源立方、维度过筛 |
| 下钻配置 | `drill_config` | 下钻路径 | 路径编号、起始维度、目标维度 |
| 结果集市 | `result_mart` | 数据Mart | 集市编号、集市名称、数据来源 |
| 基准集 | `benchmark_set` | 分析基线 | 基线编号、基线名称、数据范围 |
| 查询审计 | `query_audit` | 审计记录 | 记录编号、查询语句、执行耗时 |
| 成本治理 | `cost_govern` | 配额策略 | 策略编号、适用角色、查询上限 |
| 新鲜度监控 | `freshness_mon` | 新鲜度规则 | 规则编号、目标数据集、SLA秒数 |
| 分析导出 | `data_exporter` | 导出任务 | 任务编号、数据来源、筛选条件 |
| 定时计算 | `schedule_job` | 预计算任务 | 任务编号、目标立方、Cron表达式 |
| 湖仓读取 | `data_lake_read` | 湖仓路径 | 路径编号、存储位置、文件格式 |
| 结果缓存 | `result_cache` | 缓存条目 | 条目编号、查询摘要、结果哈希 |
| 数据契约 | `data_contract` | 契约定义 | 契约编号、数据集、Schema约束 |
| 实验配置 | `ab_config` | AB实验 | 实验编号、实验名称、分流比例 |

## 指标取数链路（批次与可追溯结果）

指标工厂的计算不再用「立方编号子串」找缓存，而是按完整立方标识 + 完整批次取数：

1. **立方标识精确匹配**：`olap_cube:1` 与 `olap_cube:11` 是两座立方，
   统一由 `app/services/cube_ref.py` 解析完整编号，杜绝立方 1 读到立方 11 的明细。
2. **批次完整性**：`aggregate_cache` 的每条批次带 `batch_id` 与完整性标记
   （`complete` / `expected_rows` / `完整性`）。指标计算只消费完整批次；
   半批/未齐批次会被排除，半批金额不计入已确认合计。
3. **并发只算一次**：刷新期间同一组维度 + 度量（或同一指标 + 立方 + 批次）的
   并发提交由 `app/services/concurrent.py` 合并成一次实际计算，其余调用复用结果。
4. **可追溯结果**：计算成功写入 `result_cache` 时同时记录
   `batch_id`、`metric_id`、`cube_id`、`formula`、维度/度量与确认时间；
   没有完整聚合依据时返回失败并标记 `产出异常`，不会用随机金额冒充一次成功产出，
   同时清理历史上「有效但无批次」的结果。
5. **报表重算读结果缓存**：`recompute_report` 只读取结果缓存里当时确认、带批次的数值，
   兼容历史报表的指标标识（`METR-0001` / `metric_1` / 数字 id）与计算口径（公式）。
   无可用结果的行会明确给出 `reason` 与可引用的批次，而不是给出猜测值。

相关接口：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/pipelines/metric-compute` | 按完整立方标识取完整批次计算指标 |
| POST | `/api/pipelines/aggregate-batches` | 列出某立方的完整/不完整批次 |
| POST | `/api/pipelines/metric-report` | 报表重算（只读结果缓存确认值） |
| POST | `/api/pipelines/metric-report/recompute` | 按历史报表标识与口径重算 |

指标工厂列表里的「计算指标 / 确认产出」动作同样走这条链路，失败时接口返回
`ok=false` 并保留异常状态。

## 约定

- 全部模块共享`frontend/src/views/WorkbenchView.vue|tsx`和一套资源路由。
- 每个模块仍保留独立`backend/app/services/<模块>.py`；纯前端保留
  `frontend/src/domain/<模块>.ts`，用于承载真实业务差异并维持36题位容量。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

领域：数据工程
子方向：数据分析与数据应用
