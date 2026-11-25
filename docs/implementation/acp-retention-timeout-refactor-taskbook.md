# WebCodex ACP 生命周期、轻量归档与超时优化任务书

> 建立：2026-11-25。状态：第一批架构设计静态完成；第二至七批待实施。本文件为个人二创版后续会话接续及七批任务验收入口，不是运行验收报告。

## 1. 范围、事实来源及目标

- 工作区：Windows，`E:\myself\webcodex`。以实际个人源码及当前注册 Project 为准；官方参考：<https://github.com/yyjeqhc/webcodex> 的 main，实施前重新记录所用上游提交或 blob 身份。不得将上游源码直接替代本地证据。
- 用户已确认：保留有价值的轻量归档、安全恢复与防重复派发；建立自动清理；消除历史数据对常规启动的重复扫描成本；官方核心优先、二创扩展隔离、便于后续融合。
- 目标：快速且可恢复的 ACP 准入；请求与 Run 生命周期分离；可定位每个阶段耗时与失败原因；长期维护工作有界；不牺牲权限、身份、持久化或回放安全。
- 非目标：不新增独立 SQLite 或第二套 Agent 执行器；不默认池化 Cursor 进程；不重构 Workflow Session 存储或调整已确认的 D1/D2 Session 历史保留策略；不以延长所有超时、sleep、盲目重试、取消安全检查作为修复。
- 当前用户已授权七批源码开发、静态复查、任务书更新及各批独立本地 commit；未授权实际数据迁移/删除、测试、benchmark、构建、部署、远程 Git 同步或发布。

## 2. 已核实基线与待证实问题

2026-11-25 运行诊断：Server/Runner 0.5.1、WebSocket 在线，普通 Job 0，Cursor Provider 注册；运行构建 commit c81376cadfb9cb884517adcb2f60c090de10f941，dirty=true（无法证明与当前源码逐字节一致）。ACP live diagnose 完成 initialize/session/new，Composer 2.5、Fast=false。单次无工具、无项目写入的 prompt 返回 ACP_OK，Run completed/completed；start 调用 12.502 秒，端到端约 45 秒。详细 trace 关闭，历史间歇性超时未复现，未获得阶段耗时，不把磁盘扫描或 Cursor 网络认定为唯一根因。

| 项目 | 已读官方 main | 已读个人版 |
| --- | --- | --- |
| ACP setup / Server start / control / observe 上限 | 30 / 32 / 65 / 60 秒 | 相同 |
| MCP Host policy | 已有统一 Host policy | 运行时 direct 60 秒总预算、55 秒同步/continuation 等待 |
| 普通 ACP 记录保留 | 15 分钟 | 24 小时 |
| 常规准入 | 精确 Run 查询及清理；无二创容量全扫 | 准入锁内 cleanup、容量全扫、持久化；Server 归档确认 |
| 受保护 Run 归档 | 已读对应文件未见二创归档协议 | 热库存/归档、Server 持久回执、精确归档查询 |

上游已读 blob（不是完整 main commit）：Server coding_agent.rs 85ac0d79b945f6f27b8df12fd0dc618bbd677f75；Runner coding_agent.rs 101c510c8d19426fff9a95ae2f45312809e33b35；store.rs f3f9f932c9e6bef5bec443df0c922d1146893419；lifecycle.rs 8bb84ae3c619773885940adaa4c73eaf956a0cf3；core coding_agent.rs 1ab02ad1f5464deb5f7b49720d5084326f7dbbd8。重新融合时核实最新 main，不假设这些 blob 仍为最新。

本地源码锚点（行号为诊断时快照，实施前按符号重新定位）：

- `src/tool_runtime/coding_agent.rs`：dispatch_prepared_coding_agent_start、archive_acknowledgements、reconcile_run、observe_archived_run、coding_agent_observe、start_waiter_lost_typed。
- `crates/webcodex-runner/src/webcodex_runner/coding_agent.rs`：start 准入、admission 锁、cleanup_expired、enforce_durable_capacity_for_admission、durable BeforePromptBarrier、worker 启动。
- Runner `coding_agent/lifecycle.rs`：cleanup_expired、enforce_durable_capacity、归档/终态转换；`store.rs`：list_records_for_retention、精确查询、原子持久化。
- Runner `coding_agent/setup.rs`：initialize_session；`protocol.rs`：bounded_setup_wait、wait_response；`run.rs`：run_deadline、PromptDispatchMayHaveOccurred、事件读取及 stderr drain。
- `src/mcp_host.rs`、`src/mcp/request_policy.rs`：Host budget、adapter request policy；`crates/webcodex-core/src/coding_agent.rs`：协议上限与状态模型。
- 继续追踪 MCP/REST/CLI/Desktop、Runner registry/transport、归档回执及能力协商、Workflow Session recorder、Windows 文件安全和关闭恢复路径。当前证据仅支持风险与结构差异；必须确认预算是否已在部分 adapter 裁剪。

## 3. 架构硬约束

1. Runner 拥有执行状态和持久化权威，Server 管理请求、身份路由、观察及必要回执，MCP/CLI/Desktop 仅做各自 adapter 投影；不复制权威状态，不通过 UI 判断执行事实。
2. 保留官方 ACP provider/protocol、process tree、权限、独立 Run、持久化派发屏障、幂等身份、增量 observation token、取消和重启恢复。Prompt 发送前必须完成耐久的不确定性屏障；终态持久化成功才能对外宣布相应确定性。
3. 将二创归档/保留策略、索引和维护职责集中于现有模块边界内的少量扩展接口；拟议 retention 模块仅在有真实职责时创建，不机械拆出 policy/index/maintenance 四层，不复制官方 Manager。
4. 常规启动不枚举历史目录、不逐条核对归档、不批量持久化历史回执；精确 Run ID 查询和增量容量账本作为常规路径，目标平均 O(1) 索引操作（非整个启动耗时保证）。恢复时建立必要索引；归档按需读取；独立、有界维护，容量压力只触发有限协调。
5. 活跃执行容量、热缓存容量和持久历史容量分离。计数/索引在同一权威写入契约内更新；不能通过缓存漏项、维护并发或状态写入失败突破额度/重新派发。保留不确定 Run，不让无关历史无限阻塞正常准入；存储不可用时有界拒绝并给出明确恢复路径。
6. 不弱化现有 archive marker、身份指纹、目录/文件 no-follow、Windows reparse、大小限制、冲突拒绝、跨 Runner 归属 fencing、原子发布、配套备份恢复。未证实的外部修改或磁盘异常 fail closed。
7. 新增字段尽量可选且复用现有 typed contract；核对已发布兼容基线及具名消费者，不以旧内部实现为永久兼容要求。上游实现等价或更合理时替换二创扩展，而非双轨并存。

## 4. 归档最小数据与清理决策

现有归档主要保护 lost/outcome_unknown 的恢复和防重，并非模型记忆库；现有 durable record 已排除完整 instruction，不假设现有归档保存完整日志或推理内容。先核对实际字段、调用方和回执匹配所需事实，再删冗余。必要字段包括 Run ID、项目/权限/intent/Runner/provider 归属及必要代际、派发阶段、执行确定性、终态/时间、必要错误码和回放契约；不新增完整 Prompt、源码、推理、凭据或日志。

| 决策 | 2026-11-25 用户明确确认 |
| --- | --- |
| D1 普通终态保留 | 成功及确定取消热记录 15 分钟，失败 24 小时；普通防重证据维持现有 24 小时 |
| D2 异常归档 | 保留现有最小记录，不按时间二次精简 |
| D3 最小防重清理 | 新增版本化幂等键有效期与过期拒绝协议；保留尚未核实结果的必要恢复证据，不以年龄代替核实 |
| D4 维护预算 | 每 15 分钟独立维护，每轮最多 64 条、2 秒协作预算；容量压力协调有界 |
| D5 旧格式与归档迁移 | 兼容读取旧格式，确有必要时实施原子、幂等迁移机制；真实用户数据迁移、清除仍需独立授权 |

Workflow Session 持久历史不适用此处 ACP 保留策略。

### 4.1 第一批确定的实施契约

- 冷热区分是存储/缓存生命周期而非改变 Run 事实。已成功/确定取消的热驻留以 terminal 时间为基准 15 分钟；确定失败 24 小时；普通防重耐久证据不少于终态后 24 小时，不因热容量不足提前删除。冷存储沿用现有最小 DurableRunRecord/精确分片路径；保留事件观察/终态投影所需字段，不新增第二套历史。OutcomeUnknown/Lost/持久化不确定不参与普通终态回收。归档前持久路由回执必须精确匹配身份，Server/Runner 配套恢复；归档缺失与错误区分。
- D3 新版普通 Start 键采用显式版本、不可变签发时间、唯一随机部分（建议 `wc-acp-v2:<unix-seconds>:<uuid>`）；普通有效期以签发后 24 小时为技术窗口，终态防重保留期限独立计算，不提前于 D1。拒绝 malformed、未来签发及已过期的新派发；过期请求只能观察/核对原 Run，不能以新 key 自动重试。保留原 principal+完整 key 的确定性 Run ID 算法，避免重新解释既有键时改变 Run 身份。旧 v1 记录无可验证时间，不能推断过期或批量清除；AgentTask `agent-task-coding-run:v1:<attempt_id>` 的精确 binding/claim 是具名内部消费者，保持原 Run ID 与 Attempt fence，不套用普通调用有效期。
- 新 Server/Runner 通过明确 capability 与 typed start 契约协商版本、有效期；旧 Runner 不得接收无法执行过期拒绝的新版启动，旧记录可精确观察。过期校验覆盖服务端准备/实际派发及 Runner 准入，不允许因等待跨过有效期后再启动。退回无 expiry 支持的程序不能无条件复用已清理状态；只有在持久化防重/过期水位（防止重启和时钟回拨复活已删键）及双方兼容/降级边界得到证实后，才允许实际退休相应新版证据。旧协议无法识别安全标记的降级须保留兼容防重证据或禁止使用当前状态，明确备份/回滚条件。若不能证明安全，暂保留记录而不是按时间删。禁止以客户端自报时间绕过身份绑定。
- Server 路由索引与 Runner 权威库存分别由各自拥有者维护，普通 Start 无批量归档 RPC/回执 I/O；通过独立维护或有界压力协调传输必要回执，不额外启动第二个执行器。保留 capability 协商、精确 owner/provider fence、归档 marker 与原子耐久屏障。冷记录查询按 Run ID，不载入全量历史；活跃计数、热记录计数、耐久写入失败的预约在同一准入契约内协调，重启仅恢复必要热索引，历史目录通过有界游标维护。
- 请求预算复用 McpHostRuntimePolicy（请求选择不扩大部署上限），以单次单调 deadline 的剩余量裁剪 admission/setup/Server transport/observe 等请求等待，不将 Host deadline 当作 Run deadline；外层等待丢失返回原 Run 的确定性及观察入口，不自动取消或重派。保留官方执行/取消机制及总 Run timeout。独立 REST/CLI 消费者继续走 canonical ToolRuntime，不能假设每个 adapter 已经传递预算。
- 诊断契约：在统一 Run ID 下记录 admission lock wait/hold、exact lookup 与 scan/receipt 数量、durable claim/barrier I/O、spawn、initialize、session/new、prompt、first event、terminal、response/observe 等阶段耗时及所属层错误分类；计数/耗时使用有界结构和单调时钟，不逐事件写盘，不持久化 Prompt/原始 stderr/凭据。运行测量另行授权。

## 5. 七批任务、验收与接续

| 批次 | 工作及交付 | 验收与状态 |
| --- | --- | --- |
| 1 官方兼容性与架构边界 | 核对最新双方源码、当前实现和消费者；列出归档/回执/重放/恢复不变量、启动链路和预算传递；定义分阶段观测指标、拟议模块边界及 D1-D5 决策，建立可比较基线 | 记录双方版本、证据与差异、已确认 D1-D5；架构契约已写入 4.1，静态完成（设计交付，无源码改动） |
| 2 轻量化归档 | 在已确认契约内精简字段与存储职责，精确查询、冷热隔离，兼容旧格式和配套回执；清理重复实现 | 现有 Run 不误重派；归档查询与身份/权限/错误确定性一致；迁移/降级失败保留证据；待开始 |
| 3 准入与索引优化 | 定向重构 Server 启动前批量归档确认、Runner admission 及 capacity scan；增量索引、精确查找，缩小锁范围；保留原子预约/耐久声明/并发 fencing | 正常 start 全量扫描 0、无逐历史记录 RPC/持久化；并发同 key 仅一份执行，不超容量，索引可恢复；待开始 |
| 4 独立自动清理 | 按已确认保留策略实现生命周期所有者、分批维护、增量游标、压力有界回收、取消/关闭排空、失败诊断 | 运行中及未核对不确定 Run 不误删；故障/崩溃/部分清理可恢复；不阻塞普通准入；维护任务有界、无重复线程；待开始 |
| 5 超时管理 | 复用 McpHostRuntimePolicy，贯穿剩余 request deadline，区分准入/ACP setup/Run 执行/观察/取消；检查各 adapter 与 Host 返回余量，保留 idempotent recovery | 单次观察等待不超过有效请求预算；无事件正常返回，外层 timeout 不自动取消 Run 或新 key 重派；准确区分 not_started/started/outcome_unknown；待开始 |
| 6 诊断与恢复 | 增加必要阶段/耗时与结构化错误：准入、库存/回执、写入、spawn、initialize、session/new、prompt、首事件、terminal、Server 回传。安全有界采集错误类别；不直接持久化原始 stderr/Prompt/凭据；诊断与普通结果分离 | 能定位责任层而非笼统超时；无输出不等于失败；证据可关联同一 Run；不引入逐事件同步 I/O 或无限日志；待开始 |
| 7 全链路复查与上游兼容 | 复查 Server/Runner/MCP/REST/CLI/Desktop/Workflow Session、协议、权限、取消、关停、Windows 文件与迁移路径；检查扩展边界、废弃代码/文档与具名消费者 | 静态审查完成、未验证项明确；获得独立授权后再做定向测试、基线复测、故障注入、编译/打包；待开始 |

依赖：1 → 2 → 3 → 4 → 5 → 6 → 7；已稳定契约且互不冲突的只读诊断可并行。第 1 批先定义测量/诊断契约，不等到第 6 批才收集可比较证据；需实际运行测量时遵守独立授权。不得同时修改共享状态或重叠文件。

## 6. 验收指标与验证边界

- 静态可验证：常规启动零全量库存/归档扫描；不批量加载历史到内存；精确索引查询；无重复归档回执热路径；维护有界；官方核心改动只限必要接口。
- 运行验收（需要相应授权）：隔离环境下对 0、少量、接近容量上限、长历史、并发、慢响应、断连、重启、磁盘/持久化失败等条件记录 start p50/p95、scan 次数/记录数、锁等待/持有、首事件时间、维护 I/O 和内存。使用同一数据规模、硬件、Provider、模型及 Fast 条件对比；不预先承诺毫秒数或提升百分比。
- 安全验收：同 key 重放、不同 intent 冲突、过期 key、跨项目/权限/Runner、丢失回执、归档/清理崩溃边界、取消/关闭资源回收、长期运行与旧格式兼容。明确无证据不重派。
- 默认静态复查，实际测试、benchmark、构建、打包、部署和用户数据操作按项目授权分别进行。不要把工具回执、源码审查或历史测试当成当前运行验收。

## 7. 后续官方融合规则

每批记录实际 upstream commit/blob、修改过的官方文件、二创扩展入口、取舍和具名消费者。避免改写官方通用 ACP 执行状态机、重复其协议、永久 fork 整个模块。上游更新时优先核实等价能力，能采用官方则删除对应二创逻辑；冲突时先统一契约，再适配必要扩展，禁止机械复制或覆盖用户数据。不得未经授权同步 GitHub、切换分支、提交 PR 或发布。

## 8. 进度维护

每批结束更新：状态（待开始/执行中/阻塞/静态完成/运行验收完成）、负责人及执行器、实际文件和变更范围、Run/Job ID（必要时）、核实证据、未验证项及下一批入口。已启动任务先查询原任务，不重复启动；ACP 沿用配置默认 Composer 2.5、Fast=false，不覆盖模型。完成任务书后应合并到当前权威说明或明确历史状态，不继续维护重复的接续文档。

### 2026-11-25 第一批接续记录（未验收、未提交）

- 执行器：GPT / WebCodex 原生只读、GitHub 只读；Session `wc_sess_jmM4OshWcAaA-47q`。基线分支 `integration/upstream-main-2026-11-24`，HEAD `c81376cadfb9cb884517adcb2f60c090de10f941`；工作区已有 4 个修改文件、11 个未跟踪项（含其他 Workflow Session 改动及安装包），不得纳入本次提交。
- 已完整读取任务书与根 AGENTS.md；docs/AGENTS.md、docs/implementation/AGENTS.md 不存在。重新通过 GitHub main 读取本节所列五个 ACP blob，身份均与第 2 节一致；另核实官方 src/mcp_host.rs blob `64ed6dd613b28d347942c59d62a0b4670cb15a67`。尚未固定完整 main commit。
- 本地 start（Runner coding_agent.rs:725）在 admission 锁内先 cleanup_expired，再精确 replay 查询、活跃容量扫描、enforce_durable_capacity_for_admission、耐久 BeforePromptBarrier。lifecycle.rs:279/326 容量回收反复 list_records_for_retention，archive-required 时另扫描候选。Server coding_agent.rs:671/1674 在新 Run 派发前取得归档库存并逐记录持久化回执。现有归档是原 DurableRunRecord 目录原子移出，已经不含 instruction/完整事件流；不可为了轻量化删除安全字段。core archive_identity_fingerprint（1243）绑定 run/intent/authority/project/provider/provider instance/created_at，不含 terminal message。
- Server prepare（coding_agent.rs:581）由 principal 与 idempotency_key 生成 Run ID；已读输入校验未见 key 年龄限制。故不得在未建立安全拒绝过期重放的契约前按年龄删除未知结果防重记录。现有 archive marker/回执与精确查询必须配套。
- Host policy 已有按请求选择及部署上限；ACP Server 独立 start 32s/control 65s、observe 协议上限 60s；仍需追踪 adapter 裁剪及总 deadline 传递，不能仅凭常量认定超时根因。
- 历史待确认建议（已被第 4 节用户决策取代）：D1 成功/确定取消热记录 15 分钟、明确失败 24 小时；普通防重窗口是否保持现有 24 小时须确认。D2 异常详情 7 天后仅在契约允许时精简，必要身份/状态不删；D3 未核对结果的最小防重与路由证据保留，彻底删除须另有可拒绝重放契约；D4 5 分钟独立维护、建议每轮最多 64 记录、2 秒协作预算及有界压力唤醒（不声称硬 I/O deadline）；D5 保持 v1 精确查询，兼容读优先、仅在必要时设计原子幂等迁移，不实际迁移用户数据。上述均是待确认建议而非既定规则。
- 下一步：确认 D1-D5 后补齐 Server/Runner/adapter 消费者与相关子目录 AGENTS.md、共享契约及第一批验收，再提交第一批；后续批次按依赖推进。未运行测试、构建、ACP Agent 或实际数据维护，无本批 commit。

### 2026-11-25 用户决策接续

- D1-D5 已由用户明确确认，见第 4 节；不再按之前建议执行。恢复原 Session；jobs.attention 返回 0 个活动 Job（不等同于 ACP 全局库存）。Git 状态仍为先前的 4 个修改文件与 11 个未跟踪项，任务书尚为未跟踪文件，独立提交时只选本文件。
- 已追加核对 `src/tool_runtime/agent_task.rs` 156/904-1007（固定 Attempt replay key 与 durable binding claim）、`src/tool_runtime/dispatch/execution.rs`（canonical dispatch）、`src/tool_runtime/coding_agent_archive.rs`（私有不可变回执与原子发布）、`crates/webcodex-core/src/coding_agent.rs` 330/638（Start wire 与 Run ID 校验）、`crates/webcodex-runner-registry/src/runners.rs` 1620-1690（跨 Runner 精确查找与归档库存）、`docs/compatibility-policy.md`（已发布 v0.4.6 baseline、能力协商）。相关 src、src/tool_runtime、crates、Runner 及 coding_agent 路径下 AGENTS.md 定向读取均返回 not_found。CLI/Desktop 搜索 idempotency_key 未见直接字符串消费者，不等于全链路复查完成。
- 第一批交付为设计与任务书；第二批按 4.1 开始共享 replay/冷热存储契约，第三批移除启动热路径全扫、第四批独立维护，第五批预算、第六批诊断、第七批审查。完整 adapter 路径、持久过期水位/降级安全及具体实现仍需后续逐批核实。尚无源码修改、测试、构建、真实数据维护或 ACP Agent 执行。

第一批静态验收：设计契约与用户 D1-D5、已核实本地及官方 blob 一致；无代码修改或运行验证。提交范围仅本任务书，第二批开始共享协议及冷热归档源码改造。昨日融合进度及尚未完成的安装/迁移验收仍见 [融合记录](upstream-main-fusion-progress-2026-11-24.md)；不得将其历史快照当作本轮当前源码。

