# WebCodex 官方 main 与个人二创融合 — 任务进度与接续记录

> 2026-11-24 建立。此文档是当前融合任务的进度记录与后续会话接续入口；不是运行验收报告。

## 1. 当前基线

- 工作区：`E:\myself\webcodex`（Windows）。
- 融合分支：`integration/upstream-main-2026-11-24`。
- 个人 HEAD / main：`9a85be368b832ec29e2b12642004b09446dca7dc`。
- 官方 main / MERGE_HEAD：`026413dce8a7166a3ba27d8df19e321a46782532`（https://github.com/yyjeqhc/webcodex）。
- 初始冲突 89 个，累计解决 89 个，剩余 0 个；冲突解决率 100%。第一至第六批已完成。已验证本地双父融合提交 `12c1029994b08ef1c951e829d260b45f92e4e658`，仍在 integration 分支，不等同于功能验收。
- 未重新 merge、未重置、未切换分支；保留已有融合成果及无关文件。未推送、未合入 main、未执行测试/编译或实际数据库迁移。第七批待授权。

## 2. 总体策略与授权

官方优质核心架构优先；保留二创独有且有效的功能、安全性、可靠性及用户数据；官方等价或更优的实现替代二创重复机制，不以保留旧源码为目标。已经解决的模块只按实际缺陷或架构收益定向复查，不推倒重来。

GPT 主导，WebCodex 为默认执行通道，Cursor ACP 按实际收益委托；仅在互不冲突、契约稳定时并行。ACP 沿用配置默认 Composer 2.5，不主动覆盖；此前因 retention_capacity_exhausted 明确未启动，后续须核实状态，不盲目重复派发。

目前授权：D1 官方 SQLite 结构及一次性无损迁移源码；D2 不按数量自动淘汰持久历史；第一至第六批源码开发、静态审查及本地融合提交。实际数据库访问/迁移、测试、构建、打包、推送、合入 main 仍需相应授权。

## 3. 七批任务与验收条件

| 批次 | 任务 | 状态 | 验收条件 |
| --- | --- | --- | --- |
| 1 | 架构与数据契约统一 | 已完成（D1/D2 已确认） | 对比双方真实源码；记录唯一目标契约、迁移边界、身份、缓存、持久化屏障；明确业务决策 |
| 2 | Session 核心融合 | 已完成（源码与静态复查；未编译/运行验收） | Store/Writer/Hot-Cold/消息操作统一；按需加载、缓存淘汰不丢未提交数据；移除重复机制 |
| 3 | 数据兼容与可靠性 | 已完成（迁移/恢复源码与定向静态复查；未实际迁移） | 一次性兼容迁移设计及代码（依确认策略）；备份恢复、失败原子性、旧数据身份与关闭屏障 |
| 4 | Server 调用链适配 | 已完成（源码与定向静态复查；未编译/运行验收） | 初始化、工具、Coding Agent、Work Result、Console、协作及 Project 权限契约一致；冲突归零 |
| 5 | 已融合模块复查 | 已完成（定向源码复查与必要修复；未编译/运行验收） | Runner/Desktop/MCP/ACP/OAuth/Work Result/Environment 按真实代码独立审查；仅修复有依据的问题 |
| 6 | 全局静态审查及融合提交 | 已完成（融合提交及双父核验成功） | 无未解决冲突/重复入口/废弃引用；审查权限、兼容性、持久化及 Windows；仅提交本任务改动并记录 ID |
| 7 | 编译、打包与验收 | 待授权 | 按现行构建脚本执行；报告实际结果与安装包绝对路径；运行验收后另行决定合入 main、推送 |

依赖：1 → 2 → 3/4（共享接口和数据库先串行确定，独立部分可并行）→ 5 → 6 → 7。禁止多个执行者同时修改相同文件或共享状态。阶段完成以实际源码和复查证据为准，不以任务派发为准。

## 4. 已有融合成果（此前执行，尚需第 5/6 批复查）

- Runner / Project catalog / Tunnel：官方容量、生命周期与二创任务终态和权限 fencing。
- Desktop：保留二创轮询隔离，吸收官方状态与生命周期改进。
- MCP / ACP：统一 diagnose_coding_agent；保留配置策略、权限响应和结构化恢复，禁止 Gateway 绕过 Host 审查。
- Work Result：官方布局/导航，保留二创 Diff 视觉增强，资源版本 v40。
- OAuth / Ingress：官方动态客户端注册与认证契约，二创阻塞隔离。
- Environment：保留已有账号保护、用户名规范化，吸收官方错误诊断与旧安装恢复。
- Session：已合并结构化 Project 身份、未完成写入判断、消息准入、数据库底层及 Server 初始化的部分冲突。此项为第一批前的历史状态；第二批已按 D1/D2 切换官方 SQLite / Store / Writer / Hot-Cold 主架构，见第 8.6 节。

## 5. 第一批剩余冲突基线（16 个，历史快照）


- `crates/webcodex-workflow-session/src/assignment_tests.rs`
- `crates/webcodex-workflow-session/src/collaboration_tests.rs`
- `crates/webcodex-workflow-session/src/identifier_tests.rs`
- `crates/webcodex-workflow-session/src/message_mutation_tests.rs`
- `crates/webcodex-workflow-session/src/persistence.rs`
- `crates/webcodex-workflow-session/src/session_tombstone_tests.rs`
- `crates/webcodex-workflow-session/src/store.rs`
- `crates/webcodex-workflow-session/src/store/residency.rs`
- `crates/webcodex-workflow-session/src/store/residency/tests.rs`
- `crates/webcodex-workflow-session/src/store/writer.rs`
- `crates/webcodex-workflow-session/src/store/writer/async_tests.rs`
- `crates/webcodex-workflow-session/src/store/writer/observation_tests.rs`
- `crates/webcodex-workflow-session/src/store/writer/tests.rs`
- `src/tool_runtime/tests/coding_agent_context.rs`
- `src/tool_runtime/tests/collaboration.rs`
- `src/tool_runtime/tests/sessions_retired_projects.rs`

## 6. 第一批契约核查

状态：已完成。2026-11-24 用户明确确认 D1/D2。

### 6.1 已核实的源码证据（2026-11-24）

核查基于本地 HEAD、MERGE_HEAD 和当前合并工作区，未读取实际数据库记录。

| 领域 | 二创 HEAD | 官方 MERGE_HEAD | 第一批判断 |
| --- | --- | --- | --- |
| SQLite 格式 | `workflow_session_storage(id,schema_version,legacy_imported)`；`workflow_session_state` | `workflow_session_storage(singleton,version,imported)`；`workflow_sessions` | 同为版本 1 却不是同一格式；必须识别结构，不可按版本数字直接打开 |
| 事件 | `session_id,ordinal,event_id,event_json` | `sequence,event_id,session_id,kind,call_id,ordinal,payload` | 可从现存事件 JSON 投影官方字段；保留原 ordinal 和累计计数，不补造缺失历史 |
| 消息 | `observation_revision,message_json`，分配和完成围栏见状态 | `revision,payload,assignment_floor,completion_fence` | 迁移须保留每条观察修订、分配/完成围栏及其原有缺失语义 |
| 重放记录 | `workflow_session_message_delivery_replays` | `workflow_session_deliveries` | 保留作用域、消息 ID 与载荷指纹；不得将旧请求误判为可重放 |
| 墓碑 | `workflow_session_retention_tombstones` 的 owner/incarnation/expiry_ordinal | `workflow_session_tombstones` 的 payload | 已淘汰 Session 身份不可复活；保留原所有者与 incarnation |
| 缓存 | 冷记录持有完整有界 JSON；启动 inventory 后逐条 load | inventory 只构造状态元数据；Cold 持数据库及已提交版本；16 MiB Hot 目标 | 推荐官方机制，必须区分缓存淘汰与持久历史删除 |
| 写入 | `apply_batch_for_flush`、代际屏障、同步/异步确认、关闭排空 | `persist_batch_observed`、dirty revision、expected revision、pending_events | 采用官方主结构时保留未被等价覆盖的关闭/错误/确认能力 |
| 历史 | closed 100、events 2000、messages 200 | 16 MiB 缓存、2000 最近事件读取窗口，不以旧数量上限淘汰持久历史 | 用户须明确策略；2000 读取窗口不等于 2000 持久数据上限 |
| 身份 | 类型化 `SessionProjectIdentity`，root_fingerprint + registration_incarnation | 字符串身份；可用注册路径形成指纹 | 保留二创强身份与失效拒绝，不按当前 Project 重新认领旧 Session |

源码锚点：

- `crates/webcodex-workflow-session/src/persistence/database.rs`：HEAD 65、280、379、428、637–706；MERGE_HEAD 35–111、128、186。
- 同 crate `store/residency.rs`：MERGE_HEAD 60 起；`store/writer.rs`：HEAD 401、484、505、539；MERGE_HEAD 87、169、176 起。
- 同 crate `store.rs`：HEAD 607、703、1922、1935；MERGE_HEAD 250、1301、1334、1345。
- `src/lib.rs`：当前 344/353 由 ServerInstanceGuard 确定 DB 路径；424 初始化 Session；1025 调用 shutdown_persistence。
- `crates/webcodex-store/src/schema.rs`：1024 起的 Workflow Session schema 校验；1189 validate_shared_database_backup_schema。backup.rs 1324、restore.rs 284 共用此校验。
- `crates/webcodex-workflow-session/src/model.rs`：当前 147–175 强身份类型与验证。

### 6.2 不依赖待确认业务策略的契约

1. 唯一持久化来源：使用 ServerInstanceGuard 所确定的共享数据库路径。Session Store、备份维护、恢复校验不能各自推断不同路径。
2. 缓存独立于生命周期：Active/Closed 不代表驻留状态。Cold 按已提交 revision 加载；脏记录、尚未确认的事件/消息和受保护的操作目标不能被淘汰。16 MiB 为缓存目标而非硬性进程内存上限；达到压力时控制准入，不丢数据。
3. 提交与确认：批次取一致快照；SQL 事务成功后才推进已提交 revision；并发新变更不被旧批次清除；冲突与不确定提交不得按成功处理。精确围栏优先于盲目重试。
4. 请求准入与关闭：存储不可用且未执行返回 not_started/state_changed=false；发生内存变更但提交结果未确认则返回明确的不确定状态。关闭时停止新持久化变更准入、等待在途任务、排空写入并确认最后一代提交。只保留一套实际承担此职责的 Writer。
5. 身份：原 Project ID、创建者权威指纹、root fingerprint 和 registration incarnation 联合约束，不允许从当前路径推断旧身份。证据不足的旧身份只可按明确的历史读取授权规则处理，不能获得当前 Project 执行权。保留退役历史与防复活语义。
6. 恢复：精确调用 ID、逻辑调用身份、项目和事件配对；不确定的写入不能自动重放。近期读取窗口外的未确认执行必须通过完整保留历史或可靠持久化索引核实，不得因缓存裁剪误判。
7. 模型输出和 DB 数据保留是不同契约：继续限制单次摘要、分页和查询资源；不可将模型响应上限误作持久数据删除依据。

### 6.3 已确认方案（D1/D2）

D1：采用官方 SQLite 结构，开发一次性无损兼容迁移机制，不授权实际数据库迁移。

D2：采用官方历史保留策略，不再按数量自动删除持久历史；已删除的数据不凭空恢复，现存墓碑语义保留。

D1 迁移设计必须满足：

- 在任何新 schema DDL 前，区分空库、完整二创格式、完整官方格式、未知/混合/损坏格式；未知版本/不完整状态拒绝继续。避免两个版本 1 标记互相混淆。
- 独占实例所有权且无活动 Writer、备份/恢复并发；真实迁移前必须有经校验的可恢复备份、明确回滚条件及用户授权。迁移实现与实际执行分离。
- 单次受控事务中转换旧数据；先验证目标结构和内容，再原子更新标记；事务失败保持源数据可用。不得用简单 DROP 或覆盖清空旧表。共同表名需通过受控的临时表/重命名方案处理，核对 FK 关系和其他共享数据库领域不受影响。
- 按上表转换现存主记录、事件、消息、分配/完成围栏、重放记录、墓碑、所有者、创建时间、累计计数、修订和 legacy import 状态。旧事件缺口保留；不发明当前 instruction、身份或已完成记录。若无法证明可等价映射，拒绝迁移并报告具体差异。
- 保证消息 ID 从旧复合键到官方全局键的唯一性；检查重复事件 ID/ordinal、孤立引用、revision 水位、记录数量及逐 Session 语义一致性，不能静默丢弃异常记录。
- 旧备份格式识别与恢复校验需要同步适配，且恢复校验保持只读；旧备份是否能在迁移后的版本恢复须给出明确支持路径。不维护两套常态 Writer。
- 所有验收先用隔离的合成数据覆盖空库、旧库、官方库、错误 schema、重复执行、事务中断、身份缺失、旧墓碑、未完成事件及备份恢复；实际测试与数据库操作按后续授权执行。

### 6.4 后续实施边界

- 第 2 批：统一 model/store/persistence/residency/writer/messages 及对应测试源码；按已确认 D1/D2 采用官方主架构；禁止只删除冲突标记就声称功能完成。
- 第 3 批：编写旧库到官方结构的一次性无损迁移及备份校验适配，补齐提交/关闭契约；不执行实际迁移。
- 第 4 批：统一 Server 初始化与 shutdown、Session 工具、身份和协作调用，重新审查此前已解决但依赖旧契约的源码。
- 第一批契约分析与用户确认均已完成。本轮未修改业务源码、未运行测试、未构建、未连接实际数据库、未迁移、未提交/推送。

## 7. 接续日志

| 日期 | 执行内容 | 状态 / 下一步 |
| --- | --- | --- |
| 2026-11-24 | 创建进度文档；复核分支、16 个剩余冲突；对比双方 schema、缓存、Writer、历史、强身份及备份调用链 | 第一批分析完成，D1/D2 待确认；随后经用户确认进入第 2 批，禁止重新 merge |
| 2026-11-24 | 用户确认 D1 官方结构+一次性无损迁移源码、D2 官方历史保留；完成第一批，开始第二批 | 禁止实际数据库迁移；第二批进行中 |
| 2026-11-24 | 第二批首组：官方 DB/persistence、结构化身份校验及按字节 residency，修改 4 个源码文件 | 解决 2 个冲突，剩余 14；Store/Writer 接线、恢复及迁移未完成 |
| 2026-11-24 | 第二批第二组：官方 Store/Records/Writer 接线、持久化准入与关闭、完整历史恢复扫描、Server 异步确认 | 再解决 2 个冲突，剩余 12；第二批仍进行中 |

后续每批结束更新表格和此日志，记录实际修改、冲突数量、静态检查、运行验证边界、任务标识及未完成项。若已有任务状态未知，先恢复/查询，不重复启动。

## 8. 第二批开发记录（2026-11-24）

### 8.1 已实施的第一组源码改动

| 文件 | 实际改动 |
| --- | --- |
| `crates/webcodex-workflow-session/src/persistence/database.rs` | 从固定官方 MERGE_HEAD 采用规范化 SQLite 表及 revision-backed inventory/load/write；加入旧库/混合库结构预检、事务内原子 schema 初始化、强身份与不可回退的版本/生命周期校验、墓碑拒绝写入及读取、按 Session ordinal 读取事件 |
| `crates/webcodex-workflow-session/src/persistence.rs` | 采用官方状态/历史分离和 Cold 按 revision 加载实现，适配二创类型化 Project 身份；历史身份原样保留，不在 restore 时升级为强身份 |
| `crates/webcodex-workflow-session/src/model.rs` | 补充持久身份合法性检查；区分可保存的历史身份和可授权的强身份 |
| `crates/webcodex-workflow-session/src/store/residency.rs` | 采用官方按字节预算和精确 Cold 加载；新增统一 committed_cold_for_eviction 判定，只有无 dirty/待写事件且数据库、revision、身份、生命周期、事件和消息水位一致时才可释放 Hot 数据 |

本组是第二批的基础实现，尚不是独立可运行版本。官方 SQLite 表结构代码已落地，但原 Fork DB 的迁移代码尚未开发，普通 open 对旧标记显式拒绝，不自动迁移。新版结构与旧官方字符串身份 JSON 的兼容转换仍须设计/核查；不得将字符串身份静默丢弃或赋予当前 Project 权限。

### 8.2 下一组必须完成的接线

- 以 MERGE_HEAD 的 SessionRecords、Store、Writer 为主架构，将精确代际确认、mutation admission/关闭 drain、写入异常与未知提交结果处理适配至同一 Writer；不得恢复旧按数量淘汰持久记录的机制。
- 统一 Store 的普通读取、Closed compaction 与 LRU，全部经新 committed_cold_for_eviction / revision load；保留旧墓碑，不产生新的自动历史淘汰。
- 对事件尾部之外的未确认写入建立准确的持久历史核查；不能仅扫描最近 2000 条就断言可安全重放。
- 完成消息、作业证据及强 Project identity 端到端契约；复核 create/resume/permission/close 的失败返回，不能把入队当作 durable。
- 移除已无生产消费者的旧 Dirty Queue/JSON Writer 模块（保留明确用于一次性迁移或兼容读取的代码），不保留两套活跃写入实现。
- 第三批负责一次性迁移、真实格式识别与备份/恢复校验；旧备份须有支持路径。实际数据库迁移仍不授权。
- 当前另一会话处理 ACP retention/permission 修复及其相关协议；本融合任务只操作 Session 文件和进度文档，不覆盖或暂存该会话改动。

本轮定向 diff --check 已通过；未运行测试/编译，未实际打开用户数据库或执行迁移。当前仍处于未完成合并，禁止提交/部署。

### 8.3 首组剩余冲突（14 个，历史快照）

- `crates/webcodex-workflow-session/src/assignment_tests.rs`
- `crates/webcodex-workflow-session/src/collaboration_tests.rs`
- `crates/webcodex-workflow-session/src/identifier_tests.rs`
- `crates/webcodex-workflow-session/src/message_mutation_tests.rs`
- `crates/webcodex-workflow-session/src/session_tombstone_tests.rs`
- `crates/webcodex-workflow-session/src/store.rs`
- `crates/webcodex-workflow-session/src/store/residency/tests.rs`
- `crates/webcodex-workflow-session/src/store/writer.rs`
- `crates/webcodex-workflow-session/src/store/writer/async_tests.rs`
- `crates/webcodex-workflow-session/src/store/writer/observation_tests.rs`
- `crates/webcodex-workflow-session/src/store/writer/tests.rs`
- `src/tool_runtime/tests/coding_agent_context.rs`
- `src/tool_runtime/tests/collaboration.rs`
- `src/tool_runtime/tests/sessions_retired_projects.rs`

### 8.4 第二批第二组实施记录

2026-11-24，基于 D1/D2 继续开发。累计解决 77/89 个冲突，剩余 12 个（全为 Session 及 Server 的关联测试源码；生产核心源码的冲突标记已解决）。这不代表所有生产调用链已经静态验收。

本组实际改动：
- `store.rs`：使用官方 SessionRecords、SQLite Cold locator、Hot/Cold 按字节管理及单一 Writer；生产启动 Writer 创建失败即拒绝启动；取消二创自动数量淘汰主路径，保留历史墓碑的原始序号与强 Project 身份；创建/恢复/上下文/关闭、证据和消息 mutation 走共享准入门。关闭先停止准入，等待在途 token 释放，将最后一代通知 Writer，再 join 并确认。
- `store/writer.rs`、`store/records.rs`：官方批次写入与 generation 为基础，统一 async/blocking 确认；线程退出唤醒等候者；关闭 join 检查末代是否提交；只有对应的 revision、pending event 前缀确认后才清除 dirty，后续变更保留。提交后发布异常触发 fail-closed 监督。
- `continuation.rs`、`persistence.rs`、`persistence/database.rs`：完整历史恢复证据。抽取唯一 RetainedCallIdentity/RetainedCallPairing；近期窗口不足时 SQLite 按完整调用身份和 ordinal 逆序扫描调用证据，核对 revision、事件数量/ordinal、当前 pending overlay（保留首次出现顺序及最后 payload）；只持当前配对和未提交 overlay，不恢复完整历史 RAM 树。读取失败或 locator 不一致时拒绝安全重放。已保留旧数据缺口语义。
- `src/tool_runtime/runtime.rs`：生产初始化使用 open_database 与 DEFAULT_SESSION_CACHE_BYTES/DEFAULT_RECENT_EVENT_LIMIT。
- `src/tool_runtime/session_tools.rs`、`src/tool_runtime/coding_task/startup.rs`：create/update/work_on_project 成功前异步确认持久化；保留 not-started 与 uncertain 区分。
- `messages.rs`：修复先前自动合并遗漏的确认引用长度常量和解析边界；保持现有 12 字节 tag 兼容性。
- 已无 active module 声明的旧 dirty/batch_plan/writer/flush 源码暂留，待测试源码统一后删除。legacy_import.rs 暂留作为第三批迁移参考，不能接回常态 Writer。

本轮未做运行验证。静态检查仅限本轮改动、接口引用和冲突标记；以下必须继续，不得标记第二批完成：
1. 统一剩余 12 个测试源码冲突。保留原有强身份、错误与恢复用例，淘汰明确已废弃的数量淘汰/fallback 双轨契约；补齐官方按需加载和保留策略的回归覆盖。只修改源码，不执行测试。
2. 定向审查 SessionRecords 在失败/no-op 分支提前 mark 的 dirty 生命周期、generation 溢出、Writer commit publication、长历史扫描和 SQLite I/O 与 Store mutex 的竞态/开销；必要时采用更高效的等价设计，不宣称未测量的性能收益。
3. 检查所有工具调用的 start/permission/finish 证据失败是否正确阻断真实副作用；明确 ordinary enqueue 与 durable confirmation 的边界；核查 Server shutdown 幂等性、观察失败及取消。
4. 核对近期窗口之外的任务 instruction、changed-path、summary/handoff/Work Result 证据，以及消息全历史观察和分页。当前完整未完成调用配对已实现，不能据此声称所有历史查询已经适配。
5. 第三批实现一次性无损迁移及备份恢复校验；当前旧 Fork DB 仍会被正常启动拒绝，实际迁移未授权。
6. 并行 ACP 修复会话的 Runner/Server coding agent/协议范围不得写入或暂存。本组只操作列明的 Session/Runtime 文件和进度文档。

### 8.5 第二批第二组剩余冲突（12 个，历史快照）

- `crates/webcodex-workflow-session/src/assignment_tests.rs`
- `crates/webcodex-workflow-session/src/collaboration_tests.rs`
- `crates/webcodex-workflow-session/src/identifier_tests.rs`
- `crates/webcodex-workflow-session/src/message_mutation_tests.rs`
- `crates/webcodex-workflow-session/src/session_tombstone_tests.rs`
- `crates/webcodex-workflow-session/src/store/residency/tests.rs`
- `crates/webcodex-workflow-session/src/store/writer/async_tests.rs`
- `crates/webcodex-workflow-session/src/store/writer/observation_tests.rs`
- `crates/webcodex-workflow-session/src/store/writer/tests.rs`
- `src/tool_runtime/tests/coding_agent_context.rs`
- `src/tool_runtime/tests/collaboration.rs`
- `src/tool_runtime/tests/sessions_retired_projects.rs`

### 8.6 第二批最终完成记录（2026-11-24）

**状态：第二批源码开发及定向静态复查完成。** 这是第二批范围内的完成，不是整个融合分支可部署的承诺。第三批迁移及备份/恢复兼容、第四批跨 Server 模块契约、第五/六批全局复查和第七批运行验收仍独立保留。

- 12 个剩余关联测试源码冲突已解决；初始 89 个全部解决，索引未合并条目为 0。保留强 Project identity / 退役与历史身份 / 错误恢复用例，采用官方非数量淘汰和单 Writer 语义。另修正自动合并遗留的 13 处重复 project_identity 初始化。
- SessionRecords 对消息、ACK、withdraw/replace/resolve/complete 等实际变更登记代际；失败与 no-op 不产生虚假 dirty。版本与 observation revision 受 SQLite 整数边界保护。关闭 mutation admission 并排空 token 后，Writer 在同一最终代际上请求关闭与 join；重复关闭不重启 Writer。持久化配置存在但 Writer 缺失时拒绝准入。
- SQLite 事务显式区分确认回滚与未知提交/身份或版本异常；后者触发持久化 fatal 并停止重试。事件更新检查不可变身份，写入与按需读取校验 ordinal、累计观察数与预期 revision。
- 普通 Kernel recording、dispatch governance、specialized consequential 调用执行前确认对应 Session 记录；持久化屏障失败不进入外部副作用。普通非 consequential 读路径仍用准入背压；终止事实按既有记录机制处理。Server 初始化、关闭、权限及其他交互在第四批再做跨域复查。
- 长历史不受 recent_event_limit 截断：未关闭调用、按旧观察快照定位任务指令、旧事件的权限与关闭证据、changed-path 归属均按准确 revision 和完整历史/未提交 overlay 核查。单次路径投影有独立上限，失败或缺口不得声称完整；SQLite 历史不因此删除。消息历史与分页仍使用官方持久化记录语义。
- 删除没有任何活动模块声明或调用者的 store/dirty.rs、store/batch_plan.rs、store/writer/flush.rs、persistence/legacy_import.rs 及三个旧数量淘汰常量。第三批需参考旧 JSON 读取/迁移逻辑时，从固定个人 HEAD 9a85be368b832ec29e2b12642004b09446dca7dc 的原 legacy_import.rs / persistence/database.rs 读取，不恢复双 Writer。
- 已有回归源码进一步覆盖小 recent window 下的旧未关闭调用、提交与重开，以及旧真实编辑路径归属；仅修改现有测试源码，未执行。

最终静态证据：
- 本批 55 个关联 Rust 文件 rustfmt --check（edition 2021、skip_children=true）通过；定向 git diff --cached --check 通过。
- 本批关联源码无合并标记；git ls-files -u 为空；旧数量常量及废弃模块声明全仓 Rust 引用检查无匹配。
- 保持当前融合分支及 HEAD/MERGE_HEAD 不变；只定向暂存本批文件，未提交、推送、构建、运行测试、打开用户数据库或执行迁移；不覆盖并行 ACP 会话的文件。
- 静态解析与源码复查不是 Rust 类型检查或运行验收。新版 schema 与旧 Fork schema 的真实兼容、数据完整性及全局编译结果仍未确认，不能安装或启动融合版本。

下一步：第三批，基于 D1 已确认的一次性无损迁移，先实现新旧 schema 的精确只读识别契约和源码级事务迁移、备份恢复适配；不得在实际用户数据库上执行迁移。第四批需复查上述 Session/Runtime 前置接线以及 Server 全部相关消费者。无需重新 merge 或重复第二批已完成的源码改造，后续仅针对新证据修复。

## 9. 第三批 — 数据兼容与可靠性（2026-11-24）

**状态：第三批源码实施及定向静态复查完成。** D1/D2 继续适用。仅交付兼容迁移、备份、恢复及其显式授权入口的源码；没有在真实用户数据库上执行这些操作。

### 9.1 已完成的迁移契约

- 统一格式识别：Session owning crate 的 detect_workflow_session_schema 识别 Absent/ForkV1/CurrentV1，核对精确表/列集合、主键、事件唯一键和唯一 marker 行（版本与 import 状态）；拒绝混合、不完整、未知格式与残留迁移表。Store 启动和备份/恢复共用该检测器，普通启动在 WAL/schema 写入前拒绝旧 Fork。无重复常态 Writer。
- 事务转换：persistence/database/migration.rs 只在调用者持有 IMMEDIATE transaction 时执行转换，保留旧表直到新表已全部写入且核验成功；旧事件 1-based ordinal 转换为 0-based ordinal（减 1），保持累计 waterline 和旧缺口。逐 Session 转换并保留原 revision、owner、强 Project witness（或已存在的弱历史身份）、生命周期、创建/更新时间、保留事件、最新可证实指令、消息观察修订及丢失 floor、assignment floor、completion fence、delivery replay、原墓碑及 legacy_imported marker。
- 无损与拒绝边界：通过 canonical restore 对比完整保留状态，逐表计数及 indexed identity EXCEPT 核验、foreign_key_check，验证完成才删除重命名的旧表。冲突、重复身份、错误 ordinal/水位、无法等价映射的状态或不明旧 JSON import 标记一律拒绝，不跳过、不重新认领、不创建替代身份。旧官方 string-only Project witness 无法无损形成二创强身份，本次个人 Fork 转换不猜测映射，明确拒绝不兼容行。
- 旧 JSON 一次导入：同一 IMMEDIATE transaction 读取 marker、确认空库、导入、登记 marker，阻止重复导入、墓碑重叠与重复序号；迁移保留旧 marker，避免旧 JSON 复活已删除历史。

### 9.2 实际入口与安全边界

- Store restore/session_migration.rs：OfflineRestoreOwner 在实例锁、无活动 restore journal 及 SQLite IMMEDIATE reservation 下复用 backup.rs 的已有限时分步复制、验证、发布机制，不先打开并升级通用 Store。对同一预写入快照生成并验证完整回滚备份；失败或未知结果不删除备份。事务失败确认 rollback，提交成功后再次核对 schema、文件身份及共享 DB 完整性。已是当前格式则验证后 no-op。
- Server binary 新增显式离线命令 --migrate-workflow-sessions DB_PATH，要求绝对路径；不启动 Server、不通过普通启动或后台任务自动调用。
- 旧备份恢复：restore/staging.rs 增加 restore_from_fork_backup 独立授权路径，--restore-fork-workflow-backup DB_PATH BACKUP_PATH 仅接受明确的绝对路径和原有 published backup 约束。原始备份只读；私有 stage 完成转换、校验、关闭 SQLite、重算大小与 SHA-256 后才进入原有 preserve/journal/atomic install，普通 restore 不隐式转换。恢复日志的 candidate hash 对应转换后的 stage，source backup filename 仍记录来源。Server startup 仅在 current Session schema 就绪、原有恢复条件通过后确认 Committed；转换或启动失败保持原恢复回滚边界。
- 不更改真实数据库、不改现有数据库配置、不绕过实例所有权；真实执行仍需要停机、备份及用户明确授权。具体运行说明见 session-active-residency.md，尚未发布/打包。

### 9.3 静态验收记录

- 本批 13 个相关 Rust 文件 rustfmt --check（edition 2021，skip_children=true）通过，定向 git diff --cached --check 通过；Cargo.toml/Cargo.lock 新增的仅是现有 workspace 内 Store 到 Workflow Session 的依赖，无外部依赖下载或升级。
- 现有三处回归源码补充：个人 Fork schema 的事务回滚、保留 ordinal/缺口/原 revision、强身份、消息投递及完成重放、墓碑、旧 JSON marker；Store 的真实入口备份/格式识别/旧备份私有转换与恢复日志 startup commit；Server CLI 明确参数门。修复 fixture completion_id 格式。仅静态复查，未执行测试。
- 初始 89 个 Git 冲突全部解决，索引未合并条目为 0；未重启 merge、未提交或推送，HEAD/MERGE_HEAD 保持原值。未覆盖或暂存并行 ACP 会话改动。
- 本批的源码完成不代表已经在真实数据库或临时测试数据库上证明迁移成功；Rust 类型检查、运行回归、安装验证及实际生产迁移均未执行。禁止直接部署或在实际数据上运行上述新命令，直到对应授权与验证完成。

下一批：第四批 Server 调用链适配，接续当前分支，统一 Session 新契约的全部消费者、权限/身份、Coding Agent、Work Result、Console、协作和关闭状态；不得重复第二、三批已实施的工作，仅依据实际新问题补充修复。保持并行 ACP 写入边界。

## 10. 第四批 — Server 调用链适配（2026-11-24）

状态：本批源码实施与定向静态复查完成。未编译、未运行测试、未启动新版 Server、未打开或迁移真实数据库。保持待合并分支，未提交和推送。

### 10.1 实施结果

- Session / Project authority：退役 Project 历史授权使用同一 Runner 注册表加锁快照核对所有者/授权组、连接与清单状态，拒绝仅凭全局可见性读取他人的退役记录；Session 本身仍需精确主体授权。当前注册 Session 发现先按强 Project 身份筛选再统计、排序、分页，对结果逐条授权并在返回前复核注册身份；冷记录载入失败不伪装为不存在或跳过分页。
- Session 生命周期与执行证据：关闭 Session 在发起 persistent shell 清理前确认持久化，清理后再确认；普通 kernel 与 dispatch 使用同一个实际 ToolEffect 准入判断，Mutate/Execute/Unknown 及 include_live_acp=true 的诊断均在执行前确认开始证据已提交。保留 read-only 观察的原有背压，保留 specialized 分支按 effect 分类的屏障。提交不确定不报告已完成或鼓励重复执行。
- 协作：带 delivery key 的发布和 assignment/completion 保留已有原子提交/重放逻辑；普通公开发布及 resolve 成功返回前增加持久化确认，失败返回带 Session/message id 的 persistence_uncertain；observe 长轮询返回前再次授权，避免等待期间注册变化造成旧记录暴露。未改变 message id、completion key、ACK token 或观察版本编码。
- Runtime Console：Runner Project selection 从同一清单快照传递注册 incarnation 与根指纹；只在内部 DTO 保存 typed SessionProjectIdentity（不新增公开 JSON 字段）。列表、首页统计、Runner 概览、精确详情与 Window Session 关联统一按当前注册过滤；旧历史仍保留，不计入新注册；原 project:read/runtime:read scope 分层不改。locator 先读取 Session 元数据再授权，避免先加载他人历史；返回前复核注册身份，冷读取缺失标记结果不完整。
- Work Result：工作区 observation cache 复用身份增加 registration incarnation；组装前后对比 Project 根指纹、注册 incarnation、路径和 Runner client id，精确 Session 再次授权后才返回；跨注册变更失败关闭，不混合历史证据或当前工作区。保留官方 v40 资源与既有视觉/协作投影。
- Server 启动/关闭：定向复查实例锁、pending restore 声明顺序、共享 DB 路径、Session DB builder 和恢复提交屏障；确认完整 runtime 构建后才 commit restore 并启动持久化 fatal watcher、QUIC 等任务；备份停止及 drain、Session shutdown_persistence、错误传播顺序保持。第三批显式迁移授权不变。
- MCP / ACP / OAuth 边界：核对 canonical enum → scope/metadata → MCP 直达/网关 → dispatch execution → runtime 的 diagnose_coding_agent、respond_coding_agent_permission 接线，保留精确 pending token/option 与 provider/runner instance；live diagnosis 仍需执行权限；检查 Host review 路由与共享认证调用签名。当前独立 ACP 修复仍由另一执行者持有 15 个未暂存文件（包括 Server coding_agent.rs 和 core/Runner 协议）。本批仅只读核对接口，不覆盖、不暂存、不代为提交，也不宣称独立修复已验收；已发送边界协调消息，未获得最终完成回执。第五、六批须在其写入结束后重新审查合并后的 ACP 完整状态。

### 10.2 本批改动与静态验收

- 19 个 Rust 文件修改并定向暂存：Runner registry 的 runners.rs / project_selection.rs / console_snapshot.rs；Workflow Session 的 store.rs / discovery_tests.rs；Server 的 project_resolution.rs / session_tools.rs / projects.rs / runtime.rs / sessions/mod.rs / kernel/recording.rs / dispatch/governance.rs / work_result.rs；runtime_console_http.rs / window_queries.rs / tests/overview.rs；tests/session_discovery.rs / session_retention.rs / specialized_dispatch.rs。
- 补充现有测试源码：同根不同 registration incarnation 的列表计数、Console detail、旧历史仍在；ACP permission response/live diagnostic 的实际执行效果与持久化准入判断；修正创建屏障失败的错误分类和测试引用。没有运行测试。
- 19 个改动 Rust 文件 rustfmt --check（edition 2021, skip_children=true）通过；另对 23 个直接相关 Rust 文件使用 rustfmt --emit stdout 只读解析通过；42 个文件的定向 git diff --cached --check 通过。以上只证明语法/格式/文本静态检查，不是 Rust 类型检查或功能运行验收。
- 未合并索引条目 0；HEAD/MERGE_HEAD 仍为第 1 节记录；15 个并行 ACP 未暂存文件仍隔离。没有重新 merge、切换分支、提交、推送、构建、打包、测试或实际数据迁移。

下一步：第五批，对已融合模块择优复查，待并行 ACP 写入完成后以最终源码核对归档/权限修复的契约与整体融合状态；第六批才进行全局静态审查与融合提交，第七批需另行授权运行验收。

## 11. 第五批 — 已融合模块定向复查（2026-11-24）

状态：第五批定向源码复查及已发现缺陷的源码修复完成；不等同于全局类型检查、运行验收或可部署结论。本会话接续第四批后，以 WebCodex 原生工具纳入此前独立 ACP 修改，未启动 Cursor ACP/CLI；未执行测试、构建、打包、实际数据库迁移或提交/推送。

### 11.1 分域复查和改动

- Runner / Project：`runner_protocol.rs` 移除合并遗留的重复 `RunnerProjectSummary.registration_incarnation` 声明，保留强身份字段。核对注册清单的 namespace、provider 实例、重复 Run ID 及同实例 capability 降级拒绝。`runners.rs` 增加精确归属查找，区分重复与不存在，归档候选来自授权的确切 Runner 实例有限清单；归档能力不允许同实例降级。
- ACP：此前 128 个 uncertain Run 填满热清单，不能以删除或自动重放历史解除阻塞。`coding_agent.rs`、`runner_operation.rs`、Runner `coding_agent.rs`、`store.rs`、`lifecycle.rs` 与 Server `coding_agent.rs`、新增 `coding_agent_archive.rs` 统一为 Server 先持久化不可覆盖的精确路由回执，再向该 Runner 确认有限数量的身份摘要，Runner 只归档与持久记录及现场快照吻合的受保护终态；无回执拒绝腾退，按 Run ID 分片查询，不扫描所有 Runner。回执不是执行结果：重启后观察原拥有者，校验不可变身份，原拥有者离线、身份冲突或持久化失败时失败关闭。先读热清单再读回执，避免归档切换期间将已存在 Run 当作不存在。热目录持久化 archive-required-v1 标记，防止旧 Runner 忽略归档；旧 Server 无归档支持时拒绝新 Run。目录同步不确定保留现场并在下次准入修复同一归档；正常终态原有回收策略不变，uncertain 记录不自动删除。Windows 回执复用受限账号 ACL 策略，Unix 私有目录/文件权限及非覆盖原子发布。两侧历史归档与回执必须配套保留；禁止未审查的降级或单侧历史恢复。未声称全局所有 start 都有持久路由索引，也未改变原有 Server uncertain 热绑定的保留策略。
- ACP permission：`permission.rs` 兼容 ACP ToolCallUpdate 可选 title，空标题仅用于显示；重复 option ID 拒绝；活跃回调检查和发布在同一锁内，不覆盖现有回调，保留精确 token/option、会话和权限边界。现有回归源码增加 archive marker、无回执拒绝、归档后重启保留及 Server 回执不覆盖验证；没有执行测试。
- MCP：复查 `tool_definition/coding_agents.rs`、`auth/scopes.rs`、`model_surface.rs`、`mcp/tools.rs`、`tool_exposure.rs`，确认单独 Host review 的工具不能经 Gateway 绕过；permission response 需 coding_agent:run 与 project:write，live diagnose 保留执行授权边界，不改变第四批 Session commit 屏障。
- Desktop：复查 `WorkspaceContext.tsx` 的连接 key、revision/readiness epoch、active poll 与 15 秒轮询期限；Runtime 被动观察暂停不加速 Workspace 轮询，旧连接响应不覆盖新连接，授权失败清理投影。保留既有实现，本批未重复改造 Desktop。
- OAuth：`registration.rs` 与 `token.rs` 将同步动态注册事务和授权码/刷新令牌交换移至 `spawn_blocking`；保留 `responses.rs` 已有客户端认证阻塞隔离、动态注册 issuer/ingress fence、认证方法、grant 校验、原子消费/交换及 no-store 响应，不改变授权范围。
- Work Result：复查 `work_result.rs` 的组装前后 Project incarnation/root/path/client 一致性、精确 Session 再授权、`work_result_workspace.rs` 30 秒观察复用与源/目标围栏、`console_snapshot.rs` 的强注册身份；资源 URI `ui://webcodex/work-result/v40` 保持。未修改既有 UI 表现。
- Environment：复查 `runtime_entry.rs` 的 Windows service env ACL/reparse 检查、`storage.rs` 当前账号 ACL/owner 处理、`runner_preflight.rs` 精确候选可执行文件校验与超时、迁移 journal/reset 边界；新增 `protect_windows_runtime_state_path` 仅复用现有 ACL 实现供新建 ACP 回执路径使用。未执行环境迁移或修改运行中配置。

### 11.2 验收证据与接续

- `rustfmt --edition 2021 --config skip_children=true --emit stdout` 对 29 个相关 Rust 文件只读语法解析全部通过，失败 0；同范围冲突标记 0。另检索 `CodingAgentStartRequest` 构造点、归档字段及能力、MCP canonical tool 名称和 Work Result v40 引用。确认重复注册字段仅保留一个声明。上述检查不代表 Rust 类型/链接检查，也不代表 Windows ACL/持久化实际运行成功。
- 第五批修改保留在工作区，未执行 Git 暂存、提交或推送；由第六批统一核对实际融合范围、既有索引及独立 ACP 修改，隔离无关文件，完成全局静态审查和融合提交；不可将前四批 Git 证据当作当前最新核验。第六批尤其核对归档协议的权限、持久化与配套备份/恢复边界，以及未执行的类型检查相关风险。
- 第七批按现行 Windows 脚本执行构建、打包与经授权的运行验证；仍需验证 ACP 归档重启、失败恢复、旧版本拒绝、Windows ACL、OAuth 交易并发及 Desktop 轮询。现有安装版和真实数据库/Runner 状态均未改变，不直接部署。

下一步：第六批全局静态审查及融合提交，完成后再进入第七批编译打包。

## 12. 第六批 — 全局静态审查及融合提交（2026-11-24）

### 12.1 静态审查与修复

- 核对实际 HEAD 与 MERGE_HEAD，确认目标 integration 分支与原定两个父提交一致，未合并索引为空。相对官方目标逐域检查个人差异，复用前五批实际源码审查，不重启 merge。
- ACP：加强热目录/归档两处同时存在时的冲突拒绝、归档 marker 与归档根目录配套校验、目录/文件 reparse 和符号链接拒绝、实际读取大小限制；Server 不接受同 Run ID 跨 Runner 归属替换，身份包含创建时间，归档候选批量检查内存绑定，避免逐候选重复裁剪。补充 ACP 契约文档，明确 SQLite 单库备份不包括独立的 Runner 归档和 Server 回执，配套保存，禁止单侧回滚或去除 marker。不能证明任意状态丢失后的全局防重放，Windows ACL 和持久化故障仍需运行验收。
- 修复第三批离线 Session 迁移依赖清单遗漏：`workspace-boundaries.toml` 登记 Store 对 Workflow Session 的同层正常依赖，与实际迁移所有权和已有 Cargo.toml/Cargo.lock 一致，不关闭检查。检查历史旧表名仅在格式识别及迁移代码中保留，未发现旧数量淘汰常量的运行引用。
- 复查 OAuth 注册/令牌交换的 Arc 配置、owned ingress fence、阻塞隔离契约；检查 MCP ACP permission、Work Result v40 资源、Runner capability 与 StartRequest 构造点；保留第四批强身份和持久化边界。未改变用户已确认 D1/D2，未实际迁移或操作数据库。

### 12.2 静态证据

- `rustfmt --edition 2021 --config skip_children=true --emit stdout`：相对 MERGE_HEAD 有差异的 230 个现存 Rust 文件（含新增归档文件），通过 230、失败 0；本轮纳入的 20 个 Rust 文件格式化后 `--check` 通过。此为语法与格式检查，非 Rust 编译/类型验收。
- `cargo metadata --locked --offline --no-deps --format-version 1`：23 个 workspace package；静态 workspace boundary checker 重新检查通过（先发现一项遗漏，修复后 0 项违规）。
- `check_builtin_guidance_contract.py` 通过（6 targets、37 items）；`check_markdown_links.py` 通过（287 files、1549 links、0 missing）。Desktop 与 frontend 使用本机 Node 22.12.0、各自现有 TypeScript，通过 `tsc --noEmit --incremental false --pretty false`，退出码均 0；未构建或安装依赖。
- 全局源码冲突标记检索 0；未解决索引条目 0。上游原样带入的历史验收 JSON 行尾为 CRLF，索引 blob 与 MERGE_HEAD 完全一致；仅本次 Git 文本检查按 CRLF 行尾处理，不改证据、不改全局配置。
- 仅纳入两父提交已有的融合内容、前五批及第六批任务变更，排除本地 8 个无关缓存/安装包文件；已确认官方 tracked artifacts 与 MERGE_HEAD 无差异。

### 12.3 本地提交

本地融合提交成功：`12c1029994b08ef1c951e829d260b45f92e4e658`。

- 第一父提交：`9a85be368b832ec29e2b12642004b09446dca7dc`（个人基线）。
- 第二父提交：`026413dce8a7166a3ba27d8df19e321a46782532`（已选定官方 main）。
- 树对象：`35ee6908a51ebe24d314664671576e7249cce584`，与静态审查后锁定的索引一致；相对个人基线纳入 1351 个文件，已跟踪工作区无未提交变更；8 个无关未跟踪文件原样保留。
- 本节提交回执另以仅文档提交记录，不修改融合源码。第六批完成；尚未测试、编译、打包、部署、实际迁移、推送或合入 main。第七批负责构建与运行验收。