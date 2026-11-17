# 按任务相关性排列启动目录

[English](startup-task-relevance.md)

本文记录 [Issue #1257](https://github.com/yyjeqhc/webcodex/issues/1257) 的有限范围
启动选择改进，与历史六场景知识/Skill 结果及效率基准分别保留。

## 行为与确定性证据

`work_on_project` 在应用原有每类 2,900 字节预算前，按当前指令排序已经授权、已经发现的
Skill 和 Plugin metadata。不同词的名称命中优先于描述命中；稳定排序保留同分及未匹配
候选的原顺序。完整目录及来源 revision 不变，仍可通过原有提示显式发现更多条目。
排序不会增加资源正文读取、Plugin binding 或执行。

Skill 相关性先使用已发现 metadata 的完整描述（沿用最多 1,024 个字符的限制），再应用
启动显示的 512 字节裁剪和目录预算。当原显示前缀没有词法命中、但后续描述存在命中时，
启动改为展示从最早命中词或中文短语开始的原文摘录。摘录保留 UTF-8 边界，并将省略标记
计入 512 字节上限。已有命中的前缀、无命中的描述仍使用原前缀。显式 Skill 目录的完整
metadata、顺序和 revision 不变，不增加发现或正文读取。Plugin 描述已在 Runner 协议
中限制为 512 字节，此协议不变。

长描述补全增加英文和中文回归，覆盖后部 metadata 命中、Unicode 小写展开后的原文偏移、
UTF-8 边界、原前缀保留及显式目录一致性。下文历史检查和模型任务发生在本次补全之前，
不能作为长描述的新运行时或模型验收证据。

匹配复用 manifest ranker 的 ASCII 分词、英文单复数归一化和停用词。metadata 中完整的
多字中文短语可匹配指令，不进行翻译或语义检索。最多使用指令前 2,048 个字符和 64 个
不同 ASCII 词。既有 manifest ranker、查询限制和结果顺序保持原样。

回归夹具证明原有前缀投影遗漏位于后部的相关 Skill/Plugin，而排序后在相同预算内保留。
运行时测试覆盖两类目录、同一 Session 的当前任务变化、scope 拒绝、关闭发现、不新增
Plugin binding、计数、截断、提示和整体启动上限。另有未匹配/同分顺序、中文短语、
归一化及查询上限的聚焦覆盖。

长描述补全检查（2026-10-22）：

- 修复前，新端到端回归首项为 `neutral-00`，而非预期的 `late-guidance`；修复后英文和
  中文任务均通过，显式目录的 metadata、顺序及 revision 保持原样。
- `work_on_project` 80 项、`startup_catalog` 9 项、`task_relevance` 6 项通过；过滤集合
  存在重叠，后两组直接运行第一组构建出的最终 libtest 可执行文件。
- 修改的 Rust 文件格式、diff 空白及 Markdown 链接检查通过。根包 libtest 使用下文
  同样的命令行资源覆盖。这些检查证明本地确定性行为，不构成新的模型验收。

历史实现提交 `332277688f5d05a3ca482056ccc8102015933a6d` 的检查：

- `work_on_project` 79 项、`discovery` 77 项、`startup_brief` 31 项、
  `startup_catalog` 7 项通过；过滤集合存在重叠。
- Rust 格式、diff 空白及 Markdown 链接检查通过。
- 默认 feature 的干净 dogfood Server/Runner 构建通过，使用 offline/locked。
- 默认优化的根包 libtest 编译曾收到 SIGKILL。本地测试随后仅通过命令行设置
  `profile.dogfood.package.webcodex.opt-level=0` 和
  `profile.dogfood.package.webcodex.codegen-units=256` 控制编译资源；生产构建设置
  和测试断言未改。
- 新 Plugin 回归夹具起初使用未按规范词法排序的数字名称，被既有目录验证器拒绝。
  已纠正夹具，未放宽生产准入。

## 一次真实模型任务：2026-10-22

新建 Codex 子代理配置为 `gpt-6.1-sol`、medium 推理，回答当前 Quartz 发票审计流程和
审批码。业务提示没有指定 Skill、资源路径、答案或读取顺序。模型通过私有通用 API
适配器访问原隔离测试环境；凭据及单一普通 HTTP cookie jar 由适配器处理。

夹具有 25 个 Project Skill。启动返回 5 个 descriptor，相关的 `quartz-audit` 排在首位，
并明确标记截断。模型自行选择 `load_skill`，读取当前定义，正确回答两个事实，引用
`.agents/skills/zz-quartz-audit/SKILL.md` 第 6–7 行。源码字节、SHA-256、定义版本、
精确 Project、返回行范围及未变的夹具 Git 状态均已独立核对。

全部 8 次 HTTP 尝试都与干净 `332277688f5d` 的服务端 metadata 生命周期记录匹配，
属于同一个 HTTP-cookie Window：6 次成功、2 次 HTTP 400 参数拒绝。模型修正了
`list_tools` 的不支持字段，以及在 HTTP API 上传入的 MCP 专用 `_wc` 参数。原失败均
保留，不声称首次即成功。保留的原生 API ActionAudit 未建立 Session/Window 关联，
因此不把该样本称为另一套 MCP 知识收集器的严格合格结果。

运行中候选可执行文件哈希及干净 Server/Runner 构建身份均已核验。仅临时切换既有
测试运行时，应用状态及凭据与 stable 隔离。用户明确允许共用控制平面 key，但 stable/test
Tunnel IDs 必须不同。随后恢复原测试源码和配置；稳定服务及原生 Tunnel 进程身份未变。
未新建、轮换或重绑公网连接或 key。

## 限制

本次证明一个 Codex HTTP/API 任务的实际结果和资源采用；ChatGPT/MCP/Tunnel 模型验收
路径不可用，不声称通过。未独立核验后端模型身份；token、缓存、纯模型耗时、对比延迟和
成功率未知。确定性的旧前缀对照不等于真实模型配对性能实验。原始回复、凭据和私有捕获
保留在 Git 外。该记录不代表已部署到用户 stable 环境，也不代表整个 Agent 路线完成。
