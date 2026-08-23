# pi-agent 使用手册

`pi-agent`是pi的Java 25 Agent SDK实现，面向需要在JVM应用中嵌入模型、Agent、工具、session和durable execution的开发者与运维人员。本文适用于`0.1.0-SNAPSHOT`，目标上游版本为pi `0.84.2`。

SDK定位为headless JVM runtime。未来extensions使用原生Java SPI，不直接加载TypeScript扩展；TUI专属渲染、themes、custom editor和terminal overlay不在项目范围内。Skills独立遵循Agent Skills文件协议，不受该限制。

## 1. 选择模块

项目发布三个 Maven artifact：

| Artifact | 用途 |
| --- | --- |
| `pi-agent-ai` | 模型、消息、内容块、usage、stream event和`ModelStream`协议 |
| `pi-agent-core` | Agent loop、工具、队列、session、JSONL persistence和durable operations |
| `pi-agent-vertx` | Vert.x HTTP/1.1/HTTP/2 SSE transport、OpenAI Chat/Responses和model catalog |

典型OpenAI应用只需要直接依赖core和vertx；它们会传递引入ai：

```xml
<properties>
  <maven.compiler.release>25</maven.compiler.release>
  <pi-agent.version>0.1.0-SNAPSHOT</pi-agent.version>
</properties>

<dependencies>
  <dependency>
    <groupId>io.github.idoly</groupId>
    <artifactId>pi-agent-core</artifactId>
    <version>${pi-agent.version}</version>
  </dependency>
  <dependency>
    <groupId>io.github.idoly</groupId>
    <artifactId>pi-agent-vertx</artifactId>
    <version>${pi-agent.version}</version>
  </dependency>
</dependencies>
```

如果只实现自己的transport或Provider，仅依赖`pi-agent-ai`。Core不暴露Vert.x、Mutiny或Netty类型。

## 2. 快速开始

下面示例从内置catalog选择模型，使用环境变量提供API key，并执行一次prompt：

```java
import io.github.idoly.pi.agent.*;
import io.github.idoly.pi.ai.Model;
import io.github.idoly.pi.vertx.openai.*;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public final class Main {
    public static void main(String[] args) throws Exception {
        String apiKey = System.getenv("OPENAI_API_KEY");
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalStateException("OPENAI_API_KEY is required");
        }

        Model model = OpenAiModelCatalog.bundled()
                .find("openai", "gpt-4.1")
                .orElseThrow()
                .model();

        try (OpenAiModelStream modelStream = new OpenAiModelStream()) {
            AgentOptions options = new AgentOptions(
                    "You are a concise assistant.",
                    model,
                    "off",
                    "example-session",
                    modelStream,
                    null,
                    null,
                    provider -> CompletableFuture.completedFuture(apiKey),
                    List.of(),
                    ToolExecutionMode.PARALLEL,
                    QueueMode.ONE_AT_A_TIME,
                    QueueMode.ONE_AT_A_TIME,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null
            );

            Agent agent = new Agent(options);
            agent.subscribe((event, cancellation) -> {
                if (event instanceof AgentEvent.MessageEnd ended) {
                    System.out.println(ended.message());
                }
                return CompletableFuture.completedFuture(null);
            });

            agent.prompt("用三句话解释Java虚拟线程。")
                    .toCompletableFuture().join();
        }
    }
}
```

`join()`只适合CLI、测试或普通工作线程。不要在Vert.x event-loop线程中阻塞；在服务代码中应组合`CompletionStage`或转换为Mutiny `Uni`。

## 3. 模型与Provider

### 3.1 使用内置catalog

```java
OpenAiModelCatalog catalog = OpenAiModelCatalog.bundled();

List<Model> openAiModels = catalog.models("openai");
Model model = catalog.find("openai", "gpt-4.1")
        .orElseThrow(() -> new IllegalArgumentException("model not found"))
        .model();
```

Catalog固定对应上游`0.84.2`，包含模型的：

- Provider和API类型
- Base URL
- Context window和max tokens
- Text/image输入能力
- Reasoning与thinking-level mapping
- Chat/Responses兼容标志

支持的API路由包括：

```text
openai-completions
openai-responses
azure-openai-responses
openai-codex-responses
```

### 3.2 自定义Model

不在catalog中的兼容端点可以直接构造`Model`：

```java
Model model = new Model(
        "my-model",
        "My Model",
        "openai-responses",
        "my-provider",
        "https://llm.example.com/v1",
        false,
        List.of("text"),
        128_000,
        16_384
);
```

未知模型使用默认compatibility profile。端点有非标准行为时，应通过`OpenAiModelStream`的显式compatibility构造器提供profile，而不是伪造catalog模型身份。

### 3.3 API key

库不会自动读取环境变量。应用通过`ApiKeyResolver`提供key：

```java
ApiKeyResolver resolver = provider -> CompletableFuture.completedFuture(
        switch (provider) {
            case "openai" -> System.getenv("OPENAI_API_KEY");
            default -> throw new IllegalArgumentException(
                    "No API key for provider " + provider
            );
        }
);
```

不要把key写入session、日志、tool details或checkpoint。Resolver可以异步读取secret manager。

### 3.4 Transport配置

默认`OpenAiModelStream()`拥有并在`close()`时关闭内部Vert.x实例。服务已有共享Vert.x时：

```java
Vertx vertx = Vertx.vertx();
VertxSseClientOptions transportOptions = VertxSseClientOptions.DEFAULT;
VertxSseHttpClient transport = new VertxSseHttpClient(vertx, transportOptions);
OpenAiModelStream stream = new OpenAiModelStream(
        transport, new ObjectMapper(), OpenAiModelCatalog.bundled()
);
```

此构造方式下`OpenAiModelStream`不拥有transport，应用应按以下顺序关闭：

```java
stream.close();      // 不关闭共享transport
transport.close();
vertx.close();
```

`trustAll=true`会关闭TLS证书验证，只能用于受控测试环境。

## 4. 消息与内容

常用消息构造：

```java
UserMessage user = UserMessage.text(
        "分析这段代码", System.currentTimeMillis()
);

UserMessage multimodal = new UserMessage(
        List.of(
                new TextContent("描述图片"),
                new ImageContent(base64Data, "image/png")
        ),
        System.currentTimeMillis()
);
```

User message只允许text和image。Assistant message可包含text、thinking和tool call。Tool result使用`ToolResultMessage`。所有内容records均为immutable；传入collection会被防御性复制。

## 5. Agent运行方式

### 5.1 普通prompt

```java
CompletionStage<Void> run = agent.prompt("你好");
run.whenComplete((ignored, failure) -> {
    if (failure != null) failure.printStackTrace();
});
```

单个`Agent`同一时间只允许一个active run。重叠调用会返回exceptional stage。不同`Agent`实例可以并发运行。

### 5.2 读取状态

```java
AgentState state = agent.state();
System.out.println(state.messages());
System.out.println(state.streaming());
System.out.println(state.errorMessage());
```

`state()`返回当前快照。不要将它作为durable persistence；进程退出后普通Agent状态不会自动恢复。

### 5.3 取消

```java
CompletionStage<Void> running = agent.prompt("执行一个长任务");
agent.abort();
running.toCompletableFuture().join();
```

取消会传递到Provider stream和active tools。取消是协作式的，tool实现必须观察`CancellationSignal`。

### 5.4 Steering和follow-up

```java
agent.steer(UserMessage.text("先处理安全问题", System.currentTimeMillis()));
agent.followUp(UserMessage.text("然后给出测试", System.currentTimeMillis()));
```

Steering消息在当前run后续turn中优先注入；follow-up在当前工作完成后继续。`QueueMode.ALL`一次消费全部，`ONE_AT_A_TIME`逐条消费。

### 5.5 更新运行配置

Agent idle或运行期间可以更新下一步使用的模型、system prompt、thinking level和tools：

```java
agent.systemPrompt("You are a code reviewer.");
agent.model(otherModel);
agent.thinkingLevel("high");
agent.tools(List.of(tool));
```

替换完整message history和`reset()`只应在idle状态执行。

## 6. 事件

`subscribe`注册有序、awaited listener：

```java
AutoCloseable subscription = agent.subscribe((event, cancellation) -> {
    switch (event) {
        case AgentEvent.AgentStart ignored -> System.out.println("start");
        case AgentEvent.MessageUpdate update ->
                System.out.println(update.streamEvent());
        case AgentEvent.ToolExecutionStart start ->
                System.out.println("tool: " + start.toolName());
        case AgentEvent.AgentEnd end ->
                System.out.println("new messages: " + end.messages().size());
        default -> { }
    }
    return CompletableFuture.completedFuture(null);
});
```

Listener返回的stage会被等待，因此不要在listener中执行无界阻塞操作。关闭subscription后停止接收后续事件。

## 7. 工具

### 7.1 定义工具

```java
AgentTool weather = new AgentTool() {
    private final ToolDefinition definition = new ToolDefinition(
            "weather",
            "Get current weather for a city",
            Map.of(
                    "type", "object",
                    "properties", Map.of(
                            "city", Map.of("type", "string")
                    ),
                    "required", List.of("city"),
                    "additionalProperties", false
            )
    );

    @Override
    public ToolDefinition definition() {
        return definition;
    }

    @Override
    public CompletionStage<AgentToolResult> execute(
            String toolCallId,
            Map<String, Object> arguments,
            CancellationSignal cancellation,
            Consumer<AgentToolResult> onUpdate
    ) {
        cancellation.throwIfCancelled();
        String city = (String) arguments.get("city");
        return CompletableFuture.completedFuture(new AgentToolResult(
                List.of(new TextContent(city + ": 23 C, clear")),
                Map.of("source", "example")
        ));
    }
};
```

Schema使用JSON Schema map。Core在执行前进行schema validation。工具还可以：

- `prepareArguments`规范化参数
- `validateArguments`增加业务校验
- 覆盖`executionMode()`要求sequential execution
- 通过`onUpdate`发送中间结果
- 返回usage
- 设置`terminate=true`结束agent loop

### 7.2 并行与顺序

`ToolExecutionMode.PARALLEL`并发执行同一assistant消息中的tools，但tool results仍按原始tool-call顺序发布。任何tool声明`SEQUENTIAL`时，该batch顺序执行。

工具effect是否可安全重放是durable execution的重要业务判断；不要默认把有外部副作用的tool标为safe。

## 8. Session基础

普通`Agent`和`AgentSession`是两套不同层次：

- `Agent`：执行模型和工具的进程内状态机
- `AgentSession`：immutable entry tree、lanes、records、usage和durable persistence视图
- `SessionRunOperation`：把执行过程持久化到session的实验性扩展

创建JSONL session不会自动让普通`Agent.prompt()`持久化。

### 8.1 In-memory repository

```java
SessionRepository repository = new InMemorySessionRepository();
AgentSession session = repository.create(
        new SessionRepository.CreateOptions("demo", null)
).toCompletableFuture().join();
```

适用于测试和短生命周期任务，进程退出后数据丢失。

### 8.2 JSONL repository

```java
Path root = Path.of("data/sessions");
Path cwd = Path.of(".").toAbsolutePath().normalize();
JsonlSessionRepository repository = new JsonlSessionRepository(root, cwd);

AgentSession session = repository.create(
        new SessionRepository.CreateOptions("demo", null)
).toCompletableFuture().join();
```

JSONL使用上游record-based v4格式。每次mutation占一个物理行。Entry、record、usage、lane和fact共享sequence与ID namespace。

### 8.3 打开和列出

```java
List<SessionMetadata> sessions = repository.list()
        .toCompletableFuture().join();
AgentSession reopened = repository.open(sessions.getFirst())
        .toCompletableFuture().join();
```

不要长期缓存JSONL handle并假设它跨进程自动合并。其他进程提交后，stale handle会以`SessionError.Code.STORAGE`失败；重新`list/open`获取新handle。

### 8.4 Entry和transaction

```java
String entryId = session.appendMessage(
        UserMessage.text("hello", System.currentTimeMillis())
).toCompletableFuture().join();

session.transaction(tx -> {
    tx.append(new SessionEntryDraft.Message(
            "message-2",
            UserMessage.text("atomic", System.currentTimeMillis())
    ));
    tx.name("renamed");
    return null;
}).toCompletableFuture().join();
```

Transaction禁止嵌套。验证失败或publication失败会完整rollback，不消费sequence。

### 8.5 Lanes与branch

```java
session.createLane("experiment", entryId).toCompletableFuture().join();
AgentSession experiment = session.view("experiment");
experiment.appendMessage(UserMessage.text(
        "alternative", System.currentTimeMillis()
)).toCompletableFuture().join();
```

Lane view共享同一handle lifecycle；关闭其中一个会关闭该handle下的共享views。独立`repository.open(...)`得到独立lifecycle。

### 8.6 查询和context

```java
List<SessionEntry> entries = session.findEntries().toCompletableFuture().join();
List<SessionEntry> branch = session.findEntriesOnBranch(
        SessionBranchQuery.current()
).toCompletableFuture().join();
SessionContext context = session.context().toCompletableFuture().join();
SessionStats stats = session.stats().toCompletableFuture().join();
```

## 9. Durable run

以下API带`@ExperimentalSessionApi`，不属于上游`AgentHarness 0.84.2`公开执行契约。

```java
AgentLoopConfig config = new AgentLoopConfig(
        model,
        "off",
        "durable-session",
        modelStream,
        null,
        null,
        provider -> CompletableFuture.completedFuture(apiKey),
        ToolExecutionMode.PARALLEL,
        null,
        null,
        null,
        null,
        null,
        null
);
SessionRunOperation.Options options = new SessionRunOperation.Options(
        "You are a durable assistant.",
        config,
        List.of(weather)
);

SessionRunOperation.Outcome outcome = SessionRunOperation.run(
        session,
        List.of(UserMessage.text("Weather in Shanghai?", System.currentTimeMillis())),
        options
).toCompletableFuture().join();
```

Outcome包括：

| Outcome | 含义 |
| --- | --- |
| `Completed` | Assistant完成且没有待执行tool |
| `ToolsPending` | Assistant已持久化tool calls，等待durable tool execution |
| `Terminated` | Tool或hook要求终止 |
| `Aborted` | Durable abort完成settlement |
| `Failed` | Operation以durable error结束 |

如果使用真实Provider，`AgentLoopConfig`需要API key resolver。可构造完整config，或在应用层封装一个config factory。

### 9.1 Resume

进程崩溃后重新打开session：

```java
SessionRunOperation.Outcome recovered = SessionRunOperation.resume(
        reopened,
        new SessionRunOperation.RecoveryOptions(options, 3)
).toCompletableFuture().join();
```

未知assistant effect不会假定未执行；恢复会创建later-numbered attempt。`maxAttempts`限制恢复尝试数。

### 9.2 Durable abort

```java
boolean created = SessionRunOperation.requestAbort(session, runId)
        .toCompletableFuture().join();
```

Abort record是authority。Marker、WatchService和external notifier只用于加速跨JVM active cancellation。

## 10. Durable tool execution

当run返回`ToolsPending`：

```java
if (outcome instanceof SessionRunOperation.Outcome.ToolsPending pending) {
    SessionToolExecution.Options toolOptions = new SessionToolExecution.Options(
            Map.of(
                    "read_only_tool", SessionRecordDraft.Replay.SAFE,
                    "charge_card", SessionRecordDraft.Replay.NEVER
            ),
            Clock.systemUTC(),
            ToolExecutionMode.PARALLEL
    );

    SessionToolExecution.Outcome tools = SessionToolExecution.execute(
            session,
            pending.runId(),
            pending.assistantEntry().id(),
            List.of(weather),
            toolOptions
    ).toCompletableFuture().join();
}
```

Replay规则：

- `SAFE`：effect owner崩溃且没有durable result时可重新执行
- `NEVER`：effect可能已发生时暂停，必须人工或业务系统仲裁

对`NEVER`使用：

```java
SessionToolExecution.resolveNever(
        session,
        runId,
        assistantEntryId,
        toolIndex,
        authoritativeResult,
        false,
        toolOptions
).toCompletableFuture().join();
```

不要调用`execute()`绕过已有durable starts；恢复路径使用`resume()`。

## 11. Durable queues

Queue类型：

```text
STEER
FOLLOW_UP
NEXT_RUN
```

运行中的steer/follow-up需要run ID：

```java
SessionRunQueue.Pending pending = SessionRunQueue.enqueueMessage(
        session,
        SessionRecordDraft.Queue.STEER,
        runId,
        UserMessage.text("focus on security", System.currentTimeMillis())
).toCompletableFuture().join();
```

查询与取消：

```java
List<SessionRunQueue.Pending> queued = SessionRunQueue.pending(
        session, SessionRecordDraft.Queue.STEER, runId, 100
).toCompletableFuture().join();

SessionRunQueue.cancel(
        session, SessionRecordDraft.Queue.STEER, runId, pending.target().id()
).toCompletableFuture().join();
```

`NEXT_RUN`不绑定当前run，由`SessionRunOperation.runNext(...)`原子claim。

## 12. Recovery inspection

单个session：

```java
List<SessionOperationInspector.OpenOperation> open =
        SessionOperationInspector.inspect(session)
                .toCompletableFuture().join();
```

Inspector会报告：

- Open run/navigation/compaction
- Assistant attempts
- Unresolved SAFE/NEVER tools
- 推荐recovery action
- Suspended或aborting状态

Repository范围：

```java
JsonlSessionRepository.RecoveryReport report = repository.inspectRecovery()
        .toCompletableFuture().join();
```

大型repository使用`inspectRecoveryBatch(...)`，不要一次保留所有details。Corrupt generation独立报告，不阻断其他generation检查。

## 13. Recovery checkpoint

Checkpoint是per-generation consistency inventory，不是repository-wide同时点snapshot。

### 13.1 Capture

Destination必须位于sessions root之外：

```java
Path manifest = Path.of("data/audit/checkpoint.json");
JsonlSessionRepository.RecoveryCheckpoint checkpoint =
        repository.createRecoveryCheckpoint(manifest)
                .toCompletableFuture().join();
```

Manifest记录relative path、session ID、tail sequence、byte size和SHA-256。Malformed generation也会纳入inventory。

### 13.2 简单验证

```java
JsonlSessionRepository.RecoveryCheckpointReport report =
        repository.verifyRecoveryCheckpoint(checkpoint, 100)
                .toCompletableFuture().join();
```

状态含义：

- `UNCHANGED`：fingerprint一致
- `CHANGED`：同generation path内容变化
- `MISSING`：checkpoint中的generation已不存在
- `ADDED`：capture后发现新generation

Counts始终覆盖本次验证范围；limit只限制保留的drift details。

### 13.3 自动分页与恢复

```java
RecoveryCheckpointVerifier.Options options =
        new RecoveryCheckpointVerifier.Options(100, 50);

RecoveryCheckpointVerifier.Result result = RecoveryCheckpointVerifier.verify(
        repository,
        checkpoint,
        options,
        detail -> writeDrift(detail),
        state -> persistVerifierState(state)
).toCompletableFuture().join();
```

重启后：

```java
RecoveryCheckpointVerifier.State state = loadVerifierState();
RecoveryCheckpointVerifier.Result result = RecoveryCheckpointVerifier.resume(
        repository, checkpoint, options, state,
        detail -> writeDrift(detail),
        next -> persistVerifierState(next)
).toCompletableFuture().join();
```

Progress state包含scan cursor、detail cursor、累计counts、inspected generations和terminal bit。

Detail delivery是at-least-once：如果外部副作用完成后、state持久化前进程退出，最后一页可能重送。使用以下内容作为幂等键，或将副作用与state原子提交：

```text
relativePath + status + expected fingerprint + current fingerprint
```

## 14. Maintenance与cleanup

```java
JsonlSessionRepository.MaintenanceReport maintenance =
        repository.inspectMaintenance().toCompletableFuture().join();
```

可识别：

- Final JSONL sessions
- Session locks
- Session-ID locks
- Operation locks
- Abort markers
- Staging files

清理staging：

```java
repository.cleanupOrphanedStaging(
        new JsonlSessionRepository.StagingCleanupPolicy(Duration.ofHours(1))
).toCompletableFuture().join();
```

清理无关联abort signals：

```java
repository.cleanupUnassociatedOperationSignals(
        new JsonlSessionRepository.OperationSignalCleanupPolicy(
                Duration.ofHours(24)
        )
).toCompletableFuture().join();
```

Lock files不会自动删除。它们同时承担稳定inode互斥和generation tombstone职责；不要用通用临时文件清理脚本删除`.lock`。

## 15. Operation observation

Events不是durability authority。重连时先capture durable snapshot，再消费event suffix：

```java
SessionOperationEventBus events = new SessionOperationEventBus();
try (SessionOperationEventBus.WatchHandle<List<SessionOperationInspector.OpenOperation>> watch =
             events.watch(
                     () -> SessionOperationInspector.inspect(session)
                             .toCompletableFuture().join(),
                     1024
             )) {
    renderSnapshot(watch.snapshot());
    watch.start(this::handleOperationEvent);
}
```

Registration window buffer有界。Overflow时`start()`抛出`SessionWatchOverflowException`并自动退订；重新capture durable snapshot，不要继续使用有缺口的suffix。

## 16. Cross-JVM abort通知

默认模式是`POLLING`。可选择：

```java
JsonlSessionRepository repository = new JsonlSessionRepository(
        root,
        cwd,
        Clock.systemUTC(),
        SessionIdGenerator.uuidV7(),
        notifier,
        JsonlSessionRepository.MarkerObservationMode.WATCH_SERVICE
);
```

模式：

| 模式 | 行为 |
| --- | --- |
| `POLLING` | Virtual thread变化驱动轮询marker |
| `WATCH_SERVICE` | WatchService create/modify，失败自动fallback polling |
| `DISABLED` | 不启动后台marker observer，仍保留registration-time fence |

`JsonlOperationAbortNotifier.Sequenced`适合Redis、Kafka或数据库通知实现，可保留committed sequence。通知必须是被动加速：publish、observe或callback失败不能使已提交abort失效。

诊断：

```java
var diagnostics = repository.markerObservationDiagnostics();
System.out.println(diagnostics.watchServiceFallbacks());
System.out.println(diagnostics.durableAdvisoriesRejected());
```

这些counter仅为当前repository实例的进程内单调统计，不是durable状态。

## 17. 错误处理

异步错误通过exceptional `CompletionStage`返回：

```java
try {
    operation.toCompletableFuture().join();
} catch (CompletionException failure) {
    Throwable cause = failure.getCause();
    if (cause instanceof SessionError error) {
        switch (error.code()) {
            case NOT_FOUND -> { /* 资源不存在 */ }
            case ALREADY_EXISTS -> { /* ID冲突 */ }
            case STORAGE -> { /* reopen或报告存储故障 */ }
            case INVALID_PAYLOAD, INVALID_QUERY -> { /* 调用参数错误 */ }
            default -> { /* 按应用策略处理 */ }
        }
    }
}
```

`RecordLogCorruption`表示durable log违反已知协议，不应通过盲目retry修复。先停止effect dispatch，保留原始文件并运行recovery inspection。

## 18. 生产部署检查表

- 使用JDK 25。
- API key来自secret manager或环境，不写入session。
- 复用Vert.x和HTTP connection pool。
- 不在event loop中调用`join()`。
- 为每个Agent限制单个active run。
- Tool观察cancellation并设置正确SAFE/NEVER策略。
- JSONL root位于支持稳定file lock和atomic move的本地或受验证filesystem。
- 多JVM共享root前先运行独立进程lease测试。
- 不删除`.lock` tombstones。
- 定期运行recovery batch和checkpoint verification。
- WatchService部署查看fallback diagnostics。
- Consumer按durable snapshot重连，不依赖events补历史。
- 备份需求若要求全repository同时点，使用filesystem snapshot；当前checkpoint不提供该保证。
- `AgentHarness`仍是上游`0.84.2` scaffold，durable session APIs是Java experimental extensions。

## 19. 相关文档

- [架构](architecture.md)
- [API稳定度](api-stability.md)
- [运维测试](operations-testing.md)
- [Global write barrier决策](global-write-barrier.md)
- [发布](releasing.md)
- [0.1.0 release notes](release-notes-0.1.0.md)
