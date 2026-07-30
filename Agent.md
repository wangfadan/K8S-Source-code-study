# Agent.md

每次开新会话，先读本文件恢复上下文。

## 学习目标
client-go → operator → k8s 源码。
参考教程：<https://cit-3.gitbook.io>
本地源码：`/Users/user/study/kubernetes-1.30/`
笔记目录：`/Users/user/study/K8S-Source-code-study/`
client-go 源码：`/Users/user/study/client-go/`（顶层独立仓库，go.mod 是 `k8s.io/client-go`）

## 笔记写入约定
- 默认写到本 `Agent.md` 的对应章节里，不主动拆 `.md` 文件
- 只有你明确说"写文件"或"拆出去"，才新建单独的 `.md`
- 等一节内容稳定、你说了"开下一节"再统一清理/归档

## markdown 写作标准
参考 `kubectl工作流程.md`：

1. 一级标题写主题；二级标题写小节
2. 正文以编号列表（1./2./3.）展开，每条独立成段
3. 代码 / 配置用围栏代码块
4. 关键流程用 ASCII 图，不依赖 mermaid
5. 重要更正用 `> ` 引用块单独标注
6. 不写表格、不用 emoji 装饰
7. 章节末可加 `## xxx 视角下值得记一下的几点`

## 进度

| 阶段 | 状态 |
|---|---|
| kubectl → kubelet 流程 | ✅ |
| client-go 第 1 节（介绍） | 🔄 进行中 |
| client-go 第 2~8 节 | ⬜ |
| operator | ⬜ |
| scheduler | ⬜ |
| controller-manager | ⬜ |
| apiserver | ⬜ |
| kubelet 源码专章 | ⬜ |

## client-go 章节规划
来源：<https://cit-3.gitbook.io/client-go>

1. client-go 介绍 ← 当前
2. Informer
3. 编写一个 controller
4. Reflector
5. Indexer
6. DeltaFIFO
7. workqueue
8. sharedProcessor

源码定位：`/Users/user/study/client-go/tools/cache/`（`shared_informer.go` / `controller.go` / `reflector.go` / `indexer.go` / `delta_fifo.go`）；`/Users/user/study/client-go/util/workqueue/`。

## 后续阶段
- operator：controller-runtime
- scheduler：Schedule Framework、源码走读
- controller-manager：deployment / replicaset / statefulset / daemonset / endpoint / gc
- apiserver：scheme、认证、鉴权、admission、序列化
- kubelet：Pod 创建、PLEG、probes、eviction、statusManager
- kubeproxy / cloud-controller-manager

---

# client-go 第 1 节：介绍（client-go 目录与整体定位）

## client-go 是什么

1. client-go 是 k8s 官方 Go 语言客户端库，模块名 `k8s.io/client-go`，独立于 `kubernetes/` 主仓库发布
2. kubectl 本质就是 client-go 的一个消费者：kubectl 拼 HTTP 请求、构造 runtime object、对接 auth 的代码，全在 client-go 里
3. 集群内所有 controller（deployment / replicaset / endpoint / 自定义 operator）也都基于 client-go 写
4. 读 client-go = 同时搞懂 kubectl 的内部 + controller 的基础

## 版本对应

1. k8s ≥ 1.17 后，client-go 用 `v0.x.y` 标签，对应 k8s `v1.x.y`：`v0.30.x` ↔ `v1.30.x`
2. 你本地 `/Users/user/study/client-go` 没有 git tag 输出（已确认），go.mod 里 module 声明就是 `k8s.io/client-go`，go 1.22.0；与 `kubernetes-1.30/staging/src/k8s.io/client-go`（go.work 里有引用，但目录缺，应该是 vendor/symlink 关系）配套
3. 引入方式：`go get k8s.io/client-go@v0.30.x`，或本地 replace 指到 `../client-go`

## 顶层目录速览（核心）

1. `kubernetes/` — typed clientset（每个 GVR 一个版本化客户端），代码 100% 由 `code-generator` 生成
2. `dynamic/` — 非结构化客户端，按 `unstructured.Unstructured` + GVR 直接调 apiserver，不走代码生成
3. `metadata/` — 只关心 metadata（labels / annotations / finalizers），不动 spec/status；轻量
4. `applyconfigurations/` — SSA（server-side apply）专用类型，2xx 风格的字段构造器
5. `rest/` — 跟 apiserver 通信的最底层：`Config`、`Client`、`Request`、`Transport`、`Watcher` 全在这
6. `transport/` — round trippers 链：认证、TLS、限流、token、cert rotation、spdy、websocket
7. `tools/clientcmd/` — kubeconfig 加载与合并（`--kubeconfig` / `$KUBECONFIG` / `~/.kube/config`）
8. `tools/auth/` — exec / plugin 认证插件协议
9. `discovery/` — 发现 apiserver 支持哪些 GVR；`discovery/cached` 带本地缓存
10. `restmapper/` — 把 GVR ↔ REST 资源映射起来；kubectl 用的就是这个
11. `scale/` — 通用 scale 子资源客户端（`Scale` Get/Update，不依赖 typed clientset）
12. `openapi/` / `openapi3/` — apiserver OpenAPI v2/v3 schema 缓存（kubectl explain / kubectl validation 都靠它）
13. `informers/` — 每种资源一个 SharedInformerFactory，按 group/version 分目录
14. `listers/` — 只读缓存读取接口，跟 informers 配套：`Lister.Namespace(name).Get(name)`
15. `tools/cache/` — **核心**：Reflector / DeltaFIFO / Indexer / Store / Controller / SharedInformer 全在这
16. `tools/workqueue/`（实际在 `util/workqueue/`）— 限速队列、延迟队列；controller 标配
17. `tools/leaderelection/` — leader election
18. `tools/record/` — EventRecorder
19. `tools/pager/` — 列表分页（`--chunk-size` 行为）
20. `tools/portforward/` / `tools/remotecommand/` — kubectl port-forward / exec 的客户端部分
21. `tools/events/` — Events API v1 封装
22. `tools/reference/` — `GetReference` 工具，用于 OwnerReference 构造
23. `tools/metrics/` — client-go 自带的 metrics provider
24. `util/` — 杂项工具：`cert` / `csaupgrade` / `flowcontrol` / `homedir` / `jsonpath` / `retry`
25. `plugin/pkg/` — 插件协议
26. `testing/` — fake client（`fake.NewSimpleClientset()`）、fake watcher
27. `examples/` — 8 个可直接跑的示例：in-cluster / out-of-cluster / fake-client / leader-election / workqueue / crud / dynamic-crud
28. `pkg/version/` — 编译时注入的版本信息（`Info{ Major, Minor, GitVersion, BuildDate }`）

## 整体分层（自底向上）

```
apiserver (HTTP/JSON)
   ▲
rest/        ← 最低层：构造 Request、发 HTTP、解析响应、watch
   ▲
transport/   ← round trippers：auth、TLS、限流、retry
   ▲
clientset & dynamic & metadata & scale & applyconfigurations
   ▲
discovery & restmapper & openapi   ← kubectl 解析用
   ▲
informer & lister & workqueue     ← controller 用
   ▲
你的 controller / operator
```

## kubectl 用 client-go 的那条线

1. `main.go` 入口 → `kubectl.NewKubectlCommand`
2. `factory` 构造：`tools/clientcmd` 加载 kubeconfig → `rest.Config`
3. `kubernetes.NewForConfig`（typed）或 `dynamic.NewForConfig`（unstructured）拿到 client
4. 走 `discovery` + `restmapper` 找到 GVR 对应的 REST 路径
5. `rest.RESTClient` 拼 Request → 经 `transport` 链发出

## controller 用 client-go 的那条线

1. `cmd/<controller>/app` 启动
2. `clientcmd` 加载 kubeconfig → `rest.Config`
3. `kubernetes.NewForConfig` + `informers.NewSharedInformerFactory` + `tools/cache.NewSharedInformer`（老式）或直接 `AddEventHandler`
4. 起 controller：`workqueue.NewRateLimitingQueue` + `cache.NewIndexer`（informer 自带）
5. `Run(workers, stopCh)` → Reflector list/watch → DeltaFIFO → Indexer → 回调入队 → worker 处理

## 1.30 视角下值得记一下的几点

> kubectl 的 `--cache-dir` 默认 `~/.kube/cache`（discovery + openapi 缓存），重启 kubectl 时复用能省一次 GET。1.30 起 aggregated discovery 默认开启（`discovery/aggregated_discovery.go`），apiserver 单次返回所有 GVR，kubectl 启动更快。

> typed clientset 是代码生成的，**不要手改** `kubernetes/typed/...`。要看真实定义，去 `pkg/apis/...` 的 Go struct。

> watch 不是长连接拉取，是 HTTP/2 chunked + 序列化 JSON（见 `rest/watch/`）。1.30 的 watch 在 chunked 之外还引入了 `watchlist` 优化（`reflector_watchlist.go`），减少大列表初次同步成本。

## 下一步

- 进入 1.02 Informer：先看 `tools/cache/shared_informer.go` 顶层接口，再看 `tools/cache/controller.go` 把 informer 跑起来的入口