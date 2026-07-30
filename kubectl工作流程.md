# kubectl 背后的工作原理
## 客户端提交命令
kubectl create deployment nginx --image=nginx --replicas=3

1.首先，当我们敲下回车键执行命令后， Kubectl 会执行客户端验证，以确保非法的请求（例如，创建不支持的资源或使用格式错误的镜像名称）快速失败，并不会发送给 kube-apiserver。

2.验证通过后， kubectl 开始构造它将发送给 kube-apiserver 的 HTTP 请求。为了构造 HTTP 请求， Kubectl 使用称为 generators 的东西，这是一个负责序列化的抽象概念。

3.kubectl 生成运行时对象之后，它开始为它查找合适的 API Group 和版本，然后组装一个知道该资源的各种 REST 语义的版本化客户端。Kubectl 还将 OpenAPI scheme 缓存到 ~/.kube/cache/discovery 目录

4.为了成功发送请求， Kubectl 需要先进行身份验证。用户凭据一般存储在 kubeconfig 文件中，但该文件可以存储在不同的位置。为了定位到它， Kubectl 执行以下操作：
- 如果指定参数 --kubeconfig，那么采用该值；
- 如果指定环境变量 $KUBECONFIG，那么采用该值；
- 否则查看默认的目录，如 ~/.kube，并使用找到的第一个文件。

5.最后一步才是真正地发送 HTTP 请求。一旦请求获得成功的响应， Kubectl 将会根据所需的输出格式打印 success message。

## Authentication
1.kube-apiserver 是客户端和系统组件用来持久化和检索集群状态的主要接口。为了执行其功能，它需要能够验证请求是否合法。此过程称为认证 （Authentication）。

2.kube-apiserver 处理授权的方式与身份验证非常相似：基于 CLI 参数 输入，汇集一系列 authorizer， 这些 authorizer 将针对每个传入请求运行。如果所有 authorizer 都拒绝该请求，则该请求将导致 Forbidden 响应并且不再继续。如果单个 authorizer 批准，则请求继续。

3.虽然 Authorization 的重点是回答用户是否具有权限，但是 Admission Controllers 仍会拦截该请求，以确保其符合集群的更广泛期望和规则。它们是对象持久化到 etcd 之前的最后一个堡垒，因此它们封装了剩余的系统检查以确保操作不会产生意外或负面结果。

4.kube-apiserver 将反序列化 HTTP 请求，构造运行时对象（runtime object）（有点像 kubectl generator 的逆过程），并将它们持久化到 etcd。

## Deployment Controller
1.将 Deployment 存储到 etcd 后，我们通过 kube-apiserver 可以看到它。当这个新资源可用时， Deployment Controller 会检测到它，它的工作是监听 Deployment 的更改。

2.当我们的 Deployment 首次可用时，将执行此回调函数，并将该对象添加到内部工作队列（internal work queue）。当它处理我们的 Deployment 对象时，控制器将检查我们的 Deployment 并意识到没有与之关联的 ReplicaSet 或 Pod。
它通过使用标签选择器 (label selectors) 查询 kube-apiserver 来实现此功能。有趣的是，这个同步过程是状态不可知的。另外，它以相同的方式调谐新对象和已存在的对象。

3.在意识到没有与其关联的 ReplicaSet 或 Pod 后，Deployment Controller 就会开始执行弹性伸缩流程 (scaling process)。它通过推出（例如，创建）一个 ReplicaSet， 为其分配 label selector 并将其版本号设置为 1。

4.ReplicaSet 的 PodSpec 字段是从 Deployment 的 manifest 以及其他相关元数据中复制而来。有时 Deployment 在此之后也需要更新（例如，如果设置了 process deadline）。

当完成以上步骤之后，该 Deployment 的 status 就会被更新，然后重新进入与之前相同的循环，等待 Deployment 与期望的状态相匹配。由于 Deployment Controller 只关心 ReplicaSet， 因此调谐过程将由 ReplicaSet Controller 继续。

## ReplicaSet Controller
1.在上一步中，Deployment Controller 创建了属于该 Deployment 的第一个 ReplicaSet， 但仍然没有创建 Pod。 所以这里我们需要引入 ReplicaSet Controller！

2.当创建 ReplicaSet 时（由 Deployment Controller 创建），ReplicaSet Controller 会检查新 ReplicaSet 的状态，并意识到现有状态与期望状态之间存在偏差。然后，它试图通过调整 pod 的副本数来调谐这种状态。
用指数级的小批次探测，把"批量失败"的风险限制在最小范围内，避免拖垮 kube-apiserver。

3.Kubernetes 通过 Owner References （子资源的某个字段中引用其父资源的 ID） 来执行严格的资源对象层级结构。这确保了一旦 Controller 管理的资源被删除（级联删除），子资源就会被垃圾收集器删除，同时还为父资源提供了一种有效的方式来避免他们竞争同一个子资源（想象两对父母认为他们拥有同一个孩子的场景）。

4.有时系统中也会出现孤儿 （orphaned） 资源，通常由以下两种途径产生：
- 父资源被删除，但子资源没有被删除
- 垃圾收集策略禁止删除子资源

## Scheduler
1.当所有的 Controller 正常运行后，etcd 中就会保存一个 Deployment、一个 ReplicaSet 和 三个 Pod， 并且可以通过 kube-apiserver 查看到。然而，这些 Pod 还处于 Pending 状态，因为它们还没有被调度到集群中合适的 Node 上。最终解决这个问题的 Controller 是 Scheduler。

2.Scheduler 作为一个独立的组件运行在集群控制平面上，工作方式与其他 Controller 相同：监听事件并调谐状态。具体来说， Scheduler 的作用是过滤 PodSpec 中 NodeName 字段为空的 Pod 并尝试将其调度到合适的节点。



## kubelet运行容器（K8s 1.30）

Kubelet 服务进程处理 Pod 与 Container Runtime 之间所有的转换逻辑，包括挂载卷、容器日志、垃圾回收等操作。

> **更正一个常见说法**：并不是「Kubelet 每隔 20 秒向 kube-apiserver 查询 Pod 列表」。在 1.30 里，Kubelet 与 apiserver 之间是 **Watch 事件驱动**：用 `spec.nodeName=<本节点>` 字段选择器订阅本节点 Pod 的 ADD/UPDATE/DELETE 事件，再叠加一个 `SyncFrequency=1m` 的周期对账作为兜底。所谓的「20 秒」是 `FileCheckFrequency=20s`，是 file/http 配置源的检查周期，与 apiserver 拉取无关。

我们来看看同步过程是什么样的。Kubelet 启动后进入主循环 `syncLoop`，通过几类通道把期望状态收敛到运行时：

```
┌─────────────────────────────────────────────────────────────────────┐
│                       syncLoop（主循环）                            │
└─────────────────────────────────────────────────────────────────────┘
   configCh   ← apiserver watch / file / http 三源合并
   plegCh     ← PLEG（容器运行时状态变化）
   syncCh     ← 1m 周期对账
   housekeepingCh ← 2s 容器 GC
                       │
                       ▼
            podWorkers.UpdatePod
            （每个 Pod 一个独立 goroutine）
                       │
                       ▼
                  kl.SyncPod
```

需要注意：PLEG 事件**并不直接**调 SyncPod，而是经 `HandlePodSyncs → podWorkers.UpdatePod → pod worker goroutine → kl.SyncPod`，中间隔着 pod worker。Pod 到节点后还会过一次 **kubelet 自己的 admission**（很多人会遗漏这一点）—— 硬准入失败 pod 直接不入队，软准入失败只 kill 容器。

`kl.SyncPod` 是一段事务脚本，源码里就叫 "transaction script"。它把期望状态与运行时状态对比，按需补齐差异：大体上依次处理 `generateAPIPodStatus` → 软准入 → 设置 PodStatus → 注册 secret/configmap → 创建 Pod cgroup（`pcm.EnsureExists`，挂到 QoS 层级）→ 建数据目录 `/var/run/kubelet/pods/<id>/{volumes,plugins}` → `volumeManager.WaitForAttachAndMount`（只是阻塞 gate，**实际 mount 由 reconciler 异步做**）→ 取 pull secrets → 注册探针 → 调用 `containerRuntime.SyncPod`。后面就交给 CRI 那一层了。

运行时真实状态会持续变化，kubelet 通过三类机制感知收敛：

- **PodConfig**：watch 事件触发 `configCh`；
- **PLEG（Pod Lifecycle Event Generator）**：GenericPLEG 每 1s 调 CRI 列出容器，与上次快照比对，产出 `ContainerStarted/Died/Removed/Changed` 事件；1.30 起 `EventedPLEG` 进入 beta（KEP-3386），改为订阅 CRI runtime events；
- **statusManager**：把 PodStatus 异步批 PATCH 到 apiserver（10s ticker + readiness 触发即推，按 version 去重，不是实时）。

## CRI and pause container

到了这个阶段，大量的初始化工作都已经完成，容器已经准备好开始启动了，而容器是由容器运行时（例如 Docker）启动的。

为了更具可扩展性， Kubelet 使用 CRI （Container Runtime Interface） 来与具体的容器运行时进行交互。简而言之， CRI 提供了 Kubelet 和特定容器运行时实现之间的抽象。通过 protocol buffers（一种更快的 JSON） 和 gRPC API（一种非常适合执行 Kubernetes 操作的API）进行通信。

这是一个非常酷的想法，因为通过在 Kubelet 和容器运行时之间使用已定义的接口约定，容器编排的实际实现细节变得无关紧要。重要的是接口约定。这允许以最小的开销添加新的容器运行时，因为没有核心 Kubernetes 代码需要更改！

回到部署我们的容器，当一个 Pod 首次启动时， Kubelet 调用 RunPodSandbox 远程过程命令 （remote procedure command RPC）。沙箱 （sandbox） 是描述一组容器的 CRI 术语，在 Kubernetes 中对应的是 Pod。这个术语是故意模糊的，因此其他不使用容器的运行时，不会失去其意义（想象一个基于 hypervisor 的运行时，沙箱可能指的是 VM）。

在我们的例子里，使用的是 containerd 或 CRI-O。在 containerd / CRI-O 中，创建沙箱涉及创建 pause 容器。pause 容器像 Pod 中的所有其他容器的父级一样，因为它承载了工作负载容器最终将使用的许多 Pod 级资源。这些"资源"是 Linux Namespaces (IPC，Network，PID)。

如果你不熟悉容器在 Linux 中的工作方式，那么我们快速回顾一下。 Linux 内核具有 Namespace 的概念，允许主机操作系统分割出一组专用资源（例如 CPU 或内存）并将其提供给一个进程，就好像它是世界上唯一使用它们的东西一样。 Cgroup 在这里也很重要，因为它们是 Linux 管理资源隔离的方式。 Docker 使用这两个内核功能来托管一个保证资源强制隔离的进程。更多信息，可深入阅读 What even is a Container?

pause 容器提供了一种托管所有这些 Namespaces 的方法，并允许子容器共享它们。通过成为同一 Network Namespace 的一部分，一个好处是同一个 Pod 中的容器可以使用 localhost 相互访问。

pause 容器的第二个好处与 PID Namespace 有关。在这些 Namespace 中，进程形成一个分层树（hierarchical tree），顶部的"init" 进程负责"收获"僵尸进程。更多信息，请深入阅读 great blog post。

创建 pause 容器后，将开始检查磁盘状态然后启动主容器。

## CNI and pod networking

现在，我们的 Pod 有了基本的骨架：一个 pause 容器，它托管所有 Namespaces 以允许 Pod 间通信。但容器网络如何运作以及建立的？

当 Kubelet 为 Pod 设置网络时，它将任务委托给 CNI (Container Network Interface) 插件。其运行方式与 Container Runtime Interface 类似。简而言之， CNI 是一种抽象，允许不同的网络提供商对容器使用不同的网络实现。

Kubelet 通过 stdin 将 JSON 数据（配置文件位于 /etc/cni/net.d 中）传输到相关的 CNI 二进制文件（位于 /opt/cni/bin） 中与之交互。下面是一个简单的示例 JSON 配置文件：

```json
{
  "cniVersion": "0.3.1",
  "name": "bridge",
  "type": "bridge",
  "bridge": "cnio0",
  "isGateway": true,
  "ipMasq": true,
  "ipam": {
    "type": "host-local",
    "ranges": [
      [
        {
          "subnet": "${POD_CIDR}",
          "routes": [
            { "dst": "0.0.0.0/0" }
          ]
        }
      ]
    ]
  }
}
```
CNI 插件还可以通过 CNI_ARGS 环境变量为 Pod 指定其他的元数据，包括 Pod Name 和 Namespace。

接下来会发生什么取决于 CNI 插件，这里，我们以 bridge CNI 插件为例：

该插件首先会在 Root Network Namespace（也就是宿主机的 Network Namespace） 中设置本地 Linux 网桥，以便为该主机上的所有容器提供网络服务；

然后它会将一个网络接口 （veth 设备对的一端）插入到 pause 容器的 Network Namespace 中，并将另一端连接到网桥上。你可以这样来理解 veth 设备对：它就像一根很长的管道，一端连接到容器，一端连接到 Root Network Namespace 中，允许数据包在中间传输；

然后它会为 pause 容器的网络接口分配一个 IP 并设置相应的路由，于是 Pod 就有了自己的 IP。IP 的分配是由 JSON 配置文件中指定的 IPAM Plugin 实现的；

IPAM Plugin 的工作方式和 CNI 插件类似：通过二进制文件调用并具有标准化的接口，每一个 IPAM Plugin 都必须要确定容器网络接口的 IP、子网以及网关和路由，并将信息返回给 CNI 插件。最常见的 IPAM Plugin 称为 host-local，它从预定义的一组地址池为容器分配 IP 地址。它将相关信息保存在主机的文件系统中，从而确保了单个主机上每个容器 IP 地址的唯一性。

对于 DNS， Kubelet 将为 CNI 插件指定 Kubernetes 集群内部 DNS 服务器 IP 地址，确保正确设置容器的 resolv.conf 文件。

## Inter-host networking

到目前为止，我们已经描述了容器如何与宿主机进行通信，但跨主机之间的容器如何通信呢？

通常情况下， Kubernetes 使用 Overlay 网络来进行跨主机容器通信，这是一种动态同步多个主机间路由的方法。一个较常用的 Overlay 网络插件是 flannel，它提供了跨节点的三层网络。

flannel 不会管容器与宿主机之间的通信（这是 CNI 插件的职责），但它对主机间的流量传输负责。为此，它为主机选择一个子网并将其注册到 etcd。然后，它保留集群路由的本地表示，并将传出的数据包封装在 UDP 数据报中，确保它到达正确的主机。

更多信息，请深入阅读 CoreOS's documentation。

## Container startup

所有的网络配置都已完成。还剩什么？真正地启动工作负载容器！

一旦沙箱完成初始化并处于 active 状态， Kubelet 将开始为其创建容器。首先启动 PodSpec 中定义的 Init Container，然后再启动主容器。具体过程如下：

拉取容器的镜像。如果是私有仓库的镜像，就会使用 PodSpec 中指定的 imagePullSecrets 来拉取该镜像；

通过 CRI 创建容器。 Kubelet 使用 PodSpec 中的信息填充了一个 ContainerConfig 数据结构（在其中定义了 command， image， labels， mounts， devices， environment variables 等），然后通过 protobufs 发送给 CRI。 对于 Docker 来说，它会将这些信息反序列化并填充到自己的配置信息中，然后再发送给 Dockerd 守护进程。在这个过程中，它会将一些元数据（例如容器类型，日志路径，sandbox ID 等）添加到容器中；

然后 Kubelet 将容器注册到 CPU 管理器，它通过使用 UpdateContainerResources CRI 方法给容器分配给本地节点上的 CPU 资源；

最后容器真正地启动；

如果 Pod 中包含 Container Lifecycle Hooks，容器启动之后就会运行这些 Hooks。 Hook 的类型包括两种：Exec（执行一段命令） 和 HTTP（发送HTTP请求）。如果 PostStart Hook 启动的时间过长、挂起或者失败，容器将永远不会变成 Running 状态。

最后的最后，现在我们的集群上应该会运行三个容器，分布在一个或多个工作节点上。所有的网络，数据卷和秘钥都由 Kubelet 填充，并通过 CRI 接口添加到容器中并配置成功！

## 1.30 视角下值得记一下的几点

读到这里再回头看，几个被老博客忽略的 1.30 事实可以一句话记一下，看代码时心里有数：

- **dockershim 已经不在了**（1.24 移除）：1.30 树内没有 `pkg/kubelet/dockershim/`，Docker Engine 只能通过外部 `cri-dockerd`（`--container-runtime-endpoint=unix:///run/cri-dockerd.sock`）；
- **kubelet 不直接调 CNI**：走 CRI `RunPodSandbox`，由 containerd / CRI-O 在沙箱内部调 CNI；
- **pause 持有 netns**：所以 Pod 即使应用容器重启，IP 也不变；
- **Hooks 与 prober 分开**：PostStart/PreStop 由 `HandlerRunner` 直接执行，liveness/readiness/startup 才是 prober 在跑；
- **AppArmor 1.30 GA**：从 annotation 迁到 `securityContext.appArmorProfile`，由 CRI 强制；
- **SidecarContainers 1.30 beta**：restartable init containers 失败时不再 keep pod alive；
- **EventedPLEG 1.30 beta**：订阅 CRI runtime events 替代 GenericPLEG 周期 relist。

读源码时挑 `pkg/kubelet/kubelet.go`（`kl.SyncPod`）、`pkg/kubelet/kuberuntime/kuberuntime_manager.go`（8 步 SyncPod）、`pkg/kubelet/pleg/`、`pkg/kubelet/status/status_manager.go`、`pkg/kubelet/volumemanager/reconciler/reconciler.go` 这五个文件顺着看，链路就齐了。
