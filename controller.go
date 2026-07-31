package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// 两层“选主”，注意区分：
//   - leader election：选出唯一的控制器实例（避免多个控制器同时操作）。靠 Lease 实现。
//   - MySQL 选主：MGR 自身在组内自动选出 PRIMARY（单主模式）。控制器不参与选主，
//     只负责把 role=master 标签打到当前 PRIMARY 对应的 Pod 上，让 master Service 跟随。
//
// 因此“自动故障转移”由 MGR 完成：PRIMARY 所在 Pod 挂掉后，组内剩余成员(多数派)自动
// 选出新 PRIMARY；控制器观测到角色变化后重打标签，master Service 端点随即切换。

const (
	reconcileInterval = 5 * time.Second
)

type controller struct {
	client *kubernetes.Clientset

	masterName   string // 控制器记忆的当前主库（仅用于日志/变化感知）
	lastTopology string
}

// ordinal 从 Pod 名 mysqlha-2 取出 2。
func ordinal(name string) int {
	for i := len(name) - 1; i >= 0; i-- {
		if name[i] == '-' {
			var n int
			fmt.Sscanf(name[i+1:], "%d", &n)
			return n
		}
	}
	return -1
}

// buildSeeds 用当前各 Ready Pod 的 IP 拼出 group_replication_group_seeds 值。
// 纯 IP 通信，每次加组前用最新 IP 重新 SET，规避 Xcom 解析主机名。
func buildSeeds(ready []corev1.Pod) string {
	var parts []string
	for _, p := range ready {
		if p.Status.PodIP != "" {
			parts = append(parts, p.Status.PodIP+":33061")
		}
	}
	return strings.Join(parts, ",")
}

// buildAllowlist 用当前各 Ready Pod 的 IP 拼出 group_replication_ip_allowlist 值。
func buildAllowlist(ready []corev1.Pod) string {
	var parts []string
	for _, p := range ready {
		if p.Status.PodIP != "" {
			parts = append(parts, p.Status.PodIP)
		}
	}
	return strings.Join(parts, ",")
}

// bootstrapAndJoinAll 引导一个全新组(或全停后重建)：挑数据最全的成员 bootstrap，
// 等它 ONLINE，再对其余成员下发 START GROUP_REPLICATION。返回 bootstrap 成员 IP。
func bootstrapAndJoinAll(ctx context.Context, ready []corev1.Pod) (string, error) {
	sort.Slice(ready, func(i, j int) bool { return ordinal(ready[i].Name) < ordinal(ready[j].Name) })

	var gts []podGtid
	for _, p := range ready {
		u, _ := serverUUID(p.Status.PodIP)
		g, _ := gtidExecuted(p.Status.PodIP)
		gts = append(gts, podGtid{name: p.Name, ip: p.Status.PodIP, uuid: u, gtid: g})
	}
	bip, divergent, err := pickBootstrapIP(gts)
	if err != nil {
		return "", err
	}
	if divergent {
		logf("警告: 成员 GTID 不互为子集(可能脑裂)，选 gtid 最多的 %s bootstrap", bip)
	} else {
		logf("bootstrap 成员: %s", bip)
	}
	seeds := buildSeeds(ready)

	_ = stopGR(bip) // 若残留半启动状态先停掉，确保 bootstrap 干净
	if err := ensurePlugin(bip); err != nil {
		return "", fmt.Errorf("ensurePlugin: %w", err)
	}
	// 全新成员(gtid 空 或 只含本机 uuid 的初始化事务)：先 RESET MASTER 清掉这些
	// 各 Pod 各自 uuid 的初始化 GTID(否则彼此互不为子集、加组失败)，再建恢复账号--
	// 它会成为干净的第一笔事务，随后复制给其余成员。
	// 带历史数据的成员(重新 bootstrap)：恢复账号已随组复制存在，不 RESET 以免丢数据。
	if fresh, _ := freshState(bip); fresh {
		if err := execAll(bip, "RESET MASTER"); err != nil {
			return "", fmt.Errorf("RESET MASTER(bootstrap): %w", err)
		}
		if err := ensureReplUser(bip); err != nil {
			return "", fmt.Errorf("ensureReplUser: %w", err)
		}
	} else {
		logf("bootstrap 成员带历史数据，保留 gtid(恢复账号已随组复制存在)")
	}
	_ = setGroupSeeds(bip, seeds)
	_ = setIPAllowlist(bip, buildAllowlist(ready))
	if err := setRecoveryCreds(bip); err != nil {
		return "", fmt.Errorf("setRecoveryCreds: %w", err)
	}
	buuid, err := serverUUID(bip)
	if err != nil {
		return "", fmt.Errorf("serverUUID(bootstrap): %w", err)
	}
	if err := bootstrapGroup(bip); err != nil {
		return "", fmt.Errorf("bootstrapGroup: %w", err)
	}
	if err := waitMemberOnline(bip, buuid, 90*time.Second); err != nil {
		return "", fmt.Errorf("bootstrap 成员未 ONLINE: %w", err)
	}
	logf("bootstrap 成员已 ONLINE: %s", bip)

	for _, p := range ready {
		if p.Status.PodIP == bip {
			continue
		}
		if err := joinOnce(p.Status.PodIP, ready); err != nil {
			logf("join %s: %v (下轮重试)", p.Name, err)
		} else {
			logf("已对 %s 下发 START GROUP_REPLICATION", p.Name)
		}
	}
	return bip, nil
}

// joinOnce 让一个非成员加入运行中的组：若已在 RECOVERING/ONLINE 则不打扰；
// 否则停 GR、清全新成员的初始化 GTID、设种子/白名单/恢复凭据、START。可安全重试。
func joinOnce(ip string, ready []corev1.Pod) error {
	// 已在恢复/在线则别打断
	if ms, err := groupMembers(ip); err == nil {
		for _, m := range ms {
			if m.state == "ONLINE" || m.state == "RECOVERING" {
				return nil
			}
		}
	}
	_ = stopGR(ip)
	if err := ensurePlugin(ip); err != nil {
		return fmt.Errorf("ensurePlugin: %w", err)
	}
	if fresh, _ := freshState(ip); fresh {
		if err := execAll(ip, "RESET MASTER"); err != nil {
			return fmt.Errorf("RESET MASTER: %w", err)
		}
	}
	_ = setGroupSeeds(ip, buildSeeds(ready))
	_ = setIPAllowlist(ip, buildAllowlist(ready))
	if err := setRecoveryCreds(ip); err != nil {
		return fmt.Errorf("setRecoveryCreds: %w", err)
	}
	if err := startGR(ip); err != nil {
		return fmt.Errorf("startGR: %w", err)
	}
	return nil
}

// rejoin 让一个不在组内(或 ERROR)的 Pod 重新加组(joinOnce 的控制器包装)。
func (ctl *controller) rejoin(ctx context.Context, p corev1.Pod, ready []corev1.Pod) {
	if err := joinOnce(p.Status.PodIP, ready); err != nil {
		logf("rejoin %s: %v", p.Name, err)
		return
	}
	logf("已对 %s 下发 START GROUP_REPLICATION (加组)", p.Name)
}

// reconcile 把集群收敛到“组在跑、master Service 指向当前 PRIMARY、掉队成员已加组”的期望状态。
func (ctl *controller) reconcile(ctx context.Context) error {
	pods, err := listMySQLPods(ctx, ctl.client)
	if err != nil {
		return err
	}
	ready := filterReady(pods)
	if len(ready) == 0 {
		logf("尚无 Ready Pod，等待中")
		return nil
	}

	// uuid <-> Pod 映射
	uuid2pod := map[string]corev1.Pod{}
	pod2uuid := map[string]string{}
	for _, p := range ready {
		if u, err := serverUUID(p.Status.PodIP); err == nil && u != "" {
			uuid2pod[u] = p
			pod2uuid[p.Name] = u
		}
	}

	// 找一个 GR 在跑的成员读组视图。
	// 注意: replication_group_members 在 GR 已加载但未 START 时，会显示一行本地 OFFLINE
	// 记录--所以不能只看 len(ms)>0，必须看有没有 ONLINE/RECOVERING 成员才算“组在跑”。
	var members []groupMember
	for _, p := range ready {
		if ms, err := groupMembers(p.Status.PodIP); err == nil {
			up := false
			for _, m := range ms {
				if m.state == "ONLINE" || m.state == "RECOVERING" {
					up = true
					break
				}
			}
			if up {
				members = ms
				break
			}
		}
	}

	if len(members) == 0 {
		logf("组内无在线成员，触发 bootstrap")
		if _, err := bootstrapAndJoinAll(ctx, ready); err != nil {
			logf("bootstrap 失败: %v", err)
		}
		return nil
	}

	// 找当前 PRIMARY
	primaryUUID := ""
	for _, m := range members {
		if m.state == "ONLINE" && m.role == "PRIMARY" {
			primaryUUID = m.memberID
			break
		}
	}
	if primaryUUID != "" {
		if p, ok := uuid2pod[primaryUUID]; ok {
			if ctl.masterName != p.Name {
				logf("当前主库: %s (ip=%s)", p.Name, p.Status.PodIP)
			}
			_ = setPodRole(ctx, ctl.client, p.Name, roleMaster)
			ctl.masterName = p.Name
		}
	}

	// 在线/恢复中的成员不打扰；不在组内(或 ERROR)的触发加组。
	// 同时刷新各在线成员的 IP 白名单为当前 Pod IP，保证某 Pod 重启换 IP 后能被接纳。
	healthy := map[string]bool{}
	for _, m := range members {
		if m.state == "ONLINE" || m.state == "RECOVERING" {
			healthy[m.memberID] = true
		}
	}
	allowlist := buildAllowlist(ready)
	for _, p := range ready {
		u := pod2uuid[p.Name]
		if healthy[u] {
			if u != primaryUUID {
				_ = setPodRole(ctx, ctl.client, p.Name, roleSlave)
			}
			_ = setIPAllowlist(p.Status.PodIP, allowlist)
			continue
		}
		ctl.rejoin(ctx, p, ready)
	}
	return nil
}

// waitForAllOnline 轮询直到 want 个成员都 ONLINE，或超时。sourceIP 是 GR 在跑的成员。
func waitForAllOnline(sourceIP string, want int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ms, err := groupMembers(sourceIP)
		online := 0
		primary := ""
		if err == nil {
			for _, m := range ms {
				if m.state == "ONLINE" {
					online++
					if m.role == "PRIMARY" {
						primary = m.memberID
					}
				}
			}
		}
		logf("等待成员 ONLINE: %d/%d (primary=%s)", online, want, short(primary, 12))
		if online >= want {
			return nil
		}
		time.Sleep(3 * time.Second)
	}
	return fmt.Errorf("%d 个成员未全部 ONLINE（超时）", want)
}

// runController 启动 informer 监听 Pod 变化 + 定时器，驱动 reconcile 循环。
func runController(ctx context.Context, client *kubernetes.Clientset) {
	ctl := &controller{client: client}

	factory := informers.NewSharedInformerFactoryWithOptions(client, 30*time.Second, informers.WithNamespace(namespace))
	podInformer := factory.Core().V1().Pods().Informer()

	syncCh := make(chan struct{}, 1) // 1 容量做去抖
	schedule := func(_ ...any) {
		select {
		case syncCh <- struct{}{}:
		default:
		}
	}
	_, _ = podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(_ any) { schedule() },
		UpdateFunc: func(_, _ any) { schedule() },
		DeleteFunc: func(_ any) { schedule() },
	})

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), podInformer.HasSynced) {
		logf("informer 缓存同步失败")
		return
	}
	logf("informer 就绪，开始 reconcile（间隔 %s）", reconcileInterval)

	schedule()
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logf("控制器退出")
			return
		case <-ticker.C:
			schedule()
		case <-syncCh:
			if err := ctl.reconcile(ctx); err != nil {
				logf("reconcile 出错: %v", err)
			}
			ctl.printTopology(ctx)
		}
	}
}

// printTopology 在拓扑变化时打印，避免每 5s 刷屏。
func (ctl *controller) printTopology(ctx context.Context) {
	ts := topologyString(ctx, ctl.client)
	if ts != ctl.lastTopology {
		ctl.lastTopology = ts
		fmt.Print(ts)
	}
}

// runWithLeaderElection 用 Lease 保证同一时刻只有一个控制器实例在跑。
func runWithLeaderElection(ctx context.Context, client *kubernetes.Clientset) {
	id, err := os.Hostname()
	if err != nil || id == "" {
		id = "mgrha-controller"
	}
	lock := &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: leaseName, Namespace: namespace},
		Client:     client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{Identity: id},
	}
	cfg := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				logf("获得 Lease，以 %s 身份成为 leader，启动控制器", id)
				runController(ctx, client)
			},
			OnStoppedLeading: func() {
				logf("失去 / 释放 Lease，停止 leading")
			},
		},
	}
	le, err := leaderelection.NewLeaderElector(cfg)
	if err != nil {
		logf("创建 leader elector 失败: %v", err)
		return
	}
	le.Run(ctx)
}
