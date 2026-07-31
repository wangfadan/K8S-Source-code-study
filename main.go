package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"k8s.io/client-go/kubernetes"
)

// 用法:
//
//	go run . up      # 创建全部 K8s 资源(含 local-path PVC)，等 Pod Ready，bootstrap MGR 组，启动控制器
//	go run . down    # 删除业务资源（保留 namespace 与 PVC，数据持久；下次 up 复用数据重组）
//	go run . purge   # 在 down 基础上再删 PVC（PV 与节点目录由 reclaimPolicy=Delete 一并清理，全量重置）
//	go run . status  # 打印 MGR 组成员拓扑与当前主库
//
// 演示自动故障转移：另开终端 delete 主库 Pod（kubectl delete pod mysqlha-0 -n mysqlha），
// MGR 会在剩余多数派里自动选出新 PRIMARY，控制器把 master Service 指过去；
// 被删 Pod 在原节点借 local-path PVC 恢复数据后自动重新加组(SECONDARY)。
func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	client := GetClient()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	switch os.Args[1] {
	case "up":
		fmt.Println("==> 1. 创建 K8s 资源")
		if err := ensureResources(ctx, client); err != nil {
			fail(err)
		}
		fmt.Println("==> 2. 等待 MySQL Pod Ready")
		if err := waitForPods(ctx, client, int(replicas)); err != nil {
			fail(err)
		}
		fmt.Println("==> 3. 配置 MGR 并 bootstrap 组")
		if err := setupAndBootstrap(ctx, client); err != nil {
			fail(err)
		}
		fmt.Println("==> 4. 启动控制器(leader election)，Ctrl-C 退出")
		runWithLeaderElection(ctx, client)

	case "down":
		if err := deleteResources(ctx, client); err != nil {
			fail(err)
		}

	case "purge":
		if err := deleteResources(ctx, client); err != nil {
			fail(err)
		}
		if err := deletePVCs(ctx, client); err != nil {
			fail(err)
		}
		logf("已删除 PVC（PV 与节点目录由 reclaimPolicy=Delete 一并清理）")

	case "status":
		fmt.Print(topologyString(ctx, client))

	default:
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "用法: go run . <up|down|purge|status>")
}

// setupAndBootstrap: 等所有 Pod 的 MySQL 可连，配好恢复账号/凭据/种子，再 bootstrap 组
// (或对已在跑的组补齐未加组成员)，最后等全部 ONLINE。
func setupAndBootstrap(ctx context.Context, c *kubernetes.Clientset) error {
	pods, err := listMySQLPods(ctx, c)
	if err != nil {
		return err
	}
	ready := filterReady(pods)
	sort.Slice(ready, func(i, j int) bool { return ordinal(ready[i].Name) < ordinal(ready[j].Name) })

	for _, p := range ready {
		if err := waitMySQLUp(p.Status.PodIP, 90*time.Second); err != nil {
			return fmt.Errorf("MySQL %s 未就绪: %w", p.Name, err)
		}
		logf("MySQL 可连: %s", p.Name)
	}

	// 每个成员: 插件 + 种子 + 恢复通道凭据。
	// 注意: 恢复账号(repl) 只在 bootstrap 成员上创建(见 bootstrapAndJoinAll)，其余成员
	// 通过组复制自动获得。若每个成员各自 CREATE USER，会各自产生本机 server_uuid 的 GTID，
	// 彼此互不为子集，导致加组时报“成员含有组内没有的事务”而失败。
	seeds := buildSeeds(ready)
	for _, p := range ready {
		if err := ensurePlugin(p.Status.PodIP); err != nil {
			return fmt.Errorf("ensurePlugin %s: %w", p.Name, err)
		}
		_ = setGroupSeeds(p.Status.PodIP, seeds)
		_ = setRecoveryCreds(p.Status.PodIP)
	}

	// 组是否已在跑(有 ONLINE/RECOVERING 成员才算；纯 OFFLINE 本地行不算)
	var sourceIP string
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
				sourceIP = p.Status.PodIP
				break
			}
		}
	}

	if sourceIP == "" {
		logf("组未运行，开始 bootstrap")
		bip, err := bootstrapAndJoinAll(ctx, ready)
		if err != nil {
			return err
		}
		sourceIP = bip
	} else {
		logf("组已在运行，补齐未加组成员")
		ms, _ := groupMembers(sourceIP)
		healthy := map[string]bool{}
		for _, m := range ms {
			if m.state == "ONLINE" || m.state == "RECOVERING" {
				healthy[m.memberID] = true
			}
		}
		for _, p := range ready {
			u, _ := serverUUID(p.Status.PodIP)
			if !healthy[u] {
				if err := joinOnce(p.Status.PodIP, ready); err != nil {
					logf("join %s: %v", p.Name, err)
				} else {
					logf("已对 %s 下发 START GROUP_REPLICATION", p.Name)
				}
			}
		}
	}

	if err := waitForAllOnline(sourceIP, len(ready), 4*time.Minute); err != nil {
		logf("警告: %v（控制器会继续收敛）", err)
	}
	return nil
}

// waitMySQLUp 轮询直到 MySQL 可 ping 通。
func waitMySQLUp(podIP string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := pingMySQL(podIP); err == nil {
			return nil
		}
		time.Sleep(3 * time.Second)
	}
	return fmt.Errorf("ping %s 超时", podIP)
}
