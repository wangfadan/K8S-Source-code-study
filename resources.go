package main

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
)

// ===== 全局常量：整个 MySQL Group Replication 集群的名字与关键参数 =====
const (
	namespace   = "mysqlha"            // 独立命名空间
	appLabel    = "mysqlha"            // Pod/STS 的 app 标签
	stsName     = "mysqlha"            // StatefulSet 名字
	headlessSvc = "mysqlha"            // 无头 Service（给每个 Pod 提供稳定 DNS）
	masterSvc   = "mysqlha-master"     // 主库 Service，selector 跟着 role=master 走
	secretName  = "mysqlha-secret"     // 存 root/repl 密码
	configName  = "mysqlha-config"     // 存 mysqld 配置（含 MGR 参数）
	leaseName   = "mysqlha-controller" // leader election 用的 Lease

	replicas     = int32(3) // 1 主 2 从，满足 MGR 多数派(2/3)
	mysqlPort    = int32(3306)
	groupPort    = int32(33061) // MGR 组通信端口(XCom)
	storageClass = "local-path" // 用 local-path-provisioner 做持久化
	dataSize     = "5Gi"
	mysqlImage   = "swr.cn-north-4.myhuaweicloud.com/ddn-k8s/docker.io/mysql:8.0"

	// MGR 组名：必须是合法 UUID，全组成员一致。
	groupName = "5b86c4f8-9d3a-11ee-bd9b-0242ac120002"

	rootPwd  = "redhat" // MySQL root 密码（用户给定 root/redhat）
	replUser = "repl"   // MGR 分布式恢复账号
	replPwd  = "replpw"

	roleMaster = "master"
	roleSlave  = "slave"
)

// logf 给控制器/资源操作统一的带前缀日志。
func logf(format string, a ...any) {
	fmt.Printf("[mgrha] "+format+"\n", a...)
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "ERROR:", err)
	os.Exit(1)
}

// podFQDN 返回某个 Pod 在集群内的稳定域名（靠无头 Service 提供）。
// 形如 mysqlha-1.mysqlha.mysqlha.svc.cluster.local，状态展示/日志用。
func podFQDN(podName string) string {
	return fmt.Sprintf("%s.%s.%s.svc.cluster.local", podName, headlessSvc, namespace)
}

// ============================ 资源对象构造 ============================

func buildNamespace() *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: namespace}}
}

func buildSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: namespace},
		Data: map[string][]byte{
			"mysql-root-password": []byte(rootPwd),
			"mysql-repl-password": []byte(replPwd),
		},
	}
}

// buildConfigMap 里是所有 Pod 共享的 mysqld 配置。
// server-id 与 group_replication_local_address 是每 Pod 不同的，由 init 容器
// 按 POD_IP 单独写到 server-id.cnf / mgr-local.cnf。
func buildConfigMap() *corev1.ConfigMap {
	// 注意：go-sql-driver/mysql v1.10 已无 allowPublicKeyRetrieval，这里不涉及。
	// group_replication_* 变量用 loose- 前缀：插件由 plugin_load_add 在启动时加载，
	// 加 loose- 可在插件尚未加载的极早期也不让 mysqld 拒绝启动，稳妥。
	mysqldCnf := `[mysqld]
skip-host-cache
skip-name-resolve
gtid_mode=ON
enforce_gtid_consistency=ON
log_bin=mysql-bin
binlog_format=ROW
relay_log=relay
# 把从库回放的事务也写进自己的 binlog(带原始 GTID)，任何成员都能做恢复 donor。
log_slave_updates=ON
# 开机自动加载 MGR 插件
plugin_load_add='group_replication.so'
# 组名(合法 UUID，全组一致)
loose-group_replication_group_name='` + groupName + `'
# 不随 mysqld 启动自动加组——由控制器显式 bootstrap/start，避免鸡生蛋问题
loose-group_replication_start_on_boot=OFF
# 单主模式：1 个 PRIMARY 写，其余 SECONDARY 读
loose-group_replication_single_primary_mode=ON
loose-group_replication_enforce_update_everywhere_checks=OFF
# group_seeds(local_address) 不写死在这里：local_address 用 POD_IP 由 init 容器写，
# seeds 由控制器用当前各 Pod IP 动态 SET GLOBAL——纯 IP 通信，规避 XCom 解析 FQDN 的坑。
# 无 clone 插件，新成员靠 binlog 恢复，保证 binlog 不被过早清理
# IP 白名单：K8s pod 网段(如 100.64.0.0/10)不在 MGR 默认自动白名单(仅 10/172.16/192.168)，
# 不显式放行则成员间 XCom 互拒 "Address is not in the IP allowlist"。覆盖常见私有网段。
loose-group_replication_ip_allowlist=100.64.0.0/10,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16
# 无 clone 插件，新成员靠 binlog 恢复，保证 binlog 不被过早清理
binlog_expire_logs_seconds=2592000
`
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: configName, Namespace: namespace},
		Data:       map[string]string{"mysqld.cnf": mysqldCnf},
	}
}

func buildHeadlessService() *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: headlessSvc, Namespace: namespace},
		Spec: corev1.ServiceSpec{
			Selector:  map[string]string{"app": appLabel},
			ClusterIP: "None", // 无头：给每个 Pod 一个稳定 A 记录
			Ports: []corev1.ServicePort{
				{Name: "mysql", Port: mysqlPort, TargetPort: intstr.FromInt32(mysqlPort)},
				{Name: "group", Port: groupPort, TargetPort: intstr.FromInt32(groupPort)}, // 33061 仅声明用，Pod 间直连即可
			},
		},
	}
}

// buildMasterService 的 selector 带 role=master，控制器给哪个 Pod 打上
// role=master 标签，这个 Service 的 Endpoints 就自动指向谁——客户端永远连到当前主库。
func buildMasterService() *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: masterSvc, Namespace: namespace},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": appLabel, "role": roleMaster},
			Type:     corev1.ServiceTypeClusterIP,
			Ports:    []corev1.ServicePort{{Name: "mysql", Port: mysqlPort, TargetPort: intstr.FromInt32(mysqlPort)}},
		},
	}
}

func initContainer() corev1.Container {
	// 用同一个 mysql 镜像跑 sh：把 ConfigMap 里的 mysqld.cnf 复制到可写的 conf 卷，
	// 再按 POD_IP 写两个唯一的配置文件：
	//   server-id.cnf          —— server-id 取自 Pod IP 数值(每实例唯一，重建后变化)
	//   mgr-local.cnf          —— group_replication_local_address=<POD_IP>:33061
	// MGR 用 server_uuid(PV 持久化在 auto.cnf)标识成员身份，server-id 仅需唯一即可。
	return corev1.Container{
		Name:    "init-conf",
		Image:   mysqlImage,
		Command: []string{"sh", "-c"},
		Args: []string{
			`set -e
cp /tmp/config/mysqld.cnf /etc/mysql/conf.d/mysqld.cnf
o1=${POD_IP%%.*}; r=${POD_IP#*.}; o2=${r%%.*}; r=${r#*.}; o3=${r%%.*}; o4=${r##*.}
SID=$(( o1 * 16777216 + o2 * 65536 + o3 * 256 + o4 ))
[ "$SID" -lt 1 ] && SID=2
printf '[mysqld]\nserver-id=%d\n' "$SID" > /etc/mysql/conf.d/server-id.cnf
# report_host=POD_IP：让 MGR 把 MEMBER_HOST 上报为 Pod IP，否则上报成 Pod 主机名(mysqlha-0)
# 这种短名在集群内不可解析，导致分布式恢复连 donor 时报 "Unknown MySQL server host"。
printf '[mysqld]\nloose-group_replication_local_address=%s:33061\nreport_host=%s\n' "$POD_IP" "$POD_IP" > /etc/mysql/conf.d/mgr-local.cnf
echo "pod=${POD_NAME} ip=${POD_IP} server-id=${SID} mgr-local=${POD_IP}:33061"`,
		},
		Env: []corev1.EnvVar{
			{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
			{Name: "POD_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "configmap", MountPath: "/tmp/config", ReadOnly: true},
			{Name: "conf", MountPath: "/etc/mysql/conf.d"},
		},
	}
}

func mysqlContainer() corev1.Container {
	return corev1.Container{
		Name:  "mysql",
		Image: mysqlImage,
		Ports: []corev1.ContainerPort{
			{ContainerPort: mysqlPort, Name: "mysql"},
			{ContainerPort: groupPort, Name: "group"},
		},
		Env: []corev1.EnvVar{
			{Name: "MYSQL_ROOT_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: secretName}, Key: "mysql-root-password"}}},
			{Name: "MYSQL_ROOT_HOST", Value: "%"}, // 让 root 能从控制器(Pod IP)连进来
			{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		},
		VolumeMounts: []corev1.VolumeMount{
			{Name: "conf", MountPath: "/etc/mysql/conf.d", ReadOnly: true}, // init 写好的配置
			{Name: "data", MountPath: "/var/lib/mysql"},                    // 持久化数据(local-path PVC)
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				Exec: &corev1.ExecAction{Command: []string{"sh", "-c",
					`mysqladmin ping -h 127.0.0.1 -uroot -p"$MYSQL_ROOT_PASSWORD" --silent`}},
			},
			// MGR 配置 + 插件加载 + 首次初始化可能稍慢，给足窗口
			InitialDelaySeconds: 15, PeriodSeconds: 5, TimeoutSeconds: 5, FailureThreshold: 36,
		},
	}
}

func buildStatefulSet() *appsv1.StatefulSet {
	r := replicas
	sc := storageClass
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: stsName, Namespace: namespace, Labels: map[string]string{"app": appLabel}},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: headlessSvc,
			Replicas:    &r,
			Selector:    &metav1.LabelSelector{MatchLabels: map[string]string{"app": appLabel}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": appLabel}},
				Spec: corev1.PodSpec{
					// 强反亲和：3 个 Pod 各落一个节点。配合 local-path(每节点一份 PV)，
					// 单节点挂掉只损失 1 个成员，仍有多数派(2/3)可用。
					Affinity: &corev1.Affinity{
						PodAntiAffinity: &corev1.PodAntiAffinity{
							RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
								{
									LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": appLabel}},
									TopologyKey:   "kubernetes.io/hostname",
								},
							},
						},
					},
					// 容忍 control-plane 污点：本机只有 2 个 worker(knode1/2)+1 个 control-plane(kmaster)，
					// 不容忍则第 3 个 Pod 无处调度。dev 环境允许 MySQL 跑在 master 上换“每节点一成员”。
					Tolerations: []corev1.Toleration{
						{Key: "node-role.kubernetes.io/control-plane", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule},
						{Key: "node-role.kubernetes.io/master", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}, // 老版本兼容
					},
					InitContainers: []corev1.Container{initContainer()},
					Containers:     []corev1.Container{mysqlContainer()},
					Volumes: []corev1.Volume{
						{Name: "conf", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
						{Name: "configmap", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{
							LocalObjectReference: corev1.LocalObjectReference{Name: configName}}}},
					},
				},
			},
			// 持久化：local-path 动态 provisioning。PVC 名 data-mysqlha-{0,1,2}。
			// down 保留 PVC(数据持久)，purge 才删 PVC(全量重置)。
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{Name: "data", Labels: map[string]string{"app": appLabel}},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
						StorageClassName: &sc,
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse(dataSize),
							},
						},
					},
				},
			},
		},
	}
}

// ============================ 创建 / 删除 ============================

// ensureResources 幂等创建全部 K8s 对象（已存在则跳过）。
func ensureResources(ctx context.Context, c *kubernetes.Clientset) error {
	if _, err := c.CoreV1().Namespaces().Create(ctx, buildNamespace(), metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	if err := waitNamespaceActive(ctx, c); err != nil {
		return err
	}

	type item struct {
		name string
		do   func() error
	}
	items := []item{
		{"secret", func() error {
			_, err := c.CoreV1().Secrets(namespace).Create(ctx, buildSecret(), metav1.CreateOptions{})
			return ignoreExists(err)
		}},
		{"configmap", func() error {
			_, err := c.CoreV1().ConfigMaps(namespace).Create(ctx, buildConfigMap(), metav1.CreateOptions{})
			return ignoreExists(err)
		}},
		{"headless service", func() error {
			_, err := c.CoreV1().Services(namespace).Create(ctx, buildHeadlessService(), metav1.CreateOptions{})
			return ignoreExists(err)
		}},
		{"master service", func() error {
			_, err := c.CoreV1().Services(namespace).Create(ctx, buildMasterService(), metav1.CreateOptions{})
			return ignoreExists(err)
		}},
		{"statefulset", func() error {
			_, err := c.AppsV1().StatefulSets(namespace).Create(ctx, buildStatefulSet(), metav1.CreateOptions{})
			if apierrors.IsAlreadyExists(err) {
				// 已存在：更新 Pod 模板(配置变更可重 apply)，volumeClaimTemplates 不可变故只 patch template
				ss, gerr := c.AppsV1().StatefulSets(namespace).Get(ctx, stsName, metav1.GetOptions{})
				if gerr != nil {
					return gerr
				}
				want := buildStatefulSet()
				ss.Spec.Template = want.Spec.Template
				ss.Spec.Replicas = want.Spec.Replicas
				_, uerr := c.AppsV1().StatefulSets(namespace).Update(ctx, ss, metav1.UpdateOptions{})
				return uerr
			}
			return err
		}},
	}
	for _, it := range items {
		if err := it.do(); err != nil {
			return fmt.Errorf("create %s: %w", it.name, err)
		}
		logf("资源就绪: %s", it.name)
	}
	return nil
}

func ignoreExists(err error) error {
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func waitNamespaceActive(ctx context.Context, c *kubernetes.Clientset) error {
	for i := 0; i < 60; i++ {
		ns, err := c.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
		if err == nil && ns.Status.Phase == corev1.NamespaceActive {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return fmt.Errorf("namespace %s 未变为 Active", namespace)
}

// deleteResources 删除业务资源，但保留 PVC(数据持久)。
func deleteResources(ctx context.Context, c *kubernetes.Clientset) error {
	_ = c.AppsV1().StatefulSets(namespace).Delete(ctx, stsName, metav1.DeleteOptions{})
	_ = c.CoreV1().Services(namespace).Delete(ctx, masterSvc, metav1.DeleteOptions{})
	_ = c.CoreV1().Services(namespace).Delete(ctx, headlessSvc, metav1.DeleteOptions{})
	_ = c.CoreV1().ConfigMaps(namespace).Delete(ctx, configName, metav1.DeleteOptions{})
	_ = c.CoreV1().Secrets(namespace).Delete(ctx, secretName, metav1.DeleteOptions{})
	_ = c.CoordinationV1().Leases(namespace).Delete(ctx, leaseName, metav1.DeleteOptions{})
	logf("已下发删除请求（namespace 与 PVC 保留；purge 可删 PVC 全量重置）")
	return nil
}

// deletePVCs 删除 data-* PVC（reclaimPolicy=Delete 会一并删 PV 与节点上的目录）。
func deletePVCs(ctx context.Context, c *kubernetes.Clientset) error {
	return c.CoreV1().PersistentVolumeClaims(namespace).DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{LabelSelector: "app=" + appLabel})
}

// ============================ Pod 操作辅助 ============================

func listMySQLPods(ctx context.Context, c *kubernetes.Clientset) ([]corev1.Pod, error) {
	list, err := c.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{LabelSelector: "app=" + appLabel})
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

func podReady(p corev1.Pod) bool {
	if p.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, cond := range p.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

func filterReady(pods []corev1.Pod) []corev1.Pod {
	var out []corev1.Pod
	for _, p := range pods {
		if podReady(p) && p.Status.PodIP != "" {
			out = append(out, p)
		}
	}
	return out
}

func findPod(pods []corev1.Pod, name string) corev1.Pod {
	for _, p := range pods {
		if p.Name == name {
			return p
		}
	}
	return corev1.Pod{}
}

// setPodRole 用 merge patch 打/改 role 标签。master Service 的 selector 会随之自动生效。
func setPodRole(ctx context.Context, c *kubernetes.Clientset, podName, role string) error {
	patch := fmt.Sprintf(`{"metadata":{"labels":{"role":"%s"}}}`, role)
	_, err := c.CoreV1().Pods(namespace).Patch(ctx, podName, types.MergePatchType, []byte(patch), metav1.PatchOptions{})
	return err
}

// currentMasterName 返回当前带 role=master 标签的 Pod 名（没有则空串）。
func currentMasterName(pods []corev1.Pod) string {
	for _, p := range pods {
		if p.Labels["role"] == roleMaster {
			return p.Name
		}
	}
	return ""
}

func waitForPods(ctx context.Context, c *kubernetes.Clientset, want int) error {
	deadline := time.Now().Add(6 * time.Minute) // 含 PVC provisioning + 首次初始化，给足
	for time.Now().Before(deadline) {
		pods, err := listMySQLPods(ctx, c)
		if err == nil {
			ready := filterReady(pods)
			logf("等待 Pod Ready: %d/%d", len(ready), want)
			if len(ready) >= want {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
	return fmt.Errorf("等待 %d 个 Pod Ready 超时", want)
}

// ============================ 拓扑展示 ============================

func orNA(s string) string {
	if s == "" {
		return "N/A"
	}
	return s
}

func short(s string, n int) string {
	if len(s) > n {
		return s[:n] + "..."
	}
	return s
}

// topologyString 汇总 MGR 组成员状态 + 每个 Pod 的角色，供 status/控制器打印。
func topologyString(ctx context.Context, c *kubernetes.Clientset) string {
	pods, err := listMySQLPods(ctx, c)
	if err != nil {
		return fmt.Sprintf("列出 Pod 失败: %v\n", err)
	}
	sort.Slice(pods, func(i, j int) bool { return pods[i].Name < pods[j].Name })

	ready := filterReady(pods)

	// uuid -> podName
	uuid2pod := map[string]string{}
	pod2uuid := map[string]string{}
	for _, p := range ready {
		if u, err := serverUUID(p.Status.PodIP); err == nil && u != "" {
			uuid2pod[u] = p.Name
			pod2uuid[p.Name] = u
		}
	}

	// 找一个 GR 在跑的 Pod 读组视图
	var members []groupMember
	for _, p := range ready {
		if ms, err := groupMembers(p.Status.PodIP); err == nil && len(ms) > 0 {
			members = ms
			break
		}
	}

	master := currentMasterName(pods)
	var b strings.Builder
	fmt.Fprintf(&b, "命名空间=%s  StatefulSet=%s  Pod数=%d(ready=%d)  masterService=%s  当前主库=%s\n",
		namespace, stsName, len(pods), len(ready), masterSvc, orNA(master))

	if len(members) == 0 {
		b.WriteString("  组状态: 未运行(无在线成员)\n")
	} else {
		// 按 role 排序：PRIMARY 在前
		sort.Slice(members, func(i, j int) bool {
			if members[i].role != members[j].role {
				return members[i].role == "PRIMARY"
			}
			return members[i].memberHost < members[j].memberHost
		})
		online := 0
		for _, m := range members {
			if m.state == "ONLINE" {
				online++
			}
			pn := uuid2pod[m.memberID]
			if pn == "" {
				pn = "?" // 组里有但 Pod 列表没匹配上(可能 uuid 还没取到)
			}
			fmt.Fprintf(&b, "  %-12s role=%-9s state=%-11s host=%s:%s\n",
				pn, m.role, m.state, m.memberHost, orNA(m.memberPort))
		}
		fmt.Fprintf(&b, "  组成员: %d 在线 / %d 总\n", online, len(members))
	}
	return b.String()
}
