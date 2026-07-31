package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// dsn 拼接连接某个 Pod(用 root)的 DSN。
// 注意: go-sql-driver/mysql v1.10 已移除 allowPublicKeyRetrieval 参数--
// 对 caching_sha2_password 它会自动向服务器请求公钥(无 TLS 时)。
// 若误写该参数,驱动会把它当成未知参数,连接时执行 SET allowPublicKeyRetrieval=true,
// MySQL 报 Error 1193 Unknown system variable。
func dsn(podIP, user, pwd string) string {
	// readTimeout 给宽：START GROUP_REPLICATION 在尝试连种子/恢复时可能阻塞数秒甚至更久，
	// 8s 会把它误判成失败。fast 查询不受影响(超时只是上限)。
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/?parseTime=true&timeout=5s&readTimeout=60s&writeTimeout=60s",
		user, pwd, podIP, mysqlPort)
}

func openDB(podIP string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn(podIP, "root", rootPwd))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(2)
	return db, nil
}

// pingMySQL 做一次连通性探活。
func pingMySQL(podIP string) error {
	db, err := openDB(podIP)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return db.PingContext(ctx)
}

// execAll 在一个连接上逐条执行 SQL（mysql 驱动默认不支持多语句）。
func execAll(podIP string, stmts ...string) error {
	db, err := openDB(podIP)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	for _, s := range stmts {
		if _, err := db.ExecContext(ctx, s); err != nil {
			return fmt.Errorf("exec %q: %w", s, err)
		}
	}
	return nil
}

// ============================ MGR 相关 SQL ============================

// ensurePlugin 确认 group_replication 插件已加载。配置里 plugin_load_add 会自动加载，
// 这里兜底：没加载就 INSTALL PLUGIN。
func ensurePlugin(podIP string) error {
	db, err := openDB(podIP)
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var status sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT PLUGIN_STATUS FROM information_schema.PLUGINS WHERE PLUGIN_NAME='group_replication'").Scan(&status); err == nil {
		if strings.EqualFold(status.String, "ACTIVE") {
			return nil
		}
	}
	_, err = db.ExecContext(ctx, "INSTALL PLUGIN group_replication SONAME 'group_replication.so'")
	return err
}

// ensureReplUser 在本成员上建分布式恢复账号(任意成员都可能做 donor)。
// 用 mysql_native_password，避免恢复通道也要走 RSA 公钥。
func ensureReplUser(podIP string) error {
	return execAll(podIP,
		"CREATE USER IF NOT EXISTS '"+replUser+"'@'%'",
		"ALTER USER '"+replUser+"'@'%' IDENTIFIED WITH mysql_native_password BY '"+replPwd+"'",
		"GRANT REPLICATION SLAVE ON *.* TO '"+replUser+"'@'%'",
		"FLUSH PRIVILEGES",
	)
}

// setRecoveryCreds 设置 group_replication_recovery 通道的凭据。
// 必须在 GR 未运行时调用(通道不能正在被用)。凭据随 PV 持久化，重启后还在。
func setRecoveryCreds(podIP string) error {
	return execAll(podIP,
		"CHANGE REPLICATION SOURCE TO SOURCE_USER='"+replUser+"', SOURCE_PASSWORD='"+replPwd+"' FOR CHANNEL 'group_replication_recovery'",
	)
}

// setGroupSeeds 动态设置种子列表(当前各 Pod IP)。group_replication_group_seeds 是动态变量。
// 用纯 IP 而非 FQDN，规避 XCom 解析主机名的坑；控制器每次加组前用最新 IP 重新设置。
func setGroupSeeds(podIP, seeds string) error {
	return execAll(podIP, "SET GLOBAL group_replication_group_seeds='"+seeds+"'")
}

// setIPAllowlist 动态设置 IP 白名单(当前各 Pod IP)。group_replication_ip_allowlist 是动态变量。
// 配置里已放了网段兜底，这里再用精确 Pod IP 覆盖：pod 网段不在 MGR 默认白名单，否则成员间互拒。
func setIPAllowlist(podIP, allowlist string) error {
	if strings.TrimSpace(allowlist) == "" {
		return nil
	}
	return execAll(podIP, "SET GLOBAL group_replication_ip_allowlist='"+allowlist+"'")
}

// bootstrapGroup 在本成员上引导一个全新组(仅在组为空时调用)。
func bootstrapGroup(podIP string) error {
	return execAll(podIP,
		"SET GLOBAL group_replication_bootstrap_group=ON",
		"START GROUP_REPLICATION",
		"SET GLOBAL group_replication_bootstrap_group=OFF",
	)
}

func startGR(podIP string) error {
	return execAll(podIP, "START GROUP_REPLICATION")
}

func stopGR(podIP string) error {
	return execAll(podIP, "STOP GROUP_REPLICATION")
}

// groupMember 解析 performance_schema.replication_group_members 的一行。
type groupMember struct {
	memberID   string // server_uuid
	memberHost string // local_address 的主机部分(=Pod IP)
	memberPort string
	state      string // ONLINE / RECOVERING / OFFLINE / ERROR / UNREACHABLE
	role       string // PRIMARY / SECONDARY
}

// groupMembers 从某个 GR 在跑的成员读组视图。非成员/未启动时返回空切片。
func groupMembers(podIP string) ([]groupMember, error) {
	db, err := openDB(podIP)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx,
		"SELECT MEMBER_ID, MEMBER_HOST, MEMBER_PORT, MEMBER_STATE, MEMBER_ROLE FROM performance_schema.replication_group_members")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []groupMember
	for rows.Next() {
		var m groupMember
		var id, host, port, state, role sql.NullString
		if err := rows.Scan(&id, &host, &port, &state, &role); err != nil {
			return nil, err
		}
		m.memberID = id.String
		m.memberHost = host.String
		m.memberPort = port.String
		m.state = state.String
		m.role = role.String
		out = append(out, m)
	}
	return out, rows.Err()
}

// serverUUID 取本实例 server_uuid(随 PV 持久化于 auto.cnf)，用于把组成员映射到 Pod。
func serverUUID(podIP string) (string, error) {
	db, err := openDB(podIP)
	if err != nil {
		return "", err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var u sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&u); err != nil {
		return "", err
	}
	return u.String, nil
}

// gtidExecuted 取本实例 @@global.gtid_executed，用于 bootstrap 成员挑选。
func gtidExecuted(podIP string) (string, error) {
	db, err := openDB(podIP)
	if err != nil {
		return "", err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var g sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT COALESCE(@@global.gtid_executed,'')").Scan(&g); err != nil {
		return "", err
	}
	return g.String, nil
}

// waitMemberOnline 轮询组视图(从 sourceIP 读)直到 wantUUID 的成员 ONLINE，
// 或进入 ERROR/超时。sourceIP 必须是 GR 在跑的成员。
func waitMemberOnline(sourceIP, wantUUID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ms, err := groupMembers(sourceIP)
		if err == nil {
			for _, m := range ms {
				if m.memberID == wantUUID {
					switch m.state {
					case "ONLINE":
						return nil
					case "ERROR":
						return fmt.Errorf("成员 %s 进入 ERROR 状态", wantUUID)
					}
					break
				}
			}
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("成员 %s 在 %s 内未 ONLINE", wantUUID, timeout)
}

// ============================ bootstrap 成员挑选 ============================

type podGtid struct {
	name string
	ip   string
	uuid string // server_uuid
	gtid string
}

// freshStateByStr 判断成员是否“全新”：gtid_executed 为空，或只含本机 server_uuid
// 的事务。mysql:8.0 镜像首次初始化会生成几条本机 uuid 的 DDL GTID(建 root 等)，
// 各 Pod 因此各带自己 uuid 的事务、互不为子集，直接加组会报“成员含组内没有的事务”。
// 对全新成员加组前需 RESET MASTER 清掉这些初始化 GTID。含别的 uuid(=组内复制来的)
// 则说明已有组数据，不能 RESET(会丢数据)。
func freshStateByStr(ownUUID, gtid string) bool {
	exec := strings.TrimSpace(gtid)
	if exec == "" {
		return true
	}
	own := strings.TrimSpace(ownUUID)
	for _, part := range strings.Split(exec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		uuid := part
		if idx := strings.Index(part, ":"); idx > 0 {
			uuid = strings.TrimSpace(part[:idx])
		}
		if uuid != own {
			return false // 有别的主库产生的事务 -> 已有组数据
		}
	}
	return true
}

// freshState 查询成员并套用 freshStateByStr。
func freshState(podIP string) (bool, error) {
	db, err := openDB(podIP)
	if err != nil {
		return false, err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var ownUUID, executed sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT @@server_uuid, COALESCE(@@global.gtid_executed,'')").Scan(&ownUUID, &executed); err != nil {
		return false, err
	}
	return freshStateByStr(ownUUID.String, executed.String), nil
}

// pickBootstrapIP 挑选最该 bootstrap 的成员：
//   - 全是“全新”(gtid 空 或 只含本机 uuid 的初始化事务)：按序号最小，无需告警。
//   - 有成员带历史数据：选 gtid_executed 是其余成员超集的那个(数据最全)。
//   - 没有严格超集(数据发散，可能脑裂)：退回 gtid 串最长者并返回 divergent=true 供告警。
//
// 用 MySQL 的 GTID_SUBSET(subset,set) 判断 subset ⊆ set。GTID 集合串只含
// hex/-/:/, 无单引号，可安全嵌入单引号 SQL。
func pickBootstrapIP(pods []podGtid) (ip string, divergent bool, err error) {
	if len(pods) == 0 {
		return "", false, fmt.Errorf("无 Pod")
	}
	allFresh := true
	for _, p := range pods {
		if !freshStateByStr(p.uuid, p.gtid) {
			allFresh = false
			break
		}
	}
	if allFresh {
		return pods[0].ip, false, nil // 全新，按序号最小
	}

	// 用 pods[0] 作为执行 GTID_SUBSET 的节点
	db, err := openDB(pods[0].ip)
	if err != nil {
		return "", false, err
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	subset := func(a, b string) bool {
		if strings.TrimSpace(a) == "" {
			return true // 空集是任何集的子集
		}
		var res sql.NullInt64
		q := fmt.Sprintf("SELECT GTID_SUBSET('%s','%s')", a, b)
		if e := db.QueryRowContext(ctx, q).Scan(&res); e != nil {
			return false
		}
		return res.Int64 == 1
	}

	for i, cand := range pods {
		ok := true
		for j, other := range pods {
			if i == j {
				continue
			}
			if !subset(other.gtid, cand.gtid) {
				ok = false
				break
			}
		}
		if ok {
			return cand.ip, false, nil // cand 是所有人的超集
		}
	}
	// 发散：退回 gtid 串最长者(粗略代表事务最多)
	best := pods[0]
	for _, p := range pods[1:] {
		if len(p.gtid) > len(best.gtid) {
			best = p
		}
	}
	return best.ip, true, nil
}
