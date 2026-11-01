#!/usr/bin/env bash
# poly-config 本地 AP 三节点集群 + 三客户端 启停脚本（测试用）
#
# 用法:
#   scripts/ap-cluster.sh start      # 启动 3 个 server + 3 个 ClientDemo（已在运行的跳过）
#   scripts/ap-cluster.sh servers    # 只启动 3 个 server
#   scripts/ap-cluster.sh clients    # 只启动 3 个 ClientDemo（各连一个节点）
#   scripts/ap-cluster.sh stop       # 停止全部
#   scripts/ap-cluster.sh status     # 查看进程与集群状态
#   scripts/ap-cluster.sh logs [名]  # 实时跟随日志：不带名=全部6个，node-1/client-2=单个
#   scripts/ap-cluster.sh clean      # 停止全部并清空本地数据与日志
#
# 节点与客户端布局（loopback）:
#   server node-1  http 8848  remoting 9848  data/ap/node-1
#   server node-2  http 8849  remoting 9849  data/ap/node-2
#   server node-3  http 8850  remoting 9850  data/ap/node-3
#   client-1 → 9848    client-2 → 9849    client-3 → 9850
# 控制台: http://127.0.0.1:8848/ （8849/8850 同理，任意节点可管理全集群）
#
# 注意（AP-1 阶段语义）: 各节点尚无复制，写哪个节点就只在那个节点生效。
# 例如在 node-1 控制台发布后，只有 client-1 会收到推送；AP-3 复制接入后全集群收敛。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

NODES=(node-1 node-2 node-3)
HTTP_PORTS=(8848 8849 8850)
REMOTING_PORTS=(9848 9849 9850)
CLIENTS=(client-1 client-2 client-3)
CLUSTER_ID="local-ap"
MEMBERS="node-1=127.0.0.1:8848:9848,node-2=127.0.0.1:8849:9849,node-3=127.0.0.1:8850:9850"
DATA_ROOT="data/ap"
LOG_ROOT="logs/ap"

case "$(uname -s)" in
    MINGW*|MSYS*|CYGWIN*) CP_SEP=";"; ;;
    *) CP_SEP=":"; ;;
esac

pid_file() { echo "$LOG_ROOT/$1.pid"; }

# 存活检测与停止统一用 ps/kill：Git Bash 的 $! 是 MSYS pid，
# ps/kill 认它，tasklist/taskkill 认的是 Windows PID，两者口径不同
node_alive() {
    ps -p "$1" >/dev/null 2>&1
}

kill_node() {
    kill "$1" >/dev/null 2>&1 || kill -9 "$1" >/dev/null 2>&1 || true
}

check_env() {
    if ! command -v java >/dev/null 2>&1 || ! command -v mvn >/dev/null 2>&1; then
        echo "[env] 未找到 java/mvn。" >&2
        echo "      - Git Bash: 确认 JDK/Maven 已在 Windows PATH 中" >&2
        echo "      - WSL:      需要 Linux 工具链（sudo apt install openjdk-17-jdk maven），" >&2
        echo "                  并把 MAVEN_OPTS 配到 Windows 仓库:" >&2
        echo "                  export MAVEN_OPTS=\"-Dmaven.repo.local=/mnt/c/Users/你的用户名/.m2/repository\"" >&2
        echo "      最省事的做法是回到 Git Bash 运行本脚本" >&2
        exit 1
    fi
}

build() {
    check_env
    echo "[build] 增量编译 + 生成运行 classpath ..."
    mvn -q -pl poly-config-server -am compile
    mvn -q -pl poly-config-client -am test-compile
    mvn -q -pl poly-config-server dependency:build-classpath \
        -Dmdep.outputFile=target/ap-cp.txt -DincludeScope=runtime
    mvn -q -pl poly-config-client dependency:build-classpath \
        -Dmdep.outputFile=target/client-cp.txt -DincludeScope=test
}

server_classpath() {
    # target/classes 放在最前：改完代码不需要 mvn install 就能生效，
    # 后面的 jar 只提供第三方依赖（netty / fastjson / slf4j / remoting-core）
    echo "poly-config-server/target/classes${CP_SEP}"\
"poly-config-common/target/classes${CP_SEP}"\
"poly-config-client/target/classes${CP_SEP}"\
"$(cat poly-config-server/target/ap-cp.txt)"
}

client_classpath() {
    echo "poly-config-client/target/test-classes${CP_SEP}"\
"poly-config-client/target/classes${CP_SEP}"\
"poly-config-common/target/classes${CP_SEP}"\
"$(cat poly-config-client/target/client-cp.txt)"
}

start_servers() {
    local cp
    cp="$(server_classpath)"
    for i in 0 1 2; do
        local node="${NODES[$i]}"
        local pf
        pf="$(pid_file "$node")"
        if [ -f "$pf" ] && node_alive "$(cat "$pf")"; then
            echo "[skip] $node 已在运行 (pid $(cat "$pf"))"
            continue
        fi
        mkdir -p "$DATA_ROOT/$node"
        echo "[start] $node  http=${HTTP_PORTS[$i]}  remoting=${REMOTING_PORTS[$i]}"
        java -cp "$cp" com.ispengya.polyconfig.server.PolyConfigServer \
            --mode=ap \
            --host=127.0.0.1 \
            --advertise-host=127.0.0.1 \
            --node-id="$node" \
            --cluster-id="$CLUSTER_ID" \
            --http-port="${HTTP_PORTS[$i]}" \
            --remoting-port="${REMOTING_PORTS[$i]}" \
            --data-dir="$DATA_ROOT/$node" \
            --cluster-members="$MEMBERS" \
            >"$LOG_ROOT/$node.out" 2>&1 &
        echo $! > "$pf"
        sleep 1
    done
}

start_clients() {
    local cp
    cp="$(client_classpath)"
    for i in 0 1 2; do
        local client="${CLIENTS[$i]}"
        local pf
        pf="$(pid_file "$client")"
        if [ -f "$pf" ] && node_alive "$(cat "$pf")"; then
            echo "[skip] $client 已在运行 (pid $(cat "$pf"))"
            continue
        fi
        mkdir -p "$DATA_ROOT/client-snapshot/$client"
        echo "[start] $client  → 127.0.0.1:${REMOTING_PORTS[$i]}"
        java -cp "$cp" com.ispengya.polyconfig.client.demo.ClientDemo \
            --server="127.0.0.1:${REMOTING_PORTS[$i]}" \
            --namespace=public \
            --group=DEFAULT_GROUP \
            --dataId=order-service.yml \
            --snapshotDir="$DATA_ROOT/client-snapshot/$client" \
            >"$LOG_ROOT/$client.out" 2>&1 &
        echo $! > "$pf"
        sleep 1
    done
}

stop_all() {
    for node in "${NODES[@]}" "${CLIENTS[@]}"; do
        local pf
        pf="$(pid_file "$node")"
        if [ -f "$pf" ]; then
            local pid
            pid="$(cat "$pf")"
            if node_alive "$pid"; then
                echo "[stop] $node (pid $pid)"
                kill_node "$pid"
            fi
            rm -f "$pf"
        fi
    done
    echo "[done] 已停止"
}

status() {
    for i in 0 1 2; do
        local node="${NODES[$i]}"
        local pf
        pf="$(pid_file "$node")"
        if [ -f "$pf" ] && node_alive "$(cat "$pf")"; then
            local mode
            mode="$(curl -s --max-time 2 "http://127.0.0.1:${HTTP_PORTS[$i]}/v1/cs/cluster" \
                | grep -o '"mode":"[A-Z]*"' | head -1 || true)"
            echo "server $node  运行中 pid=$(cat "$pf")  http=127.0.0.1:${HTTP_PORTS[$i]}  ${mode:-}"
        else
            echo "server $node  未运行"
        fi
    done
    for i in 0 1 2; do
        local client="${CLIENTS[$i]}"
        local pf
        pf="$(pid_file "$client")"
        if [ -f "$pf" ] && node_alive "$(cat "$pf")"; then
            echo "client $client  运行中 pid=$(cat "$pf")  → 127.0.0.1:${REMOTING_PORTS[$i]}"
        else
            echo "client $client  未运行"
        fi
    done
}

start() {
    mkdir -p "$LOG_ROOT"
    build
    start_servers
    start_clients
    echo
    echo "[done] 集群 + 客户端已启动"
    echo "  控制台:  http://127.0.0.1:8848/  http://127.0.0.1:8849/  http://127.0.0.1:8850/"
    echo "  客户端:  tail -f $LOG_ROOT/client-1.out"
    echo "  停止:    scripts/ap-cluster.sh stop"
}

logs() {
    local name="${1:-}"
    if [ -n "$name" ]; then
        local file="$LOG_ROOT/$name.out"
        if [ ! -f "$file" ]; then
            echo "日志文件不存在: $file（先执行 start）" >&2
            exit 1
        fi
        exec tail -f "$file"
    fi
    local files=()
    for n in "${NODES[@]}" "${CLIENTS[@]}"; do
        if [ -f "$LOG_ROOT/$n.out" ]; then
            files+=("$LOG_ROOT/$n.out")
        fi
    done
    if [ "${#files[@]}" -eq 0 ]; then
        echo "还没有日志文件（先执行 start）" >&2
        exit 1
    fi
    exec tail -f "${files[@]}"
}

clean() {
    stop_all
    rm -rf "$DATA_ROOT" "$LOG_ROOT"
    echo "[done] 已清空 $DATA_ROOT 与 $LOG_ROOT"
}

case "${1:-start}" in
    start)   start ;;
    servers) mkdir -p "$LOG_ROOT"; build; start_servers ;;
    clients) mkdir -p "$LOG_ROOT"; build; start_clients ;;
    stop)    stop_all ;;
    status)  status ;;
    logs)    logs "${2:-}" ;;
    clean)   clean ;;
    *)
        echo "用法: $0 [start|servers|clients|stop|status|logs <node-1|client-2|...>|clean]" >&2
        exit 1
        ;;
esac
