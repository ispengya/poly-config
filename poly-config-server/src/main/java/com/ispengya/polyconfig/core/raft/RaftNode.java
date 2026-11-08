package com.ispengya.polyconfig.core.raft;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.protocol.AppendEntriesRequest;
import com.ispengya.polyconfig.common.protocol.AppendEntriesResponse;
import com.ispengya.polyconfig.common.protocol.LogEntryData;
import com.ispengya.polyconfig.common.protocol.VoteRequest;
import com.ispengya.polyconfig.common.protocol.VoteResponse;
import com.ispengya.polyconfig.consistency.ClusterMember;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Raft 节点状态机（cp-design.md §1 铁律：一切状态读写串行化到单线程信箱）。
 *
 * <p>单线程信箱由 1 线程的 {@link ScheduledExecutorService} 承担——所有入站
 * 消息（网络 RPC、propose）先投递成任务，选举/心跳定时器也调度在同一线程上，
 * 因此状态变更天然互斥、无需细粒度锁。网络线程只收发字节，磁盘 I/O 在状态
 * 线程内联执行（投票落盘、日志刷盘都是毫秒级）。</p>
 *
 * <p>选举遵循两条安全绳（cp-design.md §3）：规则一——任何消息先比 term，
 * 高者赢、低者忽略、本地 term 永不回退；规则二——计票前校验
 * {@code role == CANDIDATE && term == electionTerm}，跨届/退位后的迟到票作废。
 * 投票两道闸：term 合法 → 本 term 未投他人 → 候选人日志够新；
 * {@code currentTerm/votedFor} 先 fsync 落盘再回复/拉票。</p>
 *
 * <p>提交规则带本 term 限定（Figure 8）：commitIndex 只能推进到
 * "多数派 matchIndex 覆盖且 term == currentTerm" 的位置；旧 term 条目由
 * 当选后追加的 no-op 连带提交。单成员集群多数派为 1，本地追加即提交。</p>
 *
 * <p>CP-3 起提供多节点写复制：propose 追加后立即向全体 follower 推送
 * AppendEntries（逐 peer 的 nextIndex 流水线），follower 走闸1/闸2/闸3
 * （term → prev 接缝 → 冲突截断追加 + fsync），应答驱动 matchIndex 推进、
 * nextIndex 回退（带日志尾线索跳跃）与多数派提交；leaderCommit 随心跳
 * 传播，follower 侧 commit→apply 循环与 leader 同一套。</p>
 *
 * <p>组刷盘：propose 的本地追加不立即 fsync，而是入队尾部"刷盘+提交"任务
 * ——与队列中后续追加合并为一轮 fsync（多写共享一次磁盘动作）；follower
 * 侧一轮 AppendEntries 的全部条目也只 fsync 一次。leader 计入多数派的
 * 自身 matchIndex 只在 fsync 完成后推进（先持久化、再计票）。</p>
 *
 * <p><b>规模说明（刻意例外）</b>：本类超出 800 行软上限属有意为之——选举、
 * 复制、提交、组刷盘全部读写同一批"仅状态线程可见"的字段，按职责拆类会
 * 迫使这些字段公开或加锁，破坏 §1 的单线程信箱铁律；拆分收益低于内聚损失。</p>
 */
public final class RaftNode implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    /**
     * no-op 条目载荷：当选后追加，用于连带提交旧 term 条目并确立本 term 提交点。
     */
    static final byte[] NO_OP = new byte[0];

    /**
     * 角色变更/状态对外可见的只读视图（状态线程写、其余线程读）。
     */
    private volatile RaftRole roleView = RaftRole.FOLLOWER;

    private volatile long termView;

    private volatile String leaderIdView = "";

    private volatile long commitIndexView;

    /**
     * 是否已关闭。
     */
    private volatile boolean closed;

    /**
     * 存储层致命错误后 fail-stop：拒绝新写入，重启恢复。
     */
    private volatile boolean broken;

    private final String nodeId;

    private final List<ClusterMember> members;

    private final RaftLog raftLog;

    private final RaftMetaStore metaStore;

    private final RaftTransport transport;

    private final RaftApplyListener applyListener;

    private final long electionTimeoutMillis;

    private final long heartbeatMillis;

    private final Random random = new Random();

    /**
     * 单线程信箱：状态变更、定时器回调、入站消息处理全部在此线程执行。
     */
    private final ScheduledExecutorService stateExecutor;

    private ScheduledFuture<?> electionTimer;

    private ScheduledFuture<?> heartbeatTimer;

    // ===== 以下字段仅在状态线程访问 =====

    private RaftRole role = RaftRole.FOLLOWER;

    /**
     * 当前认可的 leader（"" 表示未知）。
     */
    private String leaderId = "";

    /**
     * 本次竞选的任期（规则二：与 currentTerm 比对判定"届"）。
     */
    private long electionTerm;

    private int votesGranted;

    private long commitIndex;

    private long lastApplied;

    /**
     * 日志有未 fsync 的追加（组刷盘合并用）。
     */
    private boolean logDirty;

    /**
     * 已有排队中的"刷盘+提交"任务（避免重复入队，多个追加共享一轮）。
     */
    private boolean flushScheduled;

    /**
     * 各成员已确认复制到的 index（leader 视角；本节点为本地追加位点）。
     */
    private final Map<String, Long> matchIndex = new HashMap<>();

    /**
     * 各成员下一条待发送的 index（leader 视角，CP-3 回退用）。
     */
    private final Map<String, Long> nextIndex = new HashMap<>();

    /**
     * 已 apply 的 propose 等待者（index → future），apply 后完成。
     */
    private final Map<Long, CompletableFuture<Long>> applyWaiters = new HashMap<>();

    /**
     * peer 最近一次成功交互时间（nodeId → epoch millis），喂状态页健康位。
     */
    private final ConcurrentHashMap<String, Long> peerLastSeen = new ConcurrentHashMap<>();

    /**
     * 创建 Raft 节点。
     *
     * @param nodeId 本节点 ID
     * @param members 静态成员列表（须包含本节点）
     * @param raftLog 日志
     * @param metaStore term/vote 元数据
     * @param transport 节点间传输
     * @param applyListener 已提交条目 apply 回调
     * @param electionTimeoutMillis 选举超时基数（实际随机 [t, 2t)）
     * @param heartbeatMillis 心跳周期（leader 发送空 AppendEntries 的间隔）
     * @param recoveryCommitIndex 恢复时的初始 commitIndex：单成员集群日志即
     *                            权威（= lastIndex），多成员为 0 等 leader 重新提交
     */
    public RaftNode(String nodeId, List<ClusterMember> members, RaftLog raftLog,
                    RaftMetaStore metaStore, RaftTransport transport,
                    RaftApplyListener applyListener, long electionTimeoutMillis,
                    long heartbeatMillis, long recoveryCommitIndex) {
        if (nodeId == null || nodeId.isEmpty()) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "nodeId 不能为空");
        }
        if (members == null || members.isEmpty()) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "成员列表不能为空");
        }
        boolean selfIncluded = members.stream().anyMatch(m -> nodeId.equals(m.getNodeId()));
        if (!selfIncluded) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "成员列表必须包含本节点 " + nodeId);
        }
        if (electionTimeoutMillis < 100 || heartbeatMillis < 10
                || electionTimeoutMillis < 2 * heartbeatMillis) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT,
                    "选举超时须 ≥100ms 且 ≥2 倍心跳周期, election="
                            + electionTimeoutMillis + ", heartbeat=" + heartbeatMillis);
        }
        this.nodeId = nodeId;
        // Java 8 兼容：拷贝 + 不可变视图，防调用方构造后再改动成员表
        this.members = Collections.unmodifiableList(new ArrayList<>(members));
        this.raftLog = raftLog;
        this.metaStore = metaStore;
        this.transport = transport;
        this.applyListener = applyListener;
        this.electionTimeoutMillis = electionTimeoutMillis;
        this.heartbeatMillis = heartbeatMillis;
        this.termView = metaStore.currentTerm();
        this.commitIndex = Math.max(0, Math.min(recoveryCommitIndex, raftLog.lastIndex()));
        this.commitIndexView = this.commitIndex;

        // 单成员恢复：日志即权威，立即把已提交区间 apply 进状态机
        // （对象尚未发布，构造线程执行安全；此后一切变更都走状态线程）
        if (this.commitIndex > 0) {
            this.matchIndex.put(nodeId, raftLog.lastIndex());
            applyCommitted();
        }
        this.stateExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "poly-config-raft-" + nodeId);
            thread.setDaemon(true);
            return thread;
        });
    }

    // ==================== 生命周期 ====================

    /**
     * 启动：挂上选举超时计时器（follower/candidate 语义由超时驱动）。
     */
    public void start() {
        runOnState(this::resetElectionTimer);
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        cancelElectionTimer();
        cancelHeartbeatTimer();
        stateExecutor.shutdownNow();
        try {
            stateExecutor.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        failWaiters(new PolyConfigException(ErrorCode.UNAVAILABLE, "Raft 节点已关闭"));
        // 节点关闭即释放日志句柄（SegmentLog.close 幂等，上层重复关闭无害）
        raftLog.close();
    }

    /**
     * 节点 ID（包内测试/诊断用）。
     */
    String nodeId() {
        return nodeId;
    }

    /**
     * term/vote 元数据存储（包内测试用）。
     */
    RaftMetaStore metaStore() {
        return metaStore;
    }

    // ==================== 对外只读视图（任意线程） ====================

    public RaftRole role() {
        return roleView;
    }

    public long term() {
        return termView;
    }

    public String leaderId() {
        return leaderIdView;
    }

    public long commitIndex() {
        return commitIndexView;
    }

    /**
     * peer 健康位（展示用）：15 秒窗口内有过成功的投票/心跳交互即健康。
     *
     * @param peerNodeId peer 节点 ID
     * @return 是否健康
     */
    public boolean isPeerHealthy(String peerNodeId) {
        Long lastSeen = peerLastSeen.get(peerNodeId);
        return lastSeen != null
                && System.currentTimeMillis() - lastSeen <= 15_000L;
    }

    // ==================== 入站消息（任意线程投递 → 状态线程执行） ====================

    /**
     * 处理拉票请求（网络线程投递，状态线程执行并完成 future）。
     *
     * @param request 拉票请求
     * @return 应答 future（在状态线程上完成）
     */
    public CompletableFuture<VoteResponse> submitVoteRequest(VoteRequest request) {
        CompletableFuture<VoteResponse> future = new CompletableFuture<>();
        runOnState(() -> {
            try {
                future.complete(handleVoteRequest(request));
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        return future;
    }

    /**
     * 处理复制/心跳请求（网络线程投递，状态线程执行并完成 future）。
     *
     * @param request 复制/心跳请求
     * @return 应答 future（在状态线程上完成）
     */
    public CompletableFuture<AppendEntriesResponse> submitAppendRequest(
            AppendEntriesRequest request) {
        CompletableFuture<AppendEntriesResponse> future = new CompletableFuture<>();
        runOnState(() -> {
            try {
                future.complete(handleAppendRequest(request));
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        return future;
    }

    /**
     * 提交一条写命令（仅 leader；多成员经 AppendEntries 多数派提交）。
     *
     * <p>状态线程内按序执行：prepare（D5 校验 + 赋版本）→ 追加 → 立即
     * 向 follower 推送 → 入队组刷盘任务（fsync 后才把自身计入 matchIndex
     * 并推进 commit）。future 在条目 commit + apply 后完成（D7）。</p>
     *
     * @param prepare 前置处理（校验 + 赋版本 + 编码），状态线程内按序执行
     * @return 条目 apply 后完成的 future（value = index）
     */
    public CompletableFuture<Long> propose(RaftPrepareHandler prepare) {
        CompletableFuture<Long> future = new CompletableFuture<>();
        runOnState(() -> {
            try {
                if (closed) {
                    throw new PolyConfigException(ErrorCode.UNAVAILABLE, "Raft 节点已关闭");
                }
                if (broken) {
                    throw new PolyConfigException(ErrorCode.UNAVAILABLE,
                            "日志写入失败，节点已 fail-stop，请重启恢复");
                }
                if (role != RaftRole.LEADER) {
                    String leader = leaderId.isEmpty() ? "（选举中）" : ", leader=" + leaderId;
                    throw new PolyConfigException(ErrorCode.UNAVAILABLE,
                            "当前节点不是 leader" + leader);
                }
                long next = raftLog.lastIndex() + 1;
                byte[] payload = prepare.prepare(next);
                long index = raftLog.append(metaStore.currentTerm(), payload);
                logDirty = true;
                applyWaiters.put(index, future);
                // 复制不依赖本地 fsync（follower 持久化各自独立），先推送省一个心跳周期
                replicateToPeers();
                // 组刷盘：与队列中后续追加合并为一轮 fsync，再计票提交
                scheduleFlushAndCommit();
            } catch (Throwable t) {
                future.completeExceptionally(t);
            }
        });
        return future;
    }

    /**
     * 组刷盘 + 提交（状态线程尾部任务）：一轮 fsync 覆盖此前全部待刷追加，
     * 随后把自身计入 matchIndex 并推进多数派提交。
     *
     * <p>多个 propose 排队时只有首个入队本任务，后续追加被同一轮 fsync
     * 覆盖（组提交）。刷盘前自身不计入多数派——先持久化、再计票。</p>
     */
    private void scheduleFlushAndCommit() {
        if (flushScheduled || closed) {
            return;
        }
        flushScheduled = true;
        try {
            stateExecutor.execute(() -> {
                flushScheduled = false;
                if (closed) {
                    return;
                }
                try {
                    if (logDirty) {
                        raftLog.flush();
                        logDirty = false;
                    }
                    if (role == RaftRole.LEADER) {
                        // 刷盘完成，自身副本可计入多数派
                        matchIndex.put(nodeId, raftLog.lastIndex());
                    }
                    tryCommit();
                } catch (Throwable t) {
                    log.error("组刷盘/提交任务失败", t);
                }
            });
        } catch (RejectedExecutionException e) {
            flushScheduled = false;
        }
    }

    // ==================== 投票处理（状态线程） ====================

    private VoteResponse handleVoteRequest(VoteRequest request) {
        if (request == null || request.getCandidateId() == null) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "拉票请求非法");
        }
        long currentTerm = metaStore.currentTerm();

        // 规则一：过期候选人直接拒绝并回带 term（教化其退位）
        if (request.getTerm() < currentTerm) {
            return new VoteResponse(currentTerm, false);
        }
        // 更高 term：追平朝代、清空投票、回退 follower（规则一的"高者赢"）
        if (request.getTerm() > currentTerm) {
            metaStore.persist(request.getTerm(), null);
            becomeFollower("");
        }

        // 投票两道闸：本 term 未投他人 + 候选人日志够新
        String votedFor = metaStore.votedFor();
        boolean canVote = votedFor == null || votedFor.equals(request.getCandidateId());
        boolean logUpToDate = request.getLastLogTerm() > raftLog.lastTerm()
                || (request.getLastLogTerm() == raftLog.lastTerm()
                && request.getLastLogIndex() >= raftLog.lastIndex());

        VoteResponse response = new VoteResponse(metaStore.currentTerm(), false);
        if (canVote && logUpToDate) {
            // 先落盘再回复（一 term 一票的持久化基础）
            metaStore.persist(metaStore.currentTerm(), request.getCandidateId());
            resetElectionTimer();
            response.setVoteGranted(true);
            log.debug("投出选票: term={}, candidate={}",
                    metaStore.currentTerm(), request.getCandidateId());
        } else {
            log.debug("拒绝选票: term={}, candidate={}, canVote={}, logUpToDate={}",
                    metaStore.currentTerm(), request.getCandidateId(), canVote, logUpToDate);
        }
        return response;
    }

    /**
     * 拉票应答（状态线程）。
     *
     * @param peerId 应答方
     * @param response 应答；传输失败为 {@code null}
     */
    void onVoteResponse(String peerId, VoteResponse response) {
        if (closed || response == null) {
            return;
        }
        recordPeerSeen(peerId);
        // 规则一：应答带更高 term → 立即回退
        if (response.getTerm() > metaStore.currentTerm()) {
            stepDown(response.getTerm());
            return;
        }
        // 规则二：届已变（退位/换届后）的迟到票作废
        if (role != RaftRole.CANDIDATE || electionTerm != metaStore.currentTerm()) {
            return;
        }
        if (!response.isVoteGranted()) {
            return;
        }
        votesGranted++;
        log.debug("收到选票: peer={}, votes={}/{})", peerId, votesGranted, majority());
        if (votesGranted >= majority()) {
            becomeLeader();
        }
    }

    // ==================== 复制/心跳处理（状态线程） ====================

    private AppendEntriesResponse handleAppendRequest(AppendEntriesRequest request) {
        if (request == null || request.getLeaderId() == null) {
            throw new PolyConfigException(ErrorCode.INVALID_ARGUMENT, "复制请求非法");
        }
        long currentTerm = metaStore.currentTerm();
        AppendEntriesResponse response = new AppendEntriesResponse();
        // 日志尾线索（拒绝时供 leader 跳跃回退；成功时在追加后刷新）
        response.setLastLogIndex(raftLog.lastIndex());
        response.setLastLogTerm(raftLog.lastTerm());

        // 闸1：term 比对——过期 leader 直接拒绝并回带本朝 term（教化其退位）
        if (request.getTerm() < currentTerm) {
            response.setTerm(currentTerm);
            response.setSuccess(false);
            return response;
        }
        if (request.getTerm() > currentTerm) {
            metaStore.persist(request.getTerm(), null);
        }
        // 合法 leader（本朝或新朝）：认领、重置选举计时器（心跳压制）；
        // 本节点若原是 leader 则在此退位并使在途写失败
        becomeFollower(request.getLeaderId());
        recordPeerSeen(request.getLeaderId());

        List<LogEntryData> entries = request.getEntries();
        int entryCount = entries == null ? 0 : entries.size();

        // 闸2：prevLog 接缝核对（prevLogIndex=0 表示从头开始，无接缝可校验）
        long prev = request.getPrevLogIndex();
        boolean aligned = prev == 0
                || (prev >= raftLog.firstIndex() && prev <= raftLog.lastIndex()
                && raftLog.termAt(prev) == request.getPrevLogTerm());
        if (!aligned) {
            // 拒绝并回带日志尾线索（leader 据此跳跃回退 nextIndex）
            response.setTerm(metaStore.currentTerm());
            response.setSuccess(false);
            response.setMatchIndex(-1);
            return response;
        }

        // 条目预检（防御：leader 侧按 nextIndex 逐号组装 + Base64 编码，正常
        // 不会触发）——连续性与解码都在动日志之前完成，失败不产生半截状态
        byte[][] payloads = new byte[entryCount][];
        for (int i = 0; i < entryCount; i++) {
            LogEntryData entry = entries.get(i);
            if (entry.getIndex() != prev + 1 + i) {
                log.error("AppendEntries 条目不连续: prev={}, 第 {} 条 index={}",
                        prev, i, entry.getIndex());
                response.setTerm(metaStore.currentTerm());
                response.setSuccess(false);
                response.setMatchIndex(-1);
                return response;
            }
            try {
                payloads[i] = Base64.getDecoder().decode(entry.getPayload());
            } catch (IllegalArgumentException e) {
                log.error("AppendEntries 条目 Base64 解码失败: 第 {} 条 index={}",
                        i, entry.getIndex());
                response.setTerm(metaStore.currentTerm());
                response.setSuccess(false);
                response.setMatchIndex(-1);
                return response;
            }
        }

        // 闸3：冲突截断 + 追加（同号同 term 的重复条目幂等跳过）
        if (entryCount > 0) {
            long expect = prev + 1;
            int matched = 0;
            while (matched < entryCount
                    && expect <= raftLog.lastIndex()
                    && raftLog.termAt(expect) == entries.get(matched).getTerm()) {
                expect++;
                matched++;
            }
            if (matched < entryCount) {
                if (expect <= raftLog.lastIndex()) {
                    // 冲突尾巴整段截断（保留 [firstIndex, expect-1]）
                    raftLog.truncateAfter(expect - 1);
                }
                for (int i = matched; i < entryCount; i++) {
                    raftLog.append(entries.get(i).getTerm(), payloads[i]);
                }
                // 一轮 fsync 覆盖本请求全部条目（组刷盘），fsync 后才回 true
                raftLog.flush();
                logDirty = false;
            }
        }

        // leaderCommit 传播：apply (lastApplied, min(leaderCommit, lastIndex)]
        long newCommit = Math.min(request.getLeaderCommit(), raftLog.lastIndex());
        if (newCommit > commitIndex) {
            commitIndex = newCommit;
            updateViews();
            applyCommitted();
        }

        response.setTerm(metaStore.currentTerm());
        response.setSuccess(true);
        // 已对齐 + 本请求条目全部落盘的位点（leader 据此推进 matchIndex）
        response.setMatchIndex(prev + entryCount);
        response.setLastLogIndex(raftLog.lastIndex());
        response.setLastLogTerm(raftLog.lastTerm());
        return response;
    }

    /**
     * 复制应答（状态线程）。
     *
     * <p>success → matchIndex 推进（只增不减，防乱序旧应答）+ 多数派提交，
     * 尚有缺口则立即续传（流水线）；failure → nextIndex 回退——优先利用
     * 应答回带的日志尾线索跳跃，保底退一格，随后立即重发。传输失败
     * （{@code null}）不重试，下一轮心跳自然补上（幂等：闸2/闸3 处理重复）。</p>
     *
     * @param peerId 应答方
     * @param response 应答；传输失败为 {@code null}
     */
    void onAppendResponse(String peerId, AppendEntriesResponse response) {
        if (closed || response == null) {
            return;
        }
        recordPeerSeen(peerId);
        // 规则一：应答带更高 term → 立即退位
        if (response.getTerm() > metaStore.currentTerm()) {
            stepDown(response.getTerm());
            return;
        }
        if (role != RaftRole.LEADER) {
            return;
        }
        // 迟到应答（term 落后于本朝）作废：位点推进只信本朝确认
        if (response.getTerm() < metaStore.currentTerm()) {
            return;
        }
        ClusterMember peer = memberById(peerId);
        if (peer == null) {
            return;
        }
        if (response.isSuccess()) {
            long match = response.getMatchIndex();
            long known = matchIndex.getOrDefault(peerId, 0L);
            if (match > known) {
                matchIndex.put(peerId, match);
            }
            long next = Math.max(nextIndex.getOrDefault(peerId, 1L), match + 1);
            nextIndex.put(peerId, next);
            tryCommit();
            // 该 follower 仍有缺口 → 立即续传（不等下一轮心跳）
            if (nextIndex.getOrDefault(peerId, raftLog.lastIndex() + 1)
                    <= raftLog.lastIndex()) {
                sendAppendTo(peer);
            }
        } else {
            // 闸2 被拒：利用回带的日志尾线索跳跃回退，保底退一格、不低于 1
            long next = nextIndex.getOrDefault(peerId, raftLog.lastIndex() + 1);
            long retreated = Math.min(next - 1, response.getLastLogIndex() + 1);
            retreated = Math.max(retreated, 1L);
            if (retreated >= next) {
                retreated = Math.max(next - 1, 1L);
            }
            nextIndex.put(peerId, retreated);
            log.debug("复制被拒回退: peer={}, nextIndex {} -> {}", peerId, next, retreated);
            sendAppendTo(peer);
        }
    }

    // ==================== 选举（状态线程） ====================

    private void startElection() {
        if (closed || role == RaftRole.LEADER) {
            return;
        }
        role = RaftRole.CANDIDATE;
        long newTerm = metaStore.currentTerm() + 1;
        electionTerm = newTerm;
        votesGranted = 1;
        // 先落盘：term + 自投，然后才发拉票
        metaStore.persist(newTerm, nodeId);
        updateViews();
        resetElectionTimer();
        log.info("发起选举: term={}, 日志尾=[{},{}]", newTerm,
                raftLog.lastIndex(), raftLog.lastTerm());

        if (votesGranted >= majority()) {
            becomeLeader();
            return;
        }
        VoteRequest request = new VoteRequest(newTerm, nodeId,
                raftLog.lastIndex(), raftLog.lastTerm());
        for (ClusterMember peer : members) {
            if (peer.getNodeId().equals(nodeId)) {
                continue;
            }
            transport.sendVote(peer, request,
                    resp -> deliverVoteResponse(peer.getNodeId(), resp));
        }
    }

    /**
     * 投递拉票应答到信箱（任意线程安全；传输回调与测试共用入口）。
     *
     * @param peerId 应答方
     * @param response 应答；失败为 {@code null}
     */
    void deliverVoteResponse(String peerId, VoteResponse response) {
        runOnState(() -> onVoteResponse(peerId, response));
    }

    /**
     * 投递复制/心跳应答到信箱（任意线程安全）。
     *
     * @param peerId 应答方
     * @param response 应答；失败为 {@code null}
     */
    void deliverAppendResponse(String peerId, AppendEntriesResponse response) {
        runOnState(() -> onAppendResponse(peerId, response));
    }

    private void becomeLeader() {
        role = RaftRole.LEADER;
        leaderId = nodeId;
        cancelElectionTimer();
        // no-op 先追加再初始化各 peer 位点：旧 term 条目经它连带提交
        long noOpIndex = raftLog.append(metaStore.currentTerm(), NO_OP);
        raftLog.flush();
        logDirty = false;
        long next = noOpIndex + 1;
        for (ClusterMember member : members) {
            nextIndex.put(member.getNodeId(), next);
            matchIndex.put(member.getNodeId(),
                    member.getNodeId().equals(nodeId) ? noOpIndex : 0L);
        }
        updateViews();
        log.info("当选 leader: term={}, noOpIndex={}, members={}",
                metaStore.currentTerm(), noOpIndex, members.size());
        tryCommit();
        startHeartbeatTimer();
        sendHeartbeats();
    }

    private void becomeFollower(String newLeaderId) {
        boolean wasLeader = role == RaftRole.LEADER;
        role = RaftRole.FOLLOWER;
        leaderId = newLeaderId == null ? "" : newLeaderId;
        electionTerm = 0;
        votesGranted = 0;
        if (wasLeader) {
            cancelHeartbeatTimer();
            // D5：step-down 丢弃在途水位——在途写一律失败，客户端重试新 leader
            failWaiters(new PolyConfigException(ErrorCode.UNAVAILABLE,
                    "leader 已退位，在途写未提交，请重试"));
        }
        resetElectionTimer();
        updateViews();
    }

    /**
     * 发现更高 term：追平并回退 follower（规则一）。
     *
     * @param newTerm 更高的 term
     */
    private void stepDown(long newTerm) {
        if (newTerm > metaStore.currentTerm()) {
            metaStore.persist(newTerm, null);
        }
        log.info("发现更高 term，回退 follower: term={}", newTerm);
        becomeFollower("");
    }

    // ==================== 提交与 apply（状态线程） ====================

    /**
     * 多数派提交（本 term 限定）：commitIndex = max{N | 多数 matchIndex ≥ N
     * 且 termAt(N) == currentTerm}，随后 apply (lastApplied, commitIndex]。
     */
    private void tryCommit() {
        if (role != RaftRole.LEADER) {
            return;
        }
        List<Long> matches = new ArrayList<>(members.size());
        for (ClusterMember member : members) {
            matches.add(matchIndex.getOrDefault(member.getNodeId(), 0L));
        }
        matches.sort(Collections.reverseOrder());
        long candidate = matches.get(majority() - 1);
        if (candidate <= commitIndex || candidate > raftLog.lastIndex()) {
            return;
        }
        // Figure 8：只有本 term 的条目可直接提交，旧 term 条目经 no-op 连带
        if (raftLog.termAt(candidate) != metaStore.currentTerm()) {
            return;
        }
        commitIndex = candidate;
        updateViews();
        applyCommitted();
    }

    private void applyCommitted() {
        while (lastApplied < commitIndex) {
            long next = lastApplied + 1;
            RaftLogEntry entry = raftLog.read(next);
            try {
                if (entry.getPayload().length > 0) {
                    applyListener.onApply(next, entry.getTerm(), entry.getPayload());
                }
            } catch (RuntimeException e) {
                log.error("已提交条目 apply 失败，节点 fail-stop: index={}", next, e);
                broken = true;
                failWaiters(new PolyConfigException(ErrorCode.STORAGE_ERROR,
                        "状态机 apply 失败: index=" + next, e));
                return;
            }
            lastApplied = next;
            CompletableFuture<Long> waiter = applyWaiters.remove(next);
            if (waiter != null) {
                waiter.complete(next);
            }
        }
    }

    // ==================== 心跳（状态线程） ====================

    private void startHeartbeatTimer() {
        cancelHeartbeatTimer();
        heartbeatTimer = stateExecutor.scheduleAtFixedRate(this::sendHeartbeats,
                heartbeatMillis, heartbeatMillis, TimeUnit.MILLISECONDS);
    }

    /**
     * 单条消息批量上限：限制单条 AppendEntries 的条目数，避免超大 JSON 报文。
     */
    private static final int MAX_ENTRIES_PER_APPEND = 256;

    private void sendHeartbeats() {
        replicateToPeers();
    }

    /**
     * 向全体 follower 推送（状态线程）：逐 peer 按 nextIndex 组装条目，
     * 无条目即心跳。
     */
    private void replicateToPeers() {
        if (closed || role != RaftRole.LEADER) {
            return;
        }
        for (ClusterMember peer : members) {
            if (peer.getNodeId().equals(nodeId)) {
                continue;
            }
            sendAppendTo(peer);
        }
    }

    /**
     * 给单个 follower 组装并发送 AppendEntries（状态线程）：
     * prev 凭据取 nextIndex-1 的接缝，条目取 [nextIndex, lastIndex] 的批量切片。
     *
     * @param peer 目标 follower
     */
    private void sendAppendTo(ClusterMember peer) {
        if (closed || role != RaftRole.LEADER) {
            return;
        }
        long next = nextIndex.getOrDefault(peer.getNodeId(), raftLog.lastIndex() + 1);
        long prev = next - 1;
        AppendEntriesRequest request = new AppendEntriesRequest();
        request.setTerm(metaStore.currentTerm());
        request.setLeaderId(nodeId);
        request.setPrevLogIndex(prev);
        request.setPrevLogTerm(prev == 0 ? 0 : raftLog.termAt(prev));
        request.setLeaderCommit(commitIndex);
        List<LogEntryData> entries = new ArrayList<>();
        long lastIndex = raftLog.lastIndex();
        for (long index = next; index <= lastIndex && entries.size() < MAX_ENTRIES_PER_APPEND;
                index++) {
            RaftLogEntry entry = raftLog.read(index);
            entries.add(new LogEntryData(index, entry.getTerm(),
                    Base64.getEncoder().encodeToString(entry.getPayload())));
        }
        request.setEntries(entries);
        transport.sendAppend(peer, request,
                resp -> deliverAppendResponse(peer.getNodeId(), resp));
    }

    // ==================== 计时器与信箱（状态线程） ====================

    private void resetElectionTimer() {
        cancelElectionTimer();
        if (closed) {
            return;
        }
        // 随机超时 [t, 2t)：错开首轮，避免瓜分选票
        long timeout = electionTimeoutMillis
                + (long) (random.nextDouble() * electionTimeoutMillis);
        electionTimer = stateExecutor.schedule(() -> {
            if (!closed && role != RaftRole.LEADER) {
                startElection();
            }
        }, timeout, TimeUnit.MILLISECONDS);
    }

    private void cancelElectionTimer() {
        if (electionTimer != null) {
            electionTimer.cancel(false);
            electionTimer = null;
        }
    }

    private void cancelHeartbeatTimer() {
        if (heartbeatTimer != null) {
            heartbeatTimer.cancel(false);
            heartbeatTimer = null;
        }
    }

    /**
     * 投递任务到状态线程（信箱）；关闭后丢弃。
     */
    private void runOnState(Runnable task) {
        if (closed) {
            return;
        }
        try {
            stateExecutor.execute(() -> {
                if (closed) {
                    return;
                }
                try {
                    task.run();
                } catch (Throwable t) {
                    log.error("Raft 状态任务执行失败", t);
                }
            });
        } catch (RejectedExecutionException e) {
            // 关闭竞态：丢弃即可
        }
    }

    private void updateViews() {
        roleView = role;
        termView = metaStore.currentTerm();
        leaderIdView = leaderId;
        commitIndexView = commitIndex;
    }

    private void recordPeerSeen(String peerId) {
        if (peerId != null && !peerId.isEmpty() && !peerId.equals(nodeId)) {
            peerLastSeen.put(peerId, System.currentTimeMillis());
        }
    }

    private int majority() {
        return members.size() / 2 + 1;
    }

    private ClusterMember memberById(String peerId) {
        for (ClusterMember member : members) {
            if (member.getNodeId().equals(peerId)) {
                return member;
            }
        }
        return null;
    }

    private void failWaiters(PolyConfigException cause) {
        for (CompletableFuture<Long> waiter : applyWaiters.values()) {
            waiter.completeExceptionally(cause);
        }
        applyWaiters.clear();
    }

    /**
     * 仅供测试：直接驱动一次选举超时（绕过随机计时器）。
     */
    void forceElectionTimeout() {
        runOnState(() -> {
            if (!closed && role != RaftRole.LEADER) {
                startElection();
            }
        });
    }

    /**
     * 仅供测试：等待状态线程处理完当前已投递任务。
     */
    void awaitStateIdle() throws Exception {
        CompletableFuture<Void> marker = new CompletableFuture<>();
        runOnState(() -> marker.complete(null));
        marker.get(5, TimeUnit.SECONDS);
    }

    /**
     * 仅供测试：当前日志尾（状态线程外的近似读，测试在静止期调用）。
     */
    long testLastIndex() {
        return raftLog.lastIndex();
    }

    /**
     * 仅供测试：指定 index 的 term（状态线程外的近似读，测试在静止期调用）。
     */
    long testTermAt(long index) {
        if (raftLog.isEmpty() || index > raftLog.lastIndex()) {
            return -1L;
        }
        return raftLog.termAt(index);
    }
}
