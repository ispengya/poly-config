package com.ispengya.polyconfig.core.raft;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.protocol.AppendEntriesRequest;
import com.ispengya.polyconfig.common.protocol.AppendEntriesResponse;
import com.ispengya.polyconfig.common.protocol.VoteRequest;
import com.ispengya.polyconfig.common.protocol.VoteResponse;
import com.ispengya.polyconfig.consistency.ClusterMember;
import com.ispengya.polyconfig.storage.snapshot.SnapshotData;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link RaftNode} 单元测试（CP-2/CP-3/CP-4 验收：投票决策矩阵、规则一/
 * 规则二、单节点选举与写入、三节点唯一 leader 与换代、多节点复制提交、
 * nextIndex 回退追平、图 8 旧 term 未提交条目不 apply、read-index 线性读、
 * InstallSnapshot 追平）。
 */
public class RaftNodeTest {

    /**
     * 选举超时基数（实际随机 [300, 600)）。
     */
    private static final long ELECTION_TIMEOUT = 300L;

    private static final long HEARTBEAT = 50L;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    // ==================== 选举 ====================

    @Test
    public void shouldElectSingleNodeLeaderAndCommitNoOp() throws Exception {
        DirectNetwork network = new DirectNetwork();
        List<byte[]> applied = new CopyOnWriteArrayList<>();
        RaftNode node = newNode("n1", new String[]{"n1"}, network, applied, null, 0L);
        network.register("n1", node);
        try {
            node.start();
            awaitTrue(() -> node.role() == RaftRole.LEADER, 5000L, "单节点应自当选 leader");
            assertEquals("n1", node.leaderId());
            assertTrue("term 应 ≥1", node.term() >= 1);
            // 当选即追加 no-op 并以多数派=1 立即提交
            assertTrue("no-op 应提交", node.commitIndex() >= 1);
            assertTrue("no-op 空载荷不应进状态机", applied.isEmpty());
        } finally {
            node.close();
        }
    }

    @Test
    public void shouldProposeOnSingleNodeLeader() throws Exception {
        DirectNetwork network = new DirectNetwork();
        List<byte[]> applied = new CopyOnWriteArrayList<>();
        RaftNode node = newNode("n1", new String[]{"n1"}, network, applied, null, 0L);
        network.register("n1", node);
        try {
            node.start();
            awaitTrue(() -> node.role() == RaftRole.LEADER, 5000L, "单节点应自当选 leader");

            long index = node.propose(next ->
                    ("cmd-" + next).getBytes(StandardCharsets.UTF_8))
                    .get(3, TimeUnit.SECONDS);
            assertTrue("no-op 占 1 号，业务条目应 ≥2", index >= 2);
            assertEquals(1, applied.size());
            assertEquals("cmd-" + index,
                    new String(applied.get(0), StandardCharsets.UTF_8));
            assertEquals(index, node.commitIndex());
        } finally {
            node.close();
        }
    }

    @Test
    public void shouldElectSingleLeaderInThreeNodeClusterAndReElect() throws Exception {
        DirectNetwork network = new DirectNetwork();
        List<RaftNode> nodes = new ArrayList<>();
        try {
            for (String id : new String[]{"n1", "n2", "n3"}) {
                RaftNode node = newNode(id, new String[]{"n1", "n2", "n3"},
                        network, new CopyOnWriteArrayList<>(), null, 0L);
                network.register(id, node);
                nodes.add(node);
                node.start();
            }
            awaitTrue(() -> nodes.stream().filter(n -> n.role() == RaftRole.LEADER).count() == 1,
                    8000L, "三节点应选出唯一 leader");
            RaftNode leader = nodes.stream()
                    .filter(n -> n.role() == RaftRole.LEADER).findFirst()
                    .orElseThrow(() -> new IllegalStateException("leader 不存在"));
            long firstTerm = leader.term();
            awaitTrue(() -> nodes.stream().allMatch(n -> leader.nodeId().equals(n.leaderId())
                            || n == leader),
                    5000L, "全体应认同同一 leader");

            // 同 term 双主采样：连续多次检查不得出现两个同任期 leader
            for (int i = 0; i < 10; i++) {
                assertNoDualLeaders(nodes);
                Thread.sleep(30L);
            }

            // 杀 leader → 其余节点在随机超时内换代
            String deadId = leader.nodeId();
            leader.close();
            network.unregister(deadId);
            awaitTrue(() -> nodes.stream()
                            .filter(n -> n != leader && n.role() == RaftRole.LEADER).count() == 1,
                    8000L, "存活节点应选出新 leader");
            RaftNode newLeader = nodes.stream()
                    .filter(n -> n != leader && n.role() == RaftRole.LEADER)
                    .findFirst()
                    .orElseThrow(() -> new IllegalStateException("新 leader 不存在"));
            assertTrue("新 leader term 应更高: first=" + firstTerm
                    + ", now=" + newLeader.term(), newLeader.term() > firstTerm);
        } finally {
            for (RaftNode node : nodes) {
                node.close();
            }
        }
    }

    // ==================== 投票决策矩阵 ====================

    @Test
    public void shouldGrantVoteOncePerTermAndPersistBeforeResponse() throws Exception {
        DirectNetwork network = new DirectNetwork();
        RaftNode node = newNode("n1", new String[]{"n1", "n2"}, network,
                new CopyOnWriteArrayList<>(), null, 0L);
        try {
            // 未 start：无选举计时器，纯投票决策
            VoteResponse first = node.submitVoteRequest(
                    new VoteRequest(1, "n2", 0, 0)).get(3, TimeUnit.SECONDS);
            assertTrue(first.isVoteGranted());
            assertEquals(1L, first.getTerm());
            // 落盘在应答之前：重开 meta 应看到 n2 的票
            assertEquals("n2", node.metaStore().votedFor());
            assertEquals(1L, node.metaStore().currentTerm());

            // 同 term 换人：一 term 一票，拒绝
            VoteResponse second = node.submitVoteRequest(
                    new VoteRequest(1, "n3", 0, 0)).get(3, TimeUnit.SECONDS);
            assertFalse(second.isVoteGranted());
            assertEquals(1L, second.getTerm());

            // 同 term 同人重发：幂等仍同意（不改变已持久化的事实）
            VoteResponse repeat = node.submitVoteRequest(
                    new VoteRequest(1, "n2", 0, 0)).get(3, TimeUnit.SECONDS);
            assertTrue(repeat.isVoteGranted());
        } finally {
            node.close();
        }
    }

    @Test
    public void shouldRejectStaleTermVote() throws Exception {
        DirectNetwork network = new DirectNetwork();
        RaftNode node = newNode("n1", new String[]{"n1", "n2"}, network,
                new CopyOnWriteArrayList<>(), null, 0L);
        try {
            node.submitVoteRequest(new VoteRequest(3, "n2", 0, 0))
                    .get(3, TimeUnit.SECONDS);
            VoteResponse stale = node.submitVoteRequest(
                    new VoteRequest(2, "n3", 0, 0)).get(3, TimeUnit.SECONDS);
            assertFalse("过期 term 应被拒绝", stale.isVoteGranted());
            assertEquals("应答回带当前 term", 3L, stale.getTerm());
            assertEquals("本地 term 不回退", 3L, node.term());
        } finally {
            node.close();
        }
    }

    @Test
    public void shouldRejectInferiorLogCandidate() throws Exception {
        Path dir = folder.newFolder("stale-log").toPath();
        RaftLog raftLog = new RaftLog(dir);
        for (int i = 0; i < 3; i++) {
            raftLog.append(2, ("cmd-" + i).getBytes(StandardCharsets.UTF_8));
        }
        DirectNetwork network = new DirectNetwork();
        RaftNode node = newNode("n1", new String[]{"n1", "n2"}, network,
                new CopyOnWriteArrayList<>(), raftLog, 0L);
        try {
            // 日志更旧（term 1 < 2）→ 拒
            VoteResponse olderTerm = node.submitVoteRequest(
                    new VoteRequest(5, "n2", 3, 1)).get(3, TimeUnit.SECONDS);
            assertFalse(olderTerm.isVoteGranted());
            // 同 term 但更短（index 2 < 3）→ 拒
            VoteResponse shorter = node.submitVoteRequest(
                    new VoteRequest(5, "n2", 2, 2)).get(3, TimeUnit.SECONDS);
            assertFalse(shorter.isVoteGranted());
            // 一样新 → 同意
            VoteResponse upToDate = node.submitVoteRequest(
                    new VoteRequest(5, "n2", 3, 2)).get(3, TimeUnit.SECONDS);
            assertTrue(upToDate.isVoteGranted());
        } finally {
            node.close();
            raftLog.close();
        }
    }

    // ==================== 规则一 / 规则二 ====================

    @Test
    public void shouldStepDownOnHigherTermAppend() throws Exception {
        DirectNetwork network = new DirectNetwork();
        RaftNode node = newNode("n1", new String[]{"n1"}, network,
                new CopyOnWriteArrayList<>(), null, 0L);
        network.register("n1", node);
        try {
            node.start();
            awaitTrue(() -> node.role() == RaftRole.LEADER, 5000L, "单节点应自当选");
            long leaderTerm = node.term();

            AppendEntriesRequest request = new AppendEntriesRequest();
            request.setTerm(leaderTerm + 5);
            request.setLeaderId("n2");
            request.setPrevLogIndex(0);
            request.setPrevLogTerm(0);
            request.setLeaderCommit(0);
            AppendEntriesResponse response = node.submitAppendRequest(request)
                    .get(3, TimeUnit.SECONDS);
            assertTrue(response.isSuccess());
            assertEquals(leaderTerm + 5, response.getTerm());
            assertEquals("应立即回退 follower", RaftRole.FOLLOWER, node.role());
            assertEquals(leaderTerm + 5, node.term());
            assertEquals("n2", node.leaderId());
        } finally {
            node.close();
        }
    }

    @Test
    public void shouldDiscardLateVoteResponseAfterStepDown() throws Exception {
        DirectNetwork network = new DirectNetwork();
        RaftNode node = newNode("n1", new String[]{"n1", "n2"}, network,
                new CopyOnWriteArrayList<>(), null, 0L);
        try {
            // 触发竞选 → candidate/term1（n2 未注册，拉票静默失败）
            node.forceElectionTimeout();
            node.awaitStateIdle();
            assertEquals(RaftRole.CANDIDATE, node.role());

            // 收到更高 term 心跳 → 回退 follower/term2（规则一）
            AppendEntriesRequest request = new AppendEntriesRequest();
            request.setTerm(2);
            request.setLeaderId("n2");
            request.setPrevLogIndex(0);
            node.submitAppendRequest(request).get(3, TimeUnit.SECONDS);
            assertEquals(RaftRole.FOLLOWER, node.role());
            assertEquals(2L, node.term());

            // 迟到的 term1 赞成票：规则二——届已变，计票作废
            node.deliverVoteResponse("n2", new VoteResponse(1, true));
            node.awaitStateIdle();
            assertEquals("迟到票不得使其称王", RaftRole.FOLLOWER, node.role());
            assertEquals("term 不得被旧应答回带", 2L, node.term());
        } finally {
            node.close();
        }
    }

    // ==================== CP-2 范围守卫 ====================

    @Test
    public void shouldRejectProposeWhenNotLeader() throws Exception {
        DirectNetwork network = new DirectNetwork();
        RaftNode node = newNode("n1", new String[]{"n1", "n2"}, network,
                new CopyOnWriteArrayList<>(), null, 0L);
        try {
            // 未 start：始终 follower
            try {
                node.propose(next -> new byte[]{1}).get(3, TimeUnit.SECONDS);
                fail("非 leader propose 应被拒绝");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof PolyConfigException);
                assertEquals(ErrorCode.UNAVAILABLE,
                        ((PolyConfigException) e.getCause()).getErrorCode());
                assertTrue(e.getCause().getMessage().contains("leader"));
            }
        } finally {
            node.close();
        }
    }

    // ==================== CP-3 多节点复制与提交 ====================

    @Test
    public void shouldReplicateAndCommitOnMajority() throws Exception {
        DirectNetwork network = new DirectNetwork();
        List<RaftNode> nodes = new ArrayList<>();
        List<List<byte[]>> appliedAll = new ArrayList<>();
        try {
            for (String id : new String[]{"n1", "n2", "n3"}) {
                List<byte[]> applied = new CopyOnWriteArrayList<>();
                RaftNode node = newNode(id, new String[]{"n1", "n2", "n3"},
                        network, applied, null, 0L);
                network.register(id, node);
                nodes.add(node);
                appliedAll.add(applied);
                node.start();
            }
            awaitTrue(() -> nodes.stream().filter(n -> n.role() == RaftRole.LEADER).count() == 1,
                    8000L, "三节点应选出唯一 leader");
            RaftNode leader = nodes.stream()
                    .filter(n -> n.role() == RaftRole.LEADER).findFirst().get();

            long index = leader.propose(next ->
                    "cp3-write".getBytes(StandardCharsets.UTF_8))
                    .get(3, TimeUnit.SECONDS);
            assertTrue("业务条目应 ≥2", index >= 2);

            // 多数派提交 → 全体 follower 经 leaderCommit 传播 apply（推送钩子同套）
            for (int i = 0; i < nodes.size(); i++) {
                final int nodeIndex = i;
                awaitTrue(() -> appliedAll.get(nodeIndex).size() == 1
                                && nodes.get(nodeIndex).commitIndex() >= index,
                        5000L, "节点 " + nodes.get(i).nodeId() + " 应 apply 该条目");
                assertEquals("cp3-write",
                        new String(appliedAll.get(nodeIndex).get(0), StandardCharsets.UTF_8));
            }
        } finally {
            for (RaftNode node : nodes) {
                node.close();
            }
        }
    }

    @Test
    public void shouldCatchUpLaggardViaNextIndexRetreat() throws Exception {
        DirectNetwork network = new DirectNetwork();
        List<RaftNode> nodes = new ArrayList<>();
        List<List<byte[]>> appliedAll = new ArrayList<>();
        try {
            // n3 落在后面：不注册、不启动（收不到任何消息，也发不出竞选）
            for (String id : new String[]{"n1", "n2", "n3"}) {
                List<byte[]> applied = new CopyOnWriteArrayList<>();
                RaftNode node = newNode(id, new String[]{"n1", "n2", "n3"},
                        network, applied, null, 0L);
                if (!id.equals("n3")) {
                    network.register(id, node);
                    node.start();
                }
                nodes.add(node);
                appliedAll.add(applied);
            }
            RaftNode laggard = nodes.get(2);
            awaitTrue(() -> nodes.stream().filter(n -> n.role() == RaftRole.LEADER).count() == 1,
                    8000L, "三节点应选出唯一 leader（n3 未注册不影响 2/3 多数）");
            RaftNode leader = nodes.stream()
                    .filter(n -> n.role() == RaftRole.LEADER).findFirst().get();

            long index = leader.propose(next ->
                    "before-laggard".getBytes(StandardCharsets.UTF_8))
                    .get(3, TimeUnit.SECONDS);
            // n1+n2 多数派即可提交（n3 不参与）
            awaitTrue(() -> appliedAll.get(0).size() == 1 && appliedAll.get(1).size() == 1,
                    5000L, "多数派节点应 apply");

            // n3 加入：leader 首轮 prev 超前被闸2 拒绝 → 回带线索跳跃回退 → 全量追平
            network.register("n3", laggard);
            awaitTrue(() -> appliedAll.get(2).size() == 1
                            && laggard.commitIndex() >= index,
                    5000L, "落后的 n3 应经 nextIndex 回退追平");
            assertEquals("before-laggard",
                    new String(appliedAll.get(2).get(0), StandardCharsets.UTF_8));
        } finally {
            for (RaftNode node : nodes) {
                node.close();
            }
        }
    }

    @Test
    public void shouldNeverApplyUncommittedOldTermEntry() throws Exception {
        // 图 8 用例：leader 孤立时追加的旧 term 条目不得被直接提交；
        // 换代后新 leader 的同号条目把它截断——旧条目任何节点都不 apply。
        DirectNetwork network = new DirectNetwork();
        List<RaftNode> nodes = new ArrayList<>();
        List<List<byte[]>> appliedAll = new ArrayList<>();
        try {
            for (String id : new String[]{"n1", "n2", "n3"}) {
                List<byte[]> applied = new CopyOnWriteArrayList<>();
                RaftNode node = newNode(id, new String[]{"n1", "n2", "n3"},
                        network, applied, null, 0L);
                network.register(id, node);
                nodes.add(node);
                appliedAll.add(applied);
                node.start();
            }
            awaitTrue(() -> nodes.stream().filter(n -> n.role() == RaftRole.LEADER).count() == 1,
                    8000L, "三节点应选出唯一 leader");
            RaftNode leader = nodes.stream()
                    .filter(n -> n.role() == RaftRole.LEADER).findFirst().get();
            long term1 = leader.term();
            String isolatedId = leader.nodeId();

            // 孤立 leader（出站 + 入站全断）：无法取得多数派，旧 term 条目悬空
            network.isolate(isolatedId);
            CompletableFuture<Long> pending = leader.propose(next ->
                    "old-term-write".getBytes(StandardCharsets.UTF_8));

            // n2/n3 互联换代（n1 的拉票/应答全丢，不干扰计票）
            awaitTrue(() -> nodes.stream()
                            .filter(n -> n.role() == RaftRole.LEADER && n.term() > term1)
                            .count() == 1,
                    8000L, "存活节点应选出更高 term 的 leader");
            RaftNode newLeader = nodes.stream()
                    .filter(n -> n.role() == RaftRole.LEADER && n.term() > term1)
                    .findFirst().get();
            assertFalse("孤立的旧 leader 不得携带更高 term 再次当选",
                    isolatedId.equals(newLeader.nodeId()));

            // 解隔离：新 leader 的 AppendEntries 把冲突尾巴截断并教化其退位
            network.heal(isolatedId);
            try {
                pending.get(5, TimeUnit.SECONDS);
                fail("孤立 leader 的在途写应在退位时失败");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof PolyConfigException);
                assertTrue("应为 UNAVAILABLE: " + e.getCause(),
                        ((PolyConfigException) e.getCause()).getErrorCode()
                                == ErrorCode.UNAVAILABLE);
            }

            // 收敛 = 条数与 term 双条件：旧 leader 截断前也是 2 条（index=2 还是旧 term）
            awaitTrue(() -> nodes.stream().allMatch(n -> n.testLastIndex() == 2
                            && n.testTermAt(2) == newLeader.term()),
                    5000L, "全体日志应收敛（index=2 = 新 term 的 no-op）");
            for (int i = 0; i < nodes.size(); i++) {
                RaftNode node = nodes.get(i);
                assertEquals("index=2 应为新 term 的 no-op",
                        newLeader.term(), node.testTermAt(2));
                for (byte[] payload : appliedAll.get(i)) {
                    assertFalse("旧 term 未提交条目不得 apply: "
                                    + new String(payload, StandardCharsets.UTF_8),
                            new String(payload, StandardCharsets.UTF_8)
                                    .contains("old-term-write"));
                }
            }
        } finally {
            for (RaftNode node : nodes) {
                node.close();
            }
        }
    }

    @Test
    public void shouldApplyCommittedLogOnSingleNodeRecovery() throws Exception {
        Path dir = folder.newFolder("recovery").toPath();
        RaftLog raftLog = new RaftLog(dir);
        for (int i = 1; i <= 3; i++) {
            raftLog.append(1, ("cmd-" + i).getBytes(StandardCharsets.UTF_8));
        }
        long lastIndex = raftLog.lastIndex();
        raftLog.close();

        List<byte[]> applied = new CopyOnWriteArrayList<>();
        // 单成员恢复：构造时 commitIndex=lastIndex，立即 apply（重启读路径无空窗）
        RaftLog reopened = new RaftLog(dir);
        RaftNode node = newNode("n1", new String[]{"n1"}, new DirectNetwork(),
                applied, reopened, lastIndex);
        try {
            assertEquals(3, applied.size());
            assertEquals("cmd-1", new String(applied.get(0), StandardCharsets.UTF_8));
            assertEquals("cmd-3", new String(applied.get(2), StandardCharsets.UTF_8));
            assertEquals(lastIndex, node.commitIndex());
        } finally {
            node.close();
            reopened.close();
        }
    }

    // ==================== CP-4 read-index 与 InstallSnapshot ====================

    @Test
    public void shouldCompleteReadIndexOnLeaderAndRejectOnFollower() throws Exception {
        DirectNetwork network = new DirectNetwork();
        List<RaftNode> nodes = new ArrayList<>();
        try {
            for (String id : new String[]{"n1", "n2", "n3"}) {
                RaftNode node = newNode(id, new String[]{"n1", "n2", "n3"},
                        network, new CopyOnWriteArrayList<>(), null, 0L);
                network.register(id, node);
                nodes.add(node);
                node.start();
            }
            awaitTrue(() -> nodes.stream().filter(n -> n.role() == RaftRole.LEADER).count() == 1,
                    8000L, "三节点应选出唯一 leader");
            RaftNode leader = nodes.stream()
                    .filter(n -> n.role() == RaftRole.LEADER).findFirst().get();
            RaftNode follower = nodes.stream().filter(n -> n != leader).findFirst().get();

            // leader：确认轮完成，基线被状态机覆盖
            long readIndex = leader.readIndex().get(3, TimeUnit.SECONDS);
            assertTrue("readIndex 应覆盖已提交位点: " + readIndex, readIndex >= 1);
            assertTrue("完成时 lastApplied 应覆盖基线", leader.lastApplied() >= readIndex);

            // follower：角色闸拒绝（UNAVAILABLE，不返回任何值）
            try {
                follower.readIndex().get(3, TimeUnit.SECONDS);
                fail("follower 的 read-index 应被拒绝");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof PolyConfigException);
                assertEquals(ErrorCode.UNAVAILABLE,
                        ((PolyConfigException) e.getCause()).getErrorCode());
            }
        } finally {
            for (RaftNode node : nodes) {
                node.close();
            }
        }
    }

    @Test
    public void shouldFailReadIndexWhenLeaderIsolated() throws Exception {
        // 多数派不可达：确认轮凑不齐 → 超时失败而非回退读旧值
        DirectNetwork network = new DirectNetwork();
        List<RaftNode> nodes = new ArrayList<>();
        String isolatedId = null;
        try {
            for (String id : new String[]{"n1", "n2", "n3"}) {
                RaftNode node = newNode(id, new String[]{"n1", "n2", "n3"},
                        network, new CopyOnWriteArrayList<>(), null, 0L);
                network.register(id, node);
                nodes.add(node);
                node.start();
            }
            awaitTrue(() -> nodes.stream().filter(n -> n.role() == RaftRole.LEADER).count() == 1,
                    8000L, "三节点应选出唯一 leader");
            RaftNode leader = nodes.stream()
                    .filter(n -> n.role() == RaftRole.LEADER).findFirst().get();
            isolatedId = leader.nodeId();
            network.isolate(isolatedId);

            try {
                leader.readIndex().get(5, TimeUnit.SECONDS);
                fail("孤立 leader 的 read-index 应失败（凑不齐多数派确认）");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof PolyConfigException);
                assertEquals(ErrorCode.UNAVAILABLE,
                        ((PolyConfigException) e.getCause()).getErrorCode());
            }
        } finally {
            if (isolatedId != null) {
                network.heal(isolatedId);
            }
            for (RaftNode node : nodes) {
                node.close();
            }
        }
    }

    @Test
    public void shouldCatchUpLaggardViaInstallSnapshot() throws Exception {
        DirectNetwork network = new DirectNetwork();
        List<RaftNode> nodes = new ArrayList<>();
        List<List<byte[]>> appliedAll = new ArrayList<>();
        List<StubSnapshotAccess> accesses = new ArrayList<>();
        try {
            // n3 落在后面：不注册不启动（空日志，且落后于 leader 的快照边界）
            for (String id : new String[]{"n1", "n2", "n3"}) {
                List<byte[]> applied = new CopyOnWriteArrayList<>();
                StubSnapshotAccess access = new StubSnapshotAccess();
                RaftNode node = newNode(id, new String[]{"n1", "n2", "n3"},
                        network, applied, null, 0L, access);
                if (!id.equals("n3")) {
                    network.register(id, node);
                    node.start();
                }
                nodes.add(node);
                appliedAll.add(applied);
                accesses.add(access);
            }
            awaitTrue(() -> nodes.stream().filter(n -> n.role() == RaftRole.LEADER).count() == 1,
                    8000L, "两存活节点应选出 leader（2/3 多数）");
            RaftNode leader = nodes.stream()
                    .filter(n -> n.role() == RaftRole.LEADER).findFirst().get();
            int leaderIndex = nodes.indexOf(leader);

            for (int i = 1; i <= 5; i++) {
                final int n = i;
                leader.propose(next ->
                        ("snap-write-" + n).getBytes(StandardCharsets.UTF_8))
                        .get(3, TimeUnit.SECONDS);
            }
            // no-op 占 1 号，业务写 2..6 → 快照压到 5（覆盖 2..5）
            long snapshotIndex = 5L;
            long snapshotTerm = leader.testTermAt(snapshotIndex);
            SnapshotData data = new SnapshotData();
            data.setRevision(snapshotIndex);
            data.setLastIncludedIndex(snapshotIndex);
            data.setLastIncludedTerm(snapshotTerm);
            data.setEntries(new ArrayList<>());
            data.setHistories(new ArrayList<>());
            accesses.get(leaderIndex).data = data;
            leader.advanceSnapshot(snapshotIndex, snapshotTerm);
            leader.awaitStateIdle();
            assertTrue("leader 快照位点应推进", leader.snapshotIndex() >= snapshotIndex);

            // n3 加入：nextIndex 回退触到快照边界 → InstallSnapshot → 追平
            RaftNode laggard = nodes.get(2);
            network.register("n3", laggard);
            laggard.start();
            awaitTrue(() -> laggard.snapshotIndex() == snapshotIndex,
                    5000L, "落后的 n3 应安装快照");
            awaitTrue(() -> containsPayload(appliedAll.get(2), "snap-write-5"),
                    5000L, "n3 应经快照边界后继续追日志并 apply");
            awaitTrue(() -> laggard.testLastIndex() == leader.testLastIndex(),
                    5000L, "n3 日志尾应追平 leader");
            assertEquals(snapshotIndex, laggard.snapshotIndex());
            assertTrue("快照覆盖的业务写不再走 apply 流（由快照状态承载）",
                    !containsPayload(appliedAll.get(2), "snap-write-1"));
        } finally {
            for (RaftNode node : nodes) {
                node.close();
            }
        }
    }

    // ==================== 辅助 ====================

    private RaftNode newNode(String nodeId, String[] memberIds, DirectNetwork network,
                             List<byte[]> applied, RaftLog existingLog,
                             long recoveryCommit) {
        return newNode(nodeId, memberIds, network, applied, existingLog,
                recoveryCommit, null);
    }

    private RaftNode newNode(String nodeId, String[] memberIds, DirectNetwork network,
                             List<byte[]> applied, RaftLog existingLog,
                             long recoveryCommit, RaftSnapshotAccess snapshotAccess) {
        StringBuilder members = new StringBuilder();
        for (int i = 0; i < memberIds.length; i++) {
            if (i > 0) {
                members.append(',');
            }
            members.append(memberIds[i]).append("=127.0.0.1:")
                    .append(18000 + i).append(':').append(19000 + i);
        }
        List<ClusterMember> memberList = ClusterMember.parseList(members.toString());
        try {
            Path raftDir = folder.newFolder("raft-" + nodeId + "-" + System.nanoTime())
                    .toPath();
            RaftLog log = existingLog != null ? existingLog : new RaftLog(raftDir);
            RaftMetaStore metaStore = new RaftMetaStore(raftDir.resolve("meta.json"));
            // 绑定发送方视图：DirectNetwork 的隔离检查依赖发送方身份
            return new RaftNode(nodeId, memberList, log, metaStore,
                    network.viewOf(nodeId),
                    (index, term, payload) -> applied.add(payload.clone()),
                    ELECTION_TIMEOUT, HEARTBEAT, recoveryCommit,
                    0L, 0L, snapshotAccess);
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static boolean containsPayload(List<byte[]> applied, String expected) {
        for (byte[] payload : applied) {
            if (expected.equals(new String(payload, StandardCharsets.UTF_8))) {
                return true;
            }
        }
        return false;
    }

    /**
     * 测试用快照访问口：leader 侧由测试注入待下发快照，follower 侧记录安装结果。
     */
    private static final class StubSnapshotAccess implements RaftSnapshotAccess {

        private volatile SnapshotData data;

        @Override
        public SnapshotData currentSnapshot() {
            return data;
        }

        @Override
        public void installSnapshot(SnapshotData snapshot) {
            this.data = snapshot;
        }
    }

    private static void awaitTrue(Check condition, long timeoutMillis, String message)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (condition.get()) {
                return;
            }
            Thread.sleep(20L);
        }
        fail(message);
    }

    private static void assertNoDualLeaders(List<RaftNode> nodes) {
        long leaderTerm = -1;
        boolean found = false;
        for (RaftNode node : nodes) {
            if (node.role() == RaftRole.LEADER) {
                if (found && node.term() == leaderTerm) {
                    fail("出现同 term 双主: term=" + leaderTerm);
                }
                found = true;
                leaderTerm = node.term();
            }
        }
    }

    private interface Check {
        boolean get();
    }
}
