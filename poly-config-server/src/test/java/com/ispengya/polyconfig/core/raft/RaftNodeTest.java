package com.ispengya.polyconfig.core.raft;

import com.ispengya.polyconfig.common.error.ErrorCode;
import com.ispengya.polyconfig.common.error.PolyConfigException;
import com.ispengya.polyconfig.common.protocol.AppendEntriesRequest;
import com.ispengya.polyconfig.common.protocol.AppendEntriesResponse;
import com.ispengya.polyconfig.common.protocol.VoteRequest;
import com.ispengya.polyconfig.common.protocol.VoteResponse;
import com.ispengya.polyconfig.consistency.ClusterMember;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * {@link RaftNode} 单元测试（CP-2 验收：投票决策矩阵、规则一/规则二、
 * 单节点选举与写入、三节点唯一 leader 与换代）。
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

    @Test
    public void shouldRejectMultiNodeProposeUntilCp3() throws Exception {
        DirectNetwork network = new DirectNetwork();
        RaftNode n1 = newNode("n1", new String[]{"n1", "n2"}, network,
                new CopyOnWriteArrayList<>(), null, 0L);
        RaftNode n2 = newNode("n2", new String[]{"n1", "n2"}, network,
                new CopyOnWriteArrayList<>(), null, 0L);
        network.register("n1", n1);
        network.register("n2", n2);
        try {
            // n1 竞选：n2 未 start 但会应答拉票 → n1 拿到 2/2 当选
            n1.forceElectionTimeout();
            awaitTrue(() -> n1.role() == RaftRole.LEADER, 5000L, "n1 应当选");
            try {
                n1.propose(next -> new byte[]{1}).get(3, TimeUnit.SECONDS);
                fail("CP-2 多节点写应明确拒绝");
            } catch (ExecutionException e) {
                assertTrue(e.getCause() instanceof PolyConfigException);
                assertTrue("错误信息应指向 CP-3: " + e.getCause().getMessage(),
                        e.getCause().getMessage().contains("CP-3"));
            }
        } finally {
            n1.close();
            n2.close();
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

    // ==================== 辅助 ====================

    private RaftNode newNode(String nodeId, String[] memberIds, DirectNetwork network,
                             List<byte[]> applied, RaftLog existingLog,
                             long recoveryCommit) {
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
            return new RaftNode(nodeId, memberList, log, metaStore, network,
                    (index, term, payload) -> applied.add(payload.clone()),
                    ELECTION_TIMEOUT, HEARTBEAT, recoveryCommit);
        } catch (java.io.IOException e) {
            throw new IllegalStateException(e);
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
