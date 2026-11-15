//! Hardware-replica pipelined unanimous agreement — implements [`CiProtocol`].
//!
//! Mirrors the nRF52840 / BlueFlood firmware used in the accompanying paper's
//! hardware experiments, so simulator and testbed can be compared like for like:
//!
//! - Round-robin initiator. Each round the initiator floods one record: a new
//!   proposal, a replay of a requested proposal, or nothing (drain).
//! - Acknowledgements are **sender-owned**: a node advertises its own ack for
//!   every proposal it holds within the ack window, and only in its own turn.
//!   Receivers never forward another node's ack.
//! - A proposal commits at a node once that node has heard all N acks. A node's
//!   own ack counts from its own turn, so with no loss every node commits a
//!   proposal in the same round, N rounds after it was proposed.
//! - Delivery is in proposal order: a committed proposal waits for every
//!   earlier one.
//! - A node that detects a missing proposal (gap in proposal ids) requests it
//!   in its next turn, one proposal (the lowest missing) per turn, as in the
//!   firmware. The following `max_replays` initiators that hold it
//!   replay it **in place of** a new proposal.
//! - A proposal that is still uncommitted at some node `ack_window` rounds after
//!   it was proposed halts the protocol (consistent-state halt).
//!
//! Loss enters through the PHY: `network.loss_rate` per link attempt and
//! `hw.node_flood_loss` per node and round (whole flood missed).

use crate::config::SimConfig;
use crate::metrics::MetricsCollector;
use crate::network::NetworkGraph;
use crate::protocol::ProposalOutcome;
use crate::sim::pipeline::{run_ci_pipeline, CiPipelineCore, CiProtocol};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::io::Write;

/// Record kind carried by one flood.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Record {
    /// New proposal `rid`, proposed in `round`.
    New { rid: u64, round: u64 },
    /// Replay of proposal `rid`, originally proposed in `round`.
    Replay { rid: u64, round: u64 },
    /// No proposal (drain or no data to replay).
    Idle,
}

/// One flood packet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UPacket {
    pub sender: usize,
    pub record: Record,
    /// Lowest proposal id the sender is missing, if any.
    pub request: Option<u64>,
    /// Proposals the sender acknowledges (holds, within the ack window).
    pub acks: Vec<u64>,
}

/// Per-node protocol state.
#[derive(Debug, Clone, Default)]
pub struct UNode {
    /// Proposals whose data this node holds: rid -> proposal round.
    pub have: BTreeMap<u64, u64>,
    /// Proposals this node acknowledges: held at some point and proposed less
    /// than W rounds ago. Delivery does not remove an entry, only age does,
    /// so a node keeps re-advertising its ack for the whole window.
    pub acked: BTreeMap<u64, u64>,
    /// Ack bitmask heard per proposal (bit j = node j's ack).
    pub acks: BTreeMap<u64, u64>,
    /// Commit round per proposal.
    pub committed: BTreeMap<u64, u64>,
    /// Next proposal id to deliver in order.
    pub next_deliver: u64,
    /// Highest proposal id this node knows exists.
    pub max_seen: Option<u64>,
    /// Proposal round per rid, learned from any packet (needed for the halt
    /// check on proposals the node never received).
    pub known_round: BTreeMap<u64, u64>,
}

impl UNode {
    /// Lowest proposal id that exists but whose data this node lacks.
    pub fn first_missing(&self) -> Option<u64> {
        let max = self.max_seen?;
        (self.next_deliver..=max).find(|r| !self.have.contains_key(r))
    }
}

/// One row of the per-node decision trace.
#[derive(Debug, Clone)]
pub struct TraceRow {
    pub node: usize,
    pub rid: u64,
    pub proposer: usize,
    pub propose_round: u64,
    pub commit_round: u64,
    pub deliver_round: u64,
}

/// Run summary specific to the hardware replica.
#[derive(Debug, Clone, Default)]
pub struct UStats {
    pub rounds: u64,
    pub proposals: u64,
    pub requests: u64,
    pub replay_rounds: u64,
    pub idle_rounds: u64,
    pub halted_at: Option<u64>,
}

pub struct UnanimousPipelineSim {
    pub metrics: MetricsCollector,
    pub trace: Vec<TraceRow>,
    pub stats: UStats,
    core: Option<CiPipelineCore>,
}

impl UnanimousPipelineSim {
    pub fn new(config: SimConfig, graph: NetworkGraph) -> Self {
        Self {
            metrics: MetricsCollector::new(),
            trace: Vec::new(),
            stats: UStats::default(),
            core: Some(CiPipelineCore::new(config, graph)),
        }
    }

    pub fn run(&mut self) -> (MetricsCollector, u64, u64, u64) {
        let core = self.core.take().expect("already run");
        let n = core.num_nodes();
        assert!(n <= 64, "unanimous_pipeline uses u64 ack masks: N <= 64");
        let hw = core.config.hw.clone().unwrap_or_default();
        let window = hw.ack_window.unwrap_or(2 * n as u64);
        if window < 2 * n as u64 && !core.config.quiet {
            println!(
                "WARNING: ack_window {window} < 2N = {}: a lost ack cannot be repeated in time",
                2 * n
            );
        }
        let mut proto = UnanimousCi {
            nodes: vec![UNode::default(); n],
            n,
            window,
            max_replays: hw.max_replays.unwrap_or(n - 1),
            num_proposals: core.config.num_proposals as u64,
            next_rid: 0,
            proposer: Vec::new(),
            start_slot: Vec::new(),
            all_committed_slot: BTreeMap::new(),
            replay: VecDeque::new(),
            round_start_slot: 0,
            trace: Vec::new(),
            stats: UStats::default(),
            stop: false,
        };
        let core = run_ci_pipeline(&mut proto, core);
        self.trace = proto.trace;
        self.stats = proto.stats;
        if let Some(path) = hw.trace_csv.as_deref() {
            if let Err(e) = write_trace(path, &self.trace) {
                eprintln!("Failed to write trace {path}: {e}");
            }
        }
        self.metrics = core.metrics.clone();
        let l = core.nodes.iter().map(|nd| nd.slots_listen).sum();
        let f = core.nodes.iter().map(|nd| nd.slots_flood).sum();
        let s = core.nodes.iter().map(|nd| nd.slots_sleep).sum();
        (core.metrics, l, f, s)
    }
}

fn write_trace(path: &str, rows: &[TraceRow]) -> std::io::Result<()> {
    if let Some(dir) = std::path::Path::new(path).parent() {
        std::fs::create_dir_all(dir)?;
    }
    let mut f = std::io::BufWriter::new(std::fs::File::create(path)?);
    writeln!(
        f,
        "node,rid,proposer,propose_round,commit_round,deliver_round"
    )?;
    for r in rows {
        writeln!(
            f,
            "{},{},{},{},{},{}",
            r.node, r.rid, r.proposer, r.propose_round, r.commit_round, r.deliver_round
        )?;
    }
    Ok(())
}

/// Active replay obligation: proposal `rid`, `left` replay rounds remaining.
#[derive(Debug, Clone, Copy)]
struct Replay {
    rid: u64,
    round: u64,
    left: usize,
}

struct UnanimousCi {
    nodes: Vec<UNode>,
    n: usize,
    window: u64,
    max_replays: usize,
    num_proposals: u64,
    next_rid: u64,
    proposer: Vec<usize>,
    start_slot: Vec<u64>,
    all_committed_slot: BTreeMap<u64, usize>,
    replay: VecDeque<Replay>,
    round_start_slot: u64,
    trace: Vec<TraceRow>,
    stats: UStats,
    stop: bool,
}

impl CiProtocol for &mut UnanimousCi {
    fn name(&self) -> &str {
        "Unanimous Pipeline (hardware replica)"
    }
    fn on_start(&mut self, core: &mut CiPipelineCore) {
        if !core.config.quiet {
            println!(
                "Starting Unanimous Pipeline (hw replica): {} nodes, {} proposals, W={}, replays={}, diameter={}",
                core.num_nodes(),
                self.num_proposals,
                self.window,
                self.max_replays,
                core.graph.diameter()
            );
        }
    }
    fn prepare_round(&mut self, core: &mut CiPipelineCore) {
        self.build_packet(core)
    }
    fn process_round(&mut self, core: &mut CiPipelineCore) {
        self.receive(core)
    }
    fn should_stop(&self, _core: &CiPipelineCore) -> bool {
        self.stop
    }
    fn on_finish(&mut self, core: &mut CiPipelineCore) {
        self.stats.rounds = core.round_num + 1;
        self.stats.proposals = self.next_rid;
        core.metrics.print_summary(&core.nodes, core.config.quiet);
        if !core.config.quiet {
            let s = &self.stats;
            println!(
                "HW replica: rounds={} proposals={} requests={} replay_rounds={} idle_rounds={} halted_at={:?}",
                s.rounds, s.proposals, s.requests, s.replay_rounds, s.idle_rounds, s.halted_at
            );
        }
    }
}

impl UnanimousCi {
    fn build_packet(&mut self, core: &mut CiPipelineCore) {
        let i = core.initiator();
        let r = core.round_num;
        self.round_start_slot = core.current_slot;

        // 1. Record: replay obligation first, else a new proposal, else idle.
        let mut record = Record::Idle;
        if let Some(rp) = self.replay.front_mut() {
            if self.nodes[i].have.contains_key(&rp.rid) {
                record = Record::Replay {
                    rid: rp.rid,
                    round: rp.round,
                };
                self.stats.replay_rounds += 1;
            }
            rp.left -= 1;
            if rp.left == 0 {
                self.replay.pop_front();
            }
        }
        if record == Record::Idle && self.next_rid < self.num_proposals {
            let rid = self.next_rid;
            self.next_rid += 1;
            self.proposer.push(i);
            self.start_slot.push(core.current_slot);
            self.nodes[i].have.insert(rid, r);
            self.nodes[i].acked.insert(rid, r);
            self.nodes[i].known_round.insert(rid, r);
            self.nodes[i].max_seen = Some(self.nodes[i].max_seen.map_or(rid, |m| m.max(rid)));
            record = Record::New { rid, round: r };
        }
        if record == Record::Idle {
            self.stats.idle_rounds += 1;
        }

        // 2. Request the lowest missing proposal in this node's own turn.
        let request = self.nodes[i].first_missing();
        if request.is_some() {
            self.stats.requests += 1;
        }

        // 3. Own acks: every proposal held at some point and still inside the
        //    window, delivered or not.
        let w = self.window;
        self.nodes[i].acked.retain(|_, pr| r < *pr + w);
        let acks: Vec<u64> = self.nodes[i].acked.keys().copied().collect();

        let pkt = UPacket {
            sender: i,
            record,
            request,
            acks,
        };
        core.nodes[i].payload = serde_json::to_vec(&pkt).expect("serialize");
    }

    fn receive(&mut self, core: &mut CiPipelineCore) {
        let r = core.round_num;
        let n = self.n;
        let full: u64 = if n == 64 { u64::MAX } else { (1u64 << n) - 1 };
        let initiator = core.initiator();

        let pkt: UPacket =
            serde_json::from_slice(&core.nodes[initiator].payload).expect("own packet");

        // A request heard this round starts a replay obligation for the next
        // initiators (unless one is already running for the same proposal).
        if let Some(req) = pkt.request {
            let queued = self.replay.iter().any(|rp| rp.rid == req);
            if !queued && self.max_replays > 0 {
                if let Some(&round) = self.nodes[initiator].known_round.get(&req) {
                    self.replay.push_back(Replay {
                        rid: req,
                        round,
                        left: self.max_replays,
                    });
                }
            }
        }

        // Proposal round of every acked proposal, as known to the sender.
        let sender_rounds: Vec<(u64, u64)> = pkt
            .acks
            .iter()
            .filter_map(|rid| self.nodes[pkt.sender].acked.get(rid).map(|&pr| (*rid, pr)))
            .collect();

        for j in 0..n {
            if !core.participated(j) {
                continue;
            }
            let node = &mut self.nodes[j];
            match pkt.record {
                Record::New { rid, round } | Record::Replay { rid, round }
                    if rid >= node.next_deliver =>
                {
                    node.have.insert(rid, round);
                    node.acked.insert(rid, round);
                    node.known_round.insert(rid, round);
                    node.max_seen = Some(node.max_seen.map_or(rid, |m| m.max(rid)));
                }
                _ => {}
            }
            for &(rid, pr) in &sender_rounds {
                if rid < node.next_deliver {
                    continue; // already delivered here
                }
                *node.acks.entry(rid).or_insert(0) |= 1u64 << pkt.sender;
                node.known_round.entry(rid).or_insert(pr);
                node.max_seen = Some(node.max_seen.map_or(rid, |m| m.max(rid)));
            }

            // Commit every proposal with all N acks.
            let newly: Vec<u64> = node
                .acks
                .iter()
                .filter(|(rid, &m)| m == full && !node.committed.contains_key(rid))
                .map(|(&rid, _)| rid)
                .collect();
            for rid in newly {
                node.committed.insert(rid, r);
                let c = self.all_committed_slot.entry(rid).or_insert(0);
                *c += 1;
                if *c == n {
                    core.metrics.record_proposal(
                        rid as usize,
                        self.start_slot[rid as usize],
                        core.current_slot,
                        ProposalOutcome::Committed,
                    );
                }
            }

            // In-order delivery.
            let node = &mut self.nodes[j];
            while let Some(&cr) = node.committed.get(&node.next_deliver) {
                let rid = node.next_deliver;
                self.trace.push(TraceRow {
                    node: j,
                    rid,
                    proposer: self.proposer[rid as usize],
                    propose_round: node.known_round[&rid],
                    commit_round: cr,
                    deliver_round: r,
                });
                node.have.remove(&rid);
                node.acks.remove(&rid);
                node.committed.remove(&rid);
                node.known_round.remove(&rid);
                node.next_deliver += 1;
            }
        }

        // Halt: the oldest undelivered proposal at any node has left the window.
        for node in &self.nodes {
            if let Some(&pr) = node.known_round.get(&node.next_deliver) {
                if r + 1 >= pr + self.window && !node.committed.contains_key(&node.next_deliver) {
                    self.stats.halted_at = Some(r);
                    self.stop = true;
                    if !core.config.quiet {
                        println!(
                            "HALT at round {r}: proposal {} left the ack window",
                            node.next_deliver
                        );
                    }
                    return;
                }
            }
        }

        let done = self
            .nodes
            .iter()
            .all(|nd| nd.next_deliver >= self.num_proposals);
        if done && self.next_rid >= self.num_proposals {
            if !core.config.quiet {
                println!(
                    "All nodes delivered {} proposals at round {r}",
                    self.num_proposals
                );
            }
            self.stop = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CiConfig, HwConfig, NetworkConfig};

    fn config(n: usize, proposals: usize, hw: HwConfig, loss: f64) -> SimConfig {
        SimConfig {
            seed: 7,
            phy_mode: "ci".into(),
            network: NetworkConfig {
                num_nodes: n,
                topology: "full_mesh".into(),
                loss_rate: loss,
                graph_file: None,
            },
            ci: Some(CiConfig {
                flood_repeats: 3,
                round_slots: Some(4),
            }),
            ce: None,
            protocol: "unanimous_pipeline".into(),
            num_proposals: proposals,
            snapshot_interval: 1_000_000,
            max_slots: 10_000_000,
            abort_probability: 0.0,
            quiet: true,
            hw: Some(hw),
        }
    }

    fn run(cfg: SimConfig) -> UnanimousPipelineSim {
        let mut rng = <rand_chacha::ChaCha8Rng as rand::SeedableRng>::seed_from_u64(cfg.seed);
        let graph = crate::run::build_graph(&cfg.network, &mut rng);
        let mut sim = UnanimousPipelineSim::new(cfg, graph);
        sim.run();
        sim
    }

    #[test]
    fn lossless_commits_in_n_rounds_one_per_round() {
        for n in [8usize, 12, 19] {
            let sim = run(config(n, 200, HwConfig::default(), 0.0));
            assert_eq!(sim.stats.halted_at, None);
            assert_eq!(sim.stats.requests, 0);
            assert_eq!(sim.trace.len(), n * 200);
            for row in &sim.trace {
                assert_eq!(row.commit_round - row.propose_round + 1, n as u64, "N={n}");
                assert_eq!(row.deliver_round, row.commit_round);
            }
            // fill: last of 200 proposals (round 199) commits at round 199 + N - 1
            assert_eq!(sim.stats.rounds, 199 + n as u64);
        }
    }

    #[test]
    fn every_node_delivers_in_order() {
        let q = vec![0.02; 12];
        let hw = HwConfig {
            ack_window: Some(32),
            node_flood_loss: Some(q),
            ..Default::default()
        };
        let sim = run(config(12, 2000, hw, 0.0));
        let delivered_per_node = sim.trace.len() / 12;
        assert!(delivered_per_node > 0);
        for n in 0..12 {
            let rids: Vec<u64> = sim
                .trace
                .iter()
                .filter(|r| r.node == n)
                .map(|r| r.rid)
                .collect();
            assert!(
                rids.windows(2).all(|w| w[1] == w[0] + 1),
                "in-order delivery at node {n}"
            );
        }
    }

    #[test]
    fn loss_triggers_request_and_replay() {
        let mut q = vec![0.0; 12];
        q[5] = 0.002; // ~ hardware: 79 lost floods / 7176 rounds on 5 boards
        let hw = HwConfig {
            ack_window: Some(32),
            node_flood_loss: Some(q),
            ..Default::default()
        };
        let sim = run(config(12, 2000, hw, 0.0));
        assert!(sim.stats.requests > 0, "a lossy node must request");
        assert!(sim.stats.replay_rounds > 0, "requests must be replayed");
        assert_eq!(
            sim.stats.halted_at, None,
            "W = 32 >= 2N must not halt at hardware-level loss"
        );
        // Delayed deliveries exist and every delivery is in order.
        assert!(
            sim.trace
                .iter()
                .any(|r| r.deliver_round > r.commit_round
                    || r.commit_round - r.propose_round + 1 > 12)
        );
    }

    #[test]
    fn bounded_replays_are_counted_per_request() {
        let mut q = vec![0.0; 19];
        q[3] = 0.01;
        let hw = HwConfig {
            ack_window: Some(64),
            max_replays: Some(3),
            node_flood_loss: Some(q),
            ..Default::default()
        };
        let sim = run(config(19, 3000, hw, 0.0));
        assert!(sim.stats.requests > 0);
        assert!(sim.stats.replay_rounds <= 3 * sim.stats.requests);
    }

    #[test]
    fn short_window_can_halt() {
        let q = vec![0.03; 19];
        let hw = HwConfig {
            ack_window: Some(32),
            max_replays: Some(3),
            node_flood_loss: Some(q),
            ..Default::default()
        };
        let sim = run(config(19, 5000, hw, 0.0));
        assert!(
            sim.stats.halted_at.is_some(),
            "W = 32 < 2N = 38 should halt under loss"
        );
    }
}
