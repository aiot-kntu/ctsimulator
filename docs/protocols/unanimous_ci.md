# Hardware-replica unanimous agreement on CI (`unanimous_pipeline`)

**Code:** `src/sim/unanimous_pipeline.rs`  
**PHY:** `flood_identical_round` (with the optional per-node whole-flood loss)  
**Config:** `protocol = "unanimous_pipeline"`, `phy_mode = "ci"`, optional `[hw]` section  
**Profiles:** `profiles/hardware/`

---

## 1. Goal

Reproduce, round by round, the pipelined unanimous agreement that ran on the
nRF52840 / BlueFlood testbed, so simulated and measured runs can be compared
like for like. It differs from `2pc_pipeline` in four ways:

| | `2pc_pipeline` | `unanimous_pipeline` (firmware) |
|---|---|---|
| Missing vote | aborts at the deadline (N rounds) | never aborts; waits for recovery |
| Recovery | separate piggyback field, no extra round | the requested proposal is **replayed in place of a new proposal** |
| Round length | ends when the flood dies out | fixed slot schedule (`round_slots`) |
| Loss | per link attempt (`loss_rate`) | per link attempt and/or per node whole-flood (`hw.node_flood_loss`) |

---

## 2. Packet (`UPacket`)

```text
sender:  usize
record:  New { rid, round } | Replay { rid, round } | Idle
request: Option<u64>     # lowest proposal id the sender is missing
acks:    Vec<u64>        # proposals the sender acknowledges
```

---

## 3. Round loop

Initiator of round `r` is `r mod N`.

1. **Record.** If a replay obligation is pending and the initiator holds the
   proposal, it floods `Replay`; the obligation counts down by one round
   either way. Otherwise it floods `New` with the next proposal id, until
   `num_proposals` have been offered; then `Idle`.
2. **Request.** The initiator names the lowest proposal id it knows exists but
   does not hold. **One proposal per turn**, as in the firmware.
3. **Acks.** The initiator advertises its own ack for every proposal it has
   held and that was proposed less than `W` rounds ago, **delivered or not**.
   Acks are sender-owned: a node never forwards another node's ack.

After the flood, every node that received it:

- stores the proposal of a `New` / `Replay` record;
- sets the sender's bit in its ack mask of each acked proposal;
- **commits** a proposal once its mask has all N bits;
- **delivers** committed proposals in id order (a committed proposal waits for
  every earlier one).

A request starts a replay obligation of `max_replays` rounds for the next
initiators (one obligation per proposal; obligations queue).

**Halt.** If the oldest undelivered proposal at any node is still uncommitted
`W` rounds after it was proposed, the run stops (`halted_at`). No node ever
delivers a different sequence; the halt is the firmware's safety stop.

A node's own ack counts from its own turn, so without loss every node commits a
proposal in the same round, `N` rounds after it was proposed.

---

## 4. `[hw]` configuration

| Key | Meaning | Default |
|---|---|---|
| `ack_window` | W, rounds a proposal may stay uncommitted | `2N` |
| `max_replays` | replay rounds per request | `N - 1` |
| `node_flood_loss` | per-node probability of missing a whole flood; length N; the initiator is never deaf | none |
| `trace_csv` | per-node decision trace | none |

`node_flood_loss` is applied in the CI PHY and is honoured by every CI
pipeline. When it is absent no random number is drawn, so outputs of all other
configurations are unchanged.

---

## 5. Output

- `results/results_unanimous.csv`: one row per proposal, start slot to the slot
  at which the last node committed.
- `trace_csv`: `node, rid, proposer, propose_round, commit_round, deliver_round`.
  Commit latency in rounds is `commit_round - propose_round + 1`; delivery
  latency includes the wait for earlier proposals.
- Summary line `HW replica: rounds=… proposals=… requests=… replay_rounds=…
  idle_rounds=… halted_at=…`.

---

## 6. Profiles

`profiles/hardware/` holds the eight-, twelve- and nineteen-node builds without
loss, the twelve- and nineteen-node runs with the per-board loss measured on
the testbed, and the first nineteen-node trial with a 32-round window, which
halts early.
