//! PR 233 (bsv-low #984): THE ONE-WRITER RULE READ FROM A RENDER, per environment. The `owed-events` consumer's
//! concurrency may pass 1 only where the D1 judge is off (Phase C: the actor the one writer, each identity serialised
//! by its own Durable Object); in Phases A and B (`OWED_D1_JUDGE = "true"`) two consumers are two D1 writers of one
//! identity's `owed_rows`, so it is 1. The live consumer block's `max_concurrency` equals `[vars]`'
//! `OWED_CONSUMER_CONCURRENCY` (the Phase C deploy moves both), and the dead letters' consumer is one at a time in
//! every phase. Shared by `d2_actor_a` and `fix952`, each over the committed `wrangler.toml` and a judge-on fixture.

/// One environment's owed-events posture as the render states it.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct EnvPhase {
    /// `""` (the top-level render, prod) or `"env.beta."`.
    pub env: String,
    pub d1_judge: Option<String>,
    pub concurrency_var: Option<u32>,
    /// the live `owed-events` consumer block's `max_concurrency` (absent: none written)
    pub live_max: Option<u32>,
    /// the dead letters' consumer block's `max_concurrency`
    pub dlq_max: Option<u32>,
    /// owed-events consumer blocks seen (the live queue's and its dead letters')
    pub blocks: usize,
}

fn quoted(line: &str) -> Option<String> {
    line.split('"').nth(1).map(str::to_string)
}

fn number(line: &str) -> Option<u32> {
    line.split('=').nth(1)?.trim().parse().ok()
}

/// The posture of `env` (`""` or `"env.beta."`) in `render`, read by section header.
pub fn env_phase(render: &str, env: &str) -> EnvPhase {
    let vars = format!("[{env}vars]");
    let consumers = format!("[[{env}queues.consumers]]");
    let mut out = EnvPhase {
        env: env.to_string(),
        ..EnvPhase::default()
    };
    let mut section = String::new();
    // inside an owed-events consumer block: Some(is the dead letters')
    let mut block: Option<bool> = None;
    for line in render.lines() {
        let l = line.trim();
        if l.starts_with('[') {
            section = l.to_string();
            block = None;
            continue;
        }
        if section == vars {
            if l.starts_with("OWED_D1_JUDGE = \"") {
                out.d1_judge = quoted(l);
            } else if l.starts_with("OWED_CONSUMER_CONCURRENCY = \"") {
                out.concurrency_var = quoted(l).and_then(|v| v.parse().ok());
            }
        } else if section == consumers {
            if l.starts_with("queue = \"") && l.contains("owed-events") {
                block = Some(l.contains("-dlq"));
                out.blocks += 1;
            } else if l.starts_with("max_concurrency =") {
                match block {
                    Some(false) => out.live_max = number(l),
                    Some(true) => out.dlq_max = number(l),
                    None => {}
                }
            }
        }
    }
    out
}

/// Every breach of the one-writer rule in `p`; empty when the environment's render is sound.
pub fn breaches(p: &EnvPhase) -> Vec<String> {
    let env = &p.env;
    let mut v = Vec::new();
    if p.blocks != 2 {
        v.push(format!(
            "{env}: {} owed-events consumer blocks (the queue's and its dead letters')",
            p.blocks
        ));
    }
    if p.dlq_max != Some(1) {
        v.push(format!(
            "{env}: the dead letters' consumer one at a time: {:?}",
            p.dlq_max
        ));
    }
    let judge_off = match p.d1_judge.as_deref() {
        Some("true") => false,
        Some("false") => true,
        other => {
            v.push(format!("{env}: OWED_D1_JUDGE is {other:?}"));
            false
        }
    };
    if p.live_max != p.concurrency_var || p.live_max.is_none() {
        v.push(format!(
            "{env}: the consumer's max_concurrency {:?} equals OWED_CONSUMER_CONCURRENCY {:?}",
            p.live_max, p.concurrency_var
        ));
    }
    if p.live_max.unwrap_or(1) > 1 && !judge_off {
        v.push(format!(
            "{env}: one consumer at a time unless the D1 judge is off: max_concurrency {:?} with OWED_D1_JUDGE {:?}",
            p.live_max, p.d1_judge
        ));
    }
    v
}

/// The judge-on fixture the inverse pins read: concurrency 4 with the D1 judge writing (Phases A and B).
pub const JUDGE_ON_AT_FOUR: &str = r#"
[vars]
OWED_ACTOR_READ = "true"
OWED_D1_JUDGE = "true"
OWED_CONSUMER_CONCURRENCY = "4"

[[queues.consumers]]
queue = "low-owed-events"
max_batch_size = 16
# PHASE C: max_concurrency = 4
max_concurrency = 4

[[queues.consumers]]
queue = "low-owed-events-dlq"
max_concurrency = 1
"#;
