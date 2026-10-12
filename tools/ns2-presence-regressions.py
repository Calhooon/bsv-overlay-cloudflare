#!/usr/bin/env python3
"""Behavior REDs in a disposable copy; never checkout or mutate the lane.
Run one foreground group (under ten minutes): m2, l1l2, l3, scope, presence.
Second fold: delta-h1, delta-finality, delta-m1, delta-l1, delta-l2,
delta-height, delta-l3.
The presence mutant grafts the actual base courier function onto the native
port, rather than claiming a compile failure proves a behavioral regression.
"""
import argparse
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
DEADLINE = time.monotonic() + 570

ROOT = Path(__file__).resolve().parents[1]
GROUP = argparse.ArgumentParser()
GROUP.add_argument('group', choices=['fold3-m1', 'm2', 'l1l2', 'l3', 'presence', 'scope', 'delta-h1', 'delta-finality', 'delta-m1', 'delta-l1', 'delta-l2', 'delta-height', 'delta-l3'])
GROUP = GROUP.parse_args().group

def replace_function(text, name, transform):
    start = text.index(name)
    brace = text.index('{', start)
    # Rust bodies here contain only balanced string/comment braces.
    depth = 1
    end = brace + 1
    while depth:
        depth += (text[end] == '{') - (text[end] == '}')
        end += 1
    return text[:brace + 1] + transform(text[brace + 1:end - 1]) + text[end - 1:]

with tempfile.TemporaryDirectory(prefix='ns2-neutered-') as tmp:
    dest = Path(tmp) / 'repo'
    shutil.copytree(ROOT, dest, ignore=shutil.ignore_patterns('.git', 'target', 'node_modules', '.wrangler', 'build'))
    broad = dest / 'crates/overlay-cloudflare/src/broadcaster.rs'
    ops = dest / 'crates/overlay-cloudflare/src/ops.rs'
    routes = dest / 'crates/low-app-layer/src/routes.rs'
    txany = dest / 'crates/low-app-layer/src/txany.rs'
    if GROUP == 'fold3-m1':
        s = broad.read_text()
        start = s.index('async fn submit_once_and_gate(')
        s = s[:start] + s[start:].replace('self.evicted_at_ms,', 'None,', 1)
        broad.write_text(s)
        cases = [('bsv-overlay-cloudflare', 'ns2pd2_m1')]
    elif GROUP.startswith('delta-'):
        if GROUP == 'delta-h1':
            txany.write_text(replace_function(txany.read_text(), 'pub fn fact_reader_raw_hex(', lambda _: '\n    if answer.hint_source.is_some() { None } else { answer.raw_hex.as_deref() }\n'))
            cases = [('low-app-layer', 'ns2pd_h1')]
        elif GROUP == 'delta-finality':
            owed = dest / 'crates/low-app-layer/src/owed.rs'
            s = owed.read_text()
            start = s.index('Some(w) if w.looked && w.spent == Some(true) =>')
            s = s[:start] + s[start:].replace('facts["claim"] = Value::Null;', 'facts["claim"] = json!("release");', 1)
            owed.write_text(s)
            cases = [('low-app-layer', 'ns2pd_h1')]
        elif GROUP == 'delta-m1':
            broad.write_text(replace_function(broad.read_text(), 'fn submit_echo_is_fresh(', lambda _: '\n    let _ = (stamp, now_ms, evicted_at_ms); true\n'))
            cases = [('bsv-overlay-cloudflare', 'ns2pd_m1')]
        elif GROUP == 'delta-l1':
            broad.write_text(replace_function(broad.read_text(), 'fn submit_echo_is_fresh(', lambda _: '\n    let Some(stamp) = stamp else { return true };\n    crate::proof_fetcher::rfc3339_utc_ms(stamp).is_some_and(|at| at <= now_ms + 60_000 && now_ms - at < 30 * 60_000 && evicted_at_ms.is_none_or(|evicted| at > evicted))\n'))
            cases = [('bsv-overlay-cloudflare', 'ns2pd_l1')]
        elif GROUP == 'delta-l2':
            s = routes.read_text()
            start = s.index('crate::courier::note_presence("tx_any_courier_proof_fault_total")')
            stop = s.index('\n            };', start)
            s = s[:start] + 'TxObservation::Fault' + s[stop:]
            routes.write_text(s)
            cases = [('low-app-layer', 'ns2pd_l2')]
        elif GROUP == 'delta-height':
            routes.write_text(routes.read_text().replace('a.claimed_height = claimed_height;', 'a.claimed_height = None;'))
            cases = [('low-app-layer', 'ns2pd_l2')]
        else:
            routes.write_text(replace_function(routes.read_text(), 'fn arcade_anchor_probe_due(', lambda _: '\n    let _ = now_ms; true\n'))
            cases = [('low-app-layer', 'ns2pd_l3')]
    elif GROUP == 'm2':
        base = subprocess.check_output(['git', 'show', '4634090:crates/overlay-cloudflare/src/broadcaster.rs'], cwd=ROOT, text=True)
        arm = base[base.index('async fn submit_once_and_gate('):]
        arm = arm[arm.index('GateVerdict::Reached =>'):arm.index('GateVerdict::Fatal =>')]
        assert 'return Ok(GateStep::Accepted);' in arm
        s = replace_function(broad.read_text(), 'fn reached_submit_echo(', lambda _: '\n    let _ = fast_answer;\n    GateStep::Accepted\n')
        s = s.replace('return Ok(reached_submit_echo(self.fast_answer));', 'return Ok(GateStep::Accepted);')
        broad.write_text(s)
        cases = [('bsv-overlay-cloudflare', 'ns2_m2'), ('bsv-overlay-cloudflare', 'ns2_612_' ), ('bsv-overlay-cloudflare', 'the_fast_answer_is_off_by_default_and_sits_after')]
    elif GROUP == 'l1l2':
        broad.write_text(replace_function(broad.read_text(), 'fn submit_echo_is_fresh(', lambda _: '\n    let _ = (stamp, now_ms, evicted_at_ms);\n    true\n'))
        ops.write_text(replace_function(ops.read_text(), 'pub fn pending_admission_counter(', lambda _: '\n    let _ = duplicate;\n    if fast { COUNTER_SUBMIT_FAST_ADMITTED } else { COUNTER_SUBMIT_PENDING_ADMITTED }\n'))
        cases = [('bsv-overlay-cloudflare', 'ns2_l1'), ('bsv-overlay-cloudflare', 'ns2_l2')]
    elif GROUP == 'l3':
        s = txany.read_text().replace('&["SEEN_ON_NETWORK", "SEEN_MULTIPLE_NODES"]', '&["SEEN_ON_NETWORK", "SEEN_MULTIPLE_NODES", "ACCEPTED"]')
        s = replace_function(s, 'pub fn parse_arcade_subject_word(', lambda _: '\n    let _ = txid;\n    std::str::from_utf8(body).ok().map(|body| parse_arcade_word(status, body, now_ms)).unwrap_or(ArcadeWord::Unknown)\n')
        txany.write_text(s)
        cases = [('low-app-layer', 'ns2_l3'), ('low-app-layer', 'ns2_614_subject_binding')]
    elif GROUP == 'scope':
        s = routes.read_text().replace('let (ours, anchor) = tx_any_scope(env, txid_lc).await;', 'let (ours, anchor) = (true, None::<String>);').replace('WHERE t.has_proof = 1 LIMIT 1', 'WHERE 1 = 1 LIMIT 1')
        s = s.replace('const TX_ANY_OURS_SQL: &str = ', 'const TX_ANY_UNUSED_OURS_SQL: &str = ', 1)
        s += '\nconst TX_ANY_OURS_SQL: &str = "SELECT ? AS txid UNION ALL SELECT ? UNION ALL SELECT ? LIMIT 1";\n'
        # A neutered binding compiles but the source pin must catch its use.
        s = s.replace('crate::txany::mined_proof_matches(', 'ns2_unbound_mined_claim(')
        s += '\nfn ns2_unbound_mined_claim(_: &str, _: &str, _: u64, _: &str) -> bool { true }\n'
        routes.write_text(s)
        txany.write_text(replace_function(txany.read_text().replace('|| memo.evidence.contains("arcade 404 + woc 404")', '|| false'), 'pub fn fact_reader_raw_hex(', lambda _: '\n    answer.raw_hex.as_deref()\n'))
        cases = [('low-app-layer', 'ns2_m1_scope_and_held'), ('low-app-layer', 'ns2_m1_l4_production'), ('low-app-layer', 'ns2_614_both_false_absence')]
    else:
        base = subprocess.check_output(['git', 'show', '4634090:crates/low-app-layer/src/routes.rs'], cwd=ROOT, text=True)
        assert 'if a.present == Some(false) && absence == crate::txany::AbsenceCorroboration::CorroboratedAbsent' in base
        assert 'write_verdict_memo(' in base[base.index('async fn resolve_tx_any('):]
        a = base.index('async fn tx_any_external_leg_via<')
        b = base.index('/// The in-isolate', a)
        function = base[a:b].replace('tx_any_external_leg_via', 'ns2_base_courier_leg_via')
        s = routes.read_text()
        s = replace_function(s, 'async fn tx_any_presence_leg_via<', lambda _: '\n    let _ = (now_ms, ours, held_anchor, verify);\n    ns2_base_courier_leg_via(txid, get).await\n')
        s += '\nthread_local! { static BITAILS_ROUTE_HEALTHY: std::cell::Cell<Option<bool>> = const { std::cell::Cell::new(None) }; }\n' + function
        routes.write_text(s)
        cases = [('low-app-layer', 'ns2_614_arcade_seen_outranks_courier_404s'), ('low-app-layer', 'ns2_614_arcade_unknown_and_two_404s'), ('low-app-layer', 'ns2_614_seen_without'), ('low-app-layer', 'ns2_614_mined_needs'), ('low-app-layer', 'ns2_m1_foreign_unmined'), ('low-app-layer', 'ns2_m1_mined_foreign'), ('low-app-layer', 'ns2_m1_held_anchor')]
    env = dict(os.environ)
    env.setdefault('CARGO_TARGET_DIR', str(Path.home() / 'bsv/targets/ns-2-presence-fold-2'))
    for package, case in cases:
        run = subprocess.run(['cargo', 'test', '--manifest-path', 'workers/Cargo.toml', '-p', package, '--lib', case, '--quiet'], cwd=dest, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=max(1, min(180, DEADLINE - time.monotonic())))
        print(run.stdout, flush=True)
        assert run.returncode == 101 and 'test result: FAILED' in run.stdout, (case, run.returncode, 'must be behavior RED, never compile RED')
        print(f'RED {case}: exit {run.returncode}', flush=True)
    if GROUP == 'presence':
        # Separately neuter the proof binding, not the async port.
        shutil.copy2(ROOT / 'crates/low-app-layer/src/routes.rs', routes)
        txany.write_text(replace_function(txany.read_text(), 'pub fn mined_proof_matches(', lambda _: '\n    let _ = (txid, bump_hex, height, canonical);\n    true\n'))
        run = subprocess.run(['cargo', 'test', '--manifest-path', 'workers/Cargo.toml', '-p', 'low-app-layer', '--lib', 'ns2_l4_proof_binding', '--quiet'], cwd=dest, env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=max(1, min(180, DEADLINE - time.monotonic())))
        print(run.stdout, flush=True)
        assert run.returncode == 101 and 'test result: FAILED' in run.stdout
        print('RED ns2_l4_proof_binding: exit 101', flush=True)
