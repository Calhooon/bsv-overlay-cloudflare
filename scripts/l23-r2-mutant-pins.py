#!/usr/bin/env python3
"""L23R2D-L4: assertion REDs against compiling copies of the shipped behavior."""
from pathlib import Path
import os
import shutil
import subprocess
import sys

source = Path(__file__).resolve().parents[1] if Path(__file__).parent.name == 'scripts' else Path.cwd()
copy = source.parent / 'l23-r2-fold-2-red'
if not copy.exists():
    subprocess.run(['git', 'clone', '--no-hardlinks', '--quiet', str(source), str(copy)], check=True, timeout=60)
logs = Path('/tmp/l23-r2-fold-2-mutants')
logs.mkdir(exist_ok=True)
for name in subprocess.check_output(['git', 'ls-files'], cwd=source, text=True).splitlines():
    src = source / name
    if src.is_file():
        dst = copy / name
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
file = copy / 'crates/low-app-layer/src/owed.rs'
base = file.read_text()

def early_return(text, function, signature, expression):
    start = text.index('pub fn ' + function)
    at = text.index(signature, start) + len(signature)
    return text[:at] + '\n    return ' + expression + ';\n' + text[at:]

cases = {
    'core': ('--lib', 'l23_r2_a_released_hop', lambda s: early_return(s, 'markerless_released_hops', ') -> Vec<HopEntry> {', 'Vec::new()')),
    'synthetic': ('--test', 'l23_r2_synthetic', lambda s: s[:s.index('pub fn markerless_released_hops')] + s[s.index('pub fn markerless_released_hops'):].replace('.filter(|h| h.marker_verified == MarkerVerification::Verified)', '.filter(|_| true)', 1)),
    'ring': ('--test', 'l23_r2_identity_cursor', lambda s: s.replace('CASE WHEN e.txid > ?2', "CASE WHEN e.txid > ''")),
    'captured': ('--test', 'l23_r2_captured', lambda s: early_return(s, 'hop_outputs_from_join_beef', ') -> HashMap<String, (Vec<u8>, u64)> {', 'HashMap::new()')),
    'faulted': ('--test', 'l23_r2_faulted', lambda s: early_return(s, 'markerless_faulted_rows', '-> Vec<OwedRow> {', 'rows')),
    'M1': ('--test', 'l23_r2_delta_missing', lambda s: s.replace('if let Err(reason) = attempt {', 'if let Err(reason) = { let _ = attempt; Ok::<(), String>(()) } {')),
    'M1-overlap': ('--test', 'l23_r2_delta_missing', lambda s: s.replace('if !replace {', 'if false {', 1)),
    'L1': ('--test', 'l23_r2_identity_cursor', lambda s: s.replace('total > examined && (carried > 0 || (rebuilt > 0 && !ring_complete))', 'total > examined')),
    'L3': ('--test', 'l23_r2_delta_readmitted', lambda s: s.replace('active.contains(join) &&', 'true &&')),
    'N3': ('--test', 'l23_r2_identity_cursor', lambda s: s.replace('e.txid LIMIT ?3"', 'e.txid LIMIT ?3 + 1"')),
    'N4': ('--test', 'l23_r2_synthetic', lambda s: s.replace('h.marker_verified == MarkerVerification::Verified\n            ||', 'h.marker_verified != MarkerVerification::Unverified\n            ||', 1)),
}
if sys.argv[1:] == ['--route']:
    # Six independently named route assertions: no rebuilt evidence/ignored count/twin seat,
    # full-scan index reads, and missing candidate fault rows on first and cached reads.
    file.write_text(cases['M1'][2](cases['core'][2](base)))
    routes = copy / 'crates/low-app-layer/src/routes.rs'
    text = routes.read_text()
    start = text.index("struct Reads<'a>")
    end = text.index('let mut reads = Reads(db)', start)
    arm = text[start:end].replace('FROM pot_records WHERE txid = ?1', 'FROM pot_records WHERE lower(txid) = ?1')
    assert arm != text[start:end]
    routes.write_text(text[:start] + arm + text[end:])
    print(f'Route mutant prepared at {copy}; run the foreground, load-gated ci-d1-budget with a separate target directory.')
    sys.exit(0)

chosen = sys.argv[1:] or list(cases)
for name in chosen:
    mode, pin, neuter = cases[name]
    mutated = neuter(base)
    assert mutated != base, f'{name}: mutation did not change behavior'
    file.write_text(mutated)
    command = ['cargo', 'test', '--manifest-path', str(copy / 'workers/Cargo.toml'), '-p', 'low-app-layer', '--target-dir', str(Path(os.environ['CARGO_TARGET_DIR']) / 'mutants'), mode]
    if mode == '--test': command.append('l23_r2_sqlite')
    command.extend([pin, '--', '--nocapture'])
    result = subprocess.run(command, env=os.environ, capture_output=True, text=True, timeout=570)
    output = result.stdout + result.stderr
    (logs / f'{name}.log').write_text(output)
    failure = next((line for line in output.splitlines() if line.startswith('test ') and ' ... FAILED' in line), '')
    assert result.returncode == 101 and failure and 'error[E' not in output, f'{name}: not an assertion RED; see {logs / (name + ".log")}'
    print(f'{name}: exit 101; {failure}', flush=True)
file.write_text(base)
