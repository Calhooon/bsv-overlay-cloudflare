from pathlib import Path
text = Path('CLAUDE.md').read_text()
# NOTE-B (the shed's register sentence) retired with the shed (bsv-low #893 section 6.2): there is no shed to state.
assert 'The owed recompute walk is RETIRED (bsv-low #893 section 6.2' in text, 'the retirement is stated'
print('PASS the owed walk and its shed are stated retired')
assert 'deleteAll() preserves the alarm at compatibility date `2024-12-01`' in text, 'NOTE-C: state deleteAll alarm semantics with pinned compatibility date and evidence'
assert 'https://developers.cloudflare.com/changelog/post/2026-02-24-deleteall-deletes-alarms/' in text, 'NOTE-C: cite the compatibility change'
print('PASS NOTE-C compatibility-date alarm fact and evidence stated')
