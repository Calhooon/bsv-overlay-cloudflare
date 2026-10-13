from pathlib import Path
text = Path('CLAUDE.md').read_text()
sentence = 'a shed is never silent: one reserved walk per identity per window per isolate (the deferred alarm or a snapshotless first read, one durable claim), the bound ceiling + 1; a faulted reserved walk re-arms ONCE for the next window; the read answer marks a shed snapshot (`refresh: "shed"`, `retryAfterMs` >= 1,000); a filing walks the filer only, the pot\'s other parties are marked stale.'
assert sentence in text, 'NOTE-B: register must use the delta lens sentence exactly; result filings refresh nobody'
print('PASS NOTE-B exact delta register sentence; no result refresh promise')
