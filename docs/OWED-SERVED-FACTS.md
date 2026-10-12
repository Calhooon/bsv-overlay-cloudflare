# Owed list: served facts

For a rebuilt markerless row, `markerlessEvidence` means that the identity's signed party filing selected an evicted JOIN. A candidate fault may name possible released outpoints without establishing their ownership or value; it is `unbound`, has no claim, and explains which evidence could not be judged.

`markerlessIgnoredMarkers` counts both planted/unverified markers and honest `Unknown` attestations whose verification latch has not been reached. Rebuilding from signed party evidence supersedes either; only a `Verified` marker holds the outpoint against a rebuild (L23R2D-N4).

For a rebuilt markerless row, `markerCreatedAtMs` is the eviction's `evictedAt` in milliseconds, used for ordering; it is not the creation time of a hopparty marker. A released hop is stranded by the eviction release, so this value does not introduce a young-hop window (L23R2D-N5).

`markerlessProgress` reports the signed identity's eviction count, the number examined this pass, the durable ring cursor, and its completion word. The production candidate statement uses `COUNT(*) OVER ()`, limited by the same eight-JOIN constant as the plan. Its window count and ring execute against production migrations in real SQLite (19 JOINs across three pages), and the captured first-read route executes it on local D1/Miniflare in `make ci-d1-budget` (exit 0). Hosted Cloudflare D1 execution remains unverified; this lane performs no live D1 request (L23R2D-N2).
