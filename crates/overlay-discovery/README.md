# overlay-discovery

Topic managers and lookup services for the BSV overlay network.
Provides five protocol plugins that implement the `TopicManager` and
`LookupService` traits from [`overlay-engine`][engine]:

- **SHIP** — Service Host Identity Protocol
- **SLAP** — Service Lookup Availability Protocol
- **UHRP** — Universal Hash Resolution Protocol (content-addressed
  files)
- **Agent Registry** — identity key → endpoint + capabilities
- **DmDelegation** — cross-agent delegation-certificate revocation
  anchors

Part of the [`bsv-overlay-cloudflare`][root] workspace.

[engine]: ../overlay-engine
[root]: ../..

## What each plugin does

| Plugin | Topic manager | Lookup service | What the PushDrop encodes |
|---|---|---|---|
| `ship/` | `SHIPTopicManager` | `SHIPLookupService` | `SHIP`, identity key, domain, topic name |
| `slap/` | `SLAPTopicManager` | `SLAPLookupService` | `SLAP`, identity key, domain, lookup-service name |
| `uhrp/` | `UHRPTopicManager` | `UHRPLookupService` | Advertiser key, hash, URL, expiry, content-length, signature |
| `agent/` | `AgentTopicManager` | `AgentLookupService` | Identity key, certifier, endpoint, capabilities |
| `dm_delegation/` | `DmDelegationTopicManager` | `DmDelegationLookupService` | Serial number, certifier, subject, expiry |

Each topic manager:
- Validates PushDrop field count + protocol tag (`field[0]`).
- Validates URI shape, BRC-87 name format, identity-key→locking-key
  signature link (via `isTokenSignatureCorrectlyLinked`).
- Returns `outputsToAdmit` + `coinsToRetain` from a BEEF.

Each lookup service:
- Indexes admitted outputs via an `on_admit` callback into a
  per-plugin storage trait (`SHIPStorage`, `SLAPStorage`, …).
- Responds to `LookupQuestion` queries with an `output-list` or
  `formula` answer shape.

## Storage traits

Each plugin defines its own narrow storage trait (e.g. `SHIPStorage`
in `ship/storage.rs`). The trait is the ONLY dependency the plugin has
on persistence — it's implemented once for `MemoryStorage` (in-crate,
for tests) and once for D1 (in the `overlay-cloudflare` crate).

Keeping storage narrow per-plugin means a deployment can pick and
choose — register only SHIP + SLAP (mainline parity), or add any
subset of UHRP/Agent/DmDelegation as its product requires.

## Also in this crate

- **`WalletAdvertiser`** (`advertiser.rs`) — implements `overlay_engine::Advertiser`
  by PushDropping SHIP/SLAP records via a BRC-42 wallet. Creates,
  finds, and revokes advertisements.
- **`validation.rs`** — shared BRC-87 name validation and URI
  (https://, HTTPS-advertisable) validation.

## Quick taste

```rust
use overlay_discovery::ship::{
    topic_manager::SHIPTopicManager,
    lookup_service::SHIPLookupService,
    storage::SHIPStorage,
};
use std::rc::Rc;

let ship_storage: Rc<dyn SHIPStorage> = Rc::new(my_ship_storage_impl);

let topic_manager = Box::new(SHIPTopicManager::new());
let lookup_service = Box::new(SHIPLookupService::new(ship_storage.clone()));

// Hand to Engine::with_all(...)
```

## Testing

```bash
# Unit + integration + cross-validator (checks we admit the same
# outputs mainline admits, and reject the same invalid ones)
cargo test -p overlay-discovery

# With memory storage for end-to-end plugin lifecycle tests
cargo test -p overlay-discovery --features overlay-engine/memory-storage
```

The `tests/ts_sdk_parity.rs` and `tests/cross_validator.rs` suites
import real PushDrop fixtures and assert byte-for-byte that we make
the same admission decisions as `@bsv/sdk` 1.10+ and
`@bsv/overlay-discovery-services` 2.0.2.

## License

Licensed under either of [Apache-2.0](../../LICENSE-APACHE) or
[MIT](../../LICENSE-MIT) at your option.
