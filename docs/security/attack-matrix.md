# FleetShift attack matrix

Rows show the smallest useful compromised-actor set. A larger set includes the capabilities of smaller sets.

| Compromised actors | Attacker goal | Result | Main defense or limitation |
| --- | --- | --- | --- |
| Malicious tenant user | Cross-tenant read/write | Tenant escape | RM authorization, target tenant binding; exact grant and storage isolation remain open. |
| Network attacker | Replay or alter delivery | Wrong target state or unauthorized action | TLS, signatures, evidence log, generation checks. |
| RM + database | Forge or alter user delivery | On verifier path, rejected; on credential-only path, possible | Provenance and target-side verification; credential-only is weaker by design. |
| RM + database | Stop all work | Denial of service | No current independent recovery path; high residual risk. |
| RM + malicious signer | Fabricate accepted tenant intent | Valid-looking unauthorized delivery | Trust policy must not accept compromised signer; authority compromise is a root-level threat. |
| Addon + RM | Bypass addon or tenant policy | Malicious manifests or dynamic API behavior | Independent addon evidence and target checks; RM still controls orchestration. |
| UI plugin + user browser | Steal or redirect approval | User signs attacker-selected action | Isolated signing UI and clear display; implementation and asset integrity open. |
| Fleetlet + target credential | Mutate local target | Target compromise | Target-local scope and channel profile; target compromise is not fully recoverable. |
| Target + fleetlet | Pivot to RM or another target | Lateral movement | Outbound-only/buffered transport, scoped identities, network separation. |
| Tenant + resource flood | Exhaust shared platform or target | Cross-tenant outage | Backpressure is not a quota; exact budgets are unresolved. |
| Provider RM + factory credential | Bypass consumer intent | Cross-tenant factory mutation | Signed consumer intent, restricted channels, vault dual authorization; proposals not complete. |
| Operator + trust administrator | Replace accepted trust | Broad identity takeover | Separation of duties, authenticated updates, offline/recovery authority. |

## Attack-goal graphs

### Unauthorized target mutation

```text
Acquire access
  OR steal credential
  OR compromise RM
  OR compromise addon/agent
        -> reach mutation path
        -> bypass or satisfy target policy
        -> apply wrong content
```

The provenance-verifying path blocks the RM branch at target verification. Credential-only, direct proxy, break-glass, and compromised-target branches need separate controls.

### Cross-tenant disclosure

```text
Read another tenant's data
  OR exploit RM authorization
  OR exploit addon/query filtering
  OR exploit index/alias correlation
  OR compromise RM/database
```

The last branch is an explicit non-goal. The first three remain in scope and are high-risk until tested.
