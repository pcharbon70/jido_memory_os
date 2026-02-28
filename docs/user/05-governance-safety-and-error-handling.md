# 05 - Governance, Safety, and Error Handling

## Access policy
`Jido.MemoryOS.AccessPolicy` enforces operation-level access with actor/group/role/tier context.

Default behavior:
- Same actor is allowed
- Same group is allowed by default
- Cross-agent access is denied unless policy allows it

Policy settings live in `config.governance.policy`.

## Approval tokens for gated actions
For sensitive operations (for example policy updates or destructive actions), use approval tokens through the manager.

```elixir
{:ok, issued} =
  Jido.MemoryOS.MemoryManager.issue_approval_token(Jido.MemoryOS.MemoryManager,
    actor_id: "admin-1",
    actions: [:policy_update, :forget],
    reason: "approved maintenance"
  )

{:ok, result} =
  Jido.MemoryOS.MemoryManager.update_policy(
    %{id: "agent-1"},
    %{rules: [%{effect: :allow, actors: ["admin-1"], actions: [:retrieve]}]},
    actor_id: "admin-1",
    approval_token: issued.token
  )
```

## Data safety
`Jido.MemoryOS.DataSafety` applies:
- Retention rules (ttl limits, blocked tags, allowed classes)
- Redaction/masking of PII (`email`, `phone`, `ssn` patterns)
- Role-based masking modes for retrieval (`:allow | :mask | :drop`)

Governance config entry points:
- `governance.retention`
- `governance.masking`
- `safety.redaction_enabled`
- `safety.pii_strategy`

## Error model
`Jido.MemoryOS.ErrorMapping` converts failures to rich `Jido.Error.*` structs:
- `Jido.Error.ValidationError`
- `Jido.Error.ExecutionError`
- `Jido.Error.TimeoutError`
- `Jido.Error.InternalError`

This keeps failures typed and machine-actionable for workflows, retries, and observability.
