# Notes

## Inventory

- how to handle local resource IDs like kube object UIDs? should relationships (edges) be between _these_ vs names? or either, depending?
- need to think about schema a bit more – we probably want to at least support "obvious" top level fields like basic kube metadata (namespace/name)
- we need a "mark & sweep" protocol for detecting stale objects
- need relationships (see ID note)
- history / alias processing
- 

## Old

- "context engineering" built in
- policy as input to placement
- is there more we need to look at / be ready for in terms of reconciliation / updates over time? think use cases. i think this is addon driven, primarily?