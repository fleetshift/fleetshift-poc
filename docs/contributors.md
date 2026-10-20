# Contributors

This experiment builds on the work of the contributors below, including the imported UI history and experiments that may not be carried into the new repository. Thank you for the implementation, design, testing, documentation, and review that brought it this far.

## Historical snapshot

Snapshot taken October 14, 2026, from the complete predecessor history reachable from commit `7d39d9fa05b47081ea06b17e415b43ece68120a6`. Author dates span February 23–October 14, 2026 in UTC. The history contains **574 commits: 524 non-merge commits and 50 merge commits**, with five named human authors and one shared service-account identity.

| Contributor | Non-merge commits authored | Merge commits authored | Additional co-authored commits |
| --- | ---: | ---: | ---: |
| Alec Henninger | 182 | 9 | 2 |
| Joshua Packer | 1 | 0 | 0 |
| Martin Marosi | 202 | 39 | 1 |
| Matthew Short | 102 | 1 | 3 |
| Sherine Khoury | 7 | 1 | 0 |

The service-account names `platex-rehor-bot` and `Řehoř` account for another 30 non-merge commits. Git also records tool co-author credits on 155 commits for Claude and 51 for Cursor; these are separate from the human credits above and are not additional commits.

These counts describe the recorded history, not contribution value or effort. Squashed changes, reviews, discussions, and other work are not fully represented. The predecessor repository will retain the detailed history when archived.

## Counting method

Count every commit reachable from the recorded revision once, including imported ancestry and work subsequently removed. Exclude unrelated branches and uncommitted work. Separate merges from non-merges. Combine Alec Henninger's and Matthew Short's email identities, Martin Marosi/Maroši, and Sherine Khoury/`sherine-k`; group the two service-account names together. Combine Claude's model-name variants for tool credits.

For co-authorship, use Git's parsed final `Co-authored-by` trailers, counting each normalized identity once per commit and excluding its own authored commits. Credits can overlap across people and tools, so that column is not additive to the commit total. Embedded trailers in squashed commit-message bodies are not counted separately.

To inspect the source counts in a complete predecessor checkout:

```sh
history_commit=7d39d9fa05b47081ea06b17e415b43ece68120a6
git rev-list --count "$history_commit"
git rev-list --count --merges "$history_commit"
git shortlog -sne --no-merges "$history_commit"
git shortlog -sne --merges "$history_commit"
git log --format='%H %aI' "$history_commit"
git log --format='%H%n%(trailers:key=Co-authored-by,valueonly=true)' "$history_commit"
```
