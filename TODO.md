# TODO

## Design

- Incorporate "root" service account (locked up keys, rarely used) best practice? ([FM-43](https://redhat.atlassian.net/browse/FM-43))
- Look at workload identity federation kinds of techniques for authenticating the platform or authenticating agents ([FM-20](https://redhat.atlassian.net/browse/FM-20), [FM-37](https://redhat.atlassian.net/browse/FM-37), [FM-47](https://redhat.atlassian.net/browse/FM-47))
- Should RBAC "push" (bootstrapping) be done by deployments in which the pool is everything in that workspace? And then new clusters are new targets which trigger the pipeline again? This is pretty elegant and follows existing authorization model. ([FM-41](https://redhat.atlassian.net/browse/FM-41))
- I think an explicit tenant notion is needed in the managed cluster case (can possibly be sharded but instances are still managed by provider) ([FM-39](https://redhat.atlassian.net/browse/FM-39))
- What happens if agent is offline during delivery? Does it stall the whole stage? ([FM-25](https://redhat.atlassian.net/browse/FM-25))
- How do we securely store tokens used for deployment while being able to place to new targets if placement is updated? I wanted to encrypt per target but that would require reauth on new placement (maybe that is one option). Do we require refresh tokens for this, or store the initial JWT encrypted with a platform key? ([FM-48](https://redhat.atlassian.net/browse/FM-48))
- How should IdP resolution work for new clusters? In a single tenant situation with a single IdP this is easy. But what about when this is a multitenant provider? ([FM-49](https://redhat.atlassian.net/browse/FM-49))
- Look at upgrades (rolling out updates to deployments themselves) ([FM-26](https://redhat.atlassian.net/browse/FM-26))

## Functionality

- Fleetlet installation into kind cluster ([FM-25](https://redhat.atlassian.net/browse/FM-25))
- Pagination / filter / etc
- Revisit failed deployment when no targets – what if it is invalidated? (manifest update) Do we have/need a signal for when new targets might match because they've been newly registered?
- What if multiple targets match manifest types? Initial target pool needs to be a bit more specific and therefore flexible. Maybe we want an "InitialPool" kind of abstraction in addition to the placement strategy. ([FM-34](https://redhat.atlassian.net/browse/FM-34))
  - f(deployment) -> targets (initial)
  - f(targets) -> targets (strategy)
- Make sure workflows never leave a deployment permanently "stuck" without being reconciled – should always at least have a failure state but should really just keep retrying forever? ([FM-72](https://redhat.atlassian.net/browse/FM-72))

## HCP Agent

- Async delivery resilience: deliveries in Accepted/Progressing state are not
   recovered after a FleetShift process restart. The watching goroutine is lost
   and the delivery stays in Accepted permanently. This affects all async
   delivery agents (kind, hcp) but is most impactful for HCP where the async
   phase takes 10-20 minutes. Options: persistent delivery state machine,
   reconciliation loop on startup that resumes in-flight deliveries, or
   workflow-engine-managed async steps instead of agent goroutines.
- IAM policies are overly permissive for POC: several roles use `ec2:*`,
   `s3:*`, `route53:*` with `Resource: "*"` (cloud-controller, node-pool,
   control-plane-operator, cloud-network-config-controller in `aws_iam.go`).
   Tighten to least-privilege before any shared-account or production use.
- Hardcoded S3 OIDC thumbprint in `aws_iam.go` (`A9D53002E97E00E...`). AWS has
   rotated this cert before. Either fetch dynamically or document as a
   maintenance item.
- No env var support for HCP CLI flags. Design spec lists `HCP_KUBECONFIG`,
   `HCP_AWS_REGION`, `HCP_S3_BUCKET`, `HCP_PULL_SECRET` but `serve.go` only
   uses cobra flags without env binding.

## Implementation detail (nontrivial)

- Not sure about the credential design (should we assume one raw token? what about other token types?)
- The whole key binding doc should probably not travel on the deployment state
- Not sure about how canonical deployment representation is calculated for signing (e.g. CLI coupling to strategy types)

## Code / Trivial

- Rethink where generated proto types go
- Revisit workflow contract tests because I think these are just testing the same workflow logic tested elsewhere (so maybe just combine & use to test workflow implementations & workflow logic)
- In process delivery agents need to also use durable workflows (in their own addon package)
- The feetctl token output should probably be treated more like standard output rather than custom
- Not sure about how provenance is built and assigned to inputs
