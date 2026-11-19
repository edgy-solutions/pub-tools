# Which pub-tools build the cluster runs

Observed 2026-11-16 on the shared test cluster (kube context `<context>`, namespace `<namespace>`).

Names of the cluster, its namespace, the release and the image registry are placeholders on this page.
The real names are the deploy's, and invincible-agent holds them.

| | |
|---|---|
| Image | `<registry>/edgy-solutions/pub-tools@sha256:ed93f7b14b6a3d4a094c098d0fb6233deb077b1271440fb851e8952d717bc9bc` |
| Source | `1aa2cbe` (the `master` build of that commit, reviewed 2026-09-23) |
| Pods | `<release>-pub-tools` (code location) and `<release>-pub-tools-broker`, both running that digest |
| Declared in | invincible-agent `helm/invincible-agent/values.yaml`, `global.imageDigests` (pin set in `7754c0a7`) |

## Why it is not `master`

`master` is `e5612fa`, one commit past `1aa2cbe`. That commit carries two changes: the CP1252 byte
repair and the V_CHARACTERISTICS overflow fold. Its build (`1555b341`) was not reviewed. The
architect ruled on 2026-11-05 to keep `ed93f7b1` and to bring both changes forward on a reviewed build.

That decision belongs to invincible-agent, which owns the deploy. It is recorded there and not repeated
here:

- invincible-agent `44a89b21`, `helm/invincible-agent/CHANGELOG.md`, the 0.4.80 entry, which moves
  two images on the shared test cluster
- invincible-agent board item `01M6P5ZAJ382FBHEQDA23941EA`

## Keeping this true

This page states an observation, so it can go stale. Check it again with the shared test cluster's
kube context selected:

```bash
kubectl -n <namespace> get pods \
  -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.containerStatuses[*].imageID}{"\n"}{end}' \
  | grep pub-tools
```

When the cluster moves to a new build, update the table and the date in the same PR as the pin
change in invincible-agent, or delete this page.
