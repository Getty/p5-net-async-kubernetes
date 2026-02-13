# CLAUDE.md

## Distribution

Net::Async::Kubernetes - Async Kubernetes client for Perl built on IO::Async.

Uses Dist::Zilla with `[@Author::GETTY]` plugin bundle.

## Structure

- `lib/Net/Async/Kubernetes.pm` - Main client (IO::Async::Notifier)
- `lib/Net/Async/Kubernetes/Watcher.pm` - Auto-reconnecting watch stream
- `eg/demo.pl` - Comprehensive idempotent demo (requires minikube)
- `eg/watch_pods.pl` - Simple watcher example

## Dependencies

- **IO::Async** / **Net::Async::HTTP** - Async framework and HTTP transport
- **Kubernetes::REST** - Sync REST client (used for request building/response parsing)
- **IO::K8s** - Kubernetes API object classes

## Testing

```bash
prove -l t/                    # Mock tests (no cluster needed)
TEST_KUBERNETES_REST_KUBECONFIG=~/.kube/config prove -lv t/   # Live tests
```

Tests use a dual-mode architecture (`t/lib/MockTransport.pm`, `t/lib/TestKube.pm`) — same tests run against mock or live cluster.

## PodWeaver

Uses `@Author::GETTY` conventions: inline `=attr`, `=method`, `=seealso`. No manual NAME/VERSION/AUTHOR sections.
