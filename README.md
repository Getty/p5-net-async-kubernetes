# Net::Async::Kubernetes

Async Kubernetes client for Perl built on [IO::Async](https://metacpan.org/pod/IO::Async).

All API calls return [Future](https://metacpan.org/pod/Future) objects for non-blocking operation. Includes a [Watcher](https://metacpan.org/pod/Net::Async::Kubernetes::Watcher) with auto-reconnect for streaming resource events.

## Features

- **Future-based CRUD**: `list()`, `get()`, `create()`, `update()`, `patch()`, `delete()`
- **Streaming watch** with auto-reconnect and resumable `resourceVersion` tracking
- **Event callbacks**: `on_added`, `on_modified`, `on_deleted`, `on_error`, `on_event`
- **Client-side filtering**: `names` (regex/string/array) and `event_types` for declarative event filtering
- **Smart event dispatch**: only processes event types that have registered callbacks
- **Automatic 410 Gone recovery**: clears resourceVersion and restarts
- **Auto-reconnect** on stream completion and connection errors
- **Three patch types**: strategic-merge (default), merge, json
- **Custom Resource Definition (CRD) support** via `resource_map`
- **Kubeconfig support** (`~/.kube/config`) with context selection
- **SSL/TLS** with client certificates
- Built on [Kubernetes::REST](https://metacpan.org/pod/Kubernetes::REST) and [IO::K8s](https://metacpan.org/pod/IO::K8s)

## Installation

    cpanm Net::Async::Kubernetes

## Synopsis

```perl
use IO::Async::Loop;
use Net::Async::Kubernetes;

my $loop = IO::Async::Loop->new;

# From kubeconfig (easiest)
my $kube = Net::Async::Kubernetes->new(
    kubeconfig => "$ENV{HOME}/.kube/config",
);
$loop->add($kube);

# Or with explicit server/credentials
my $kube = Net::Async::Kubernetes->new(
    server      => { endpoint => 'https://kubernetes.local:6443' },
    credentials => { token => $token },
);
$loop->add($kube);

# List pods
my $pods = $kube->list('Pod', namespace => 'default')->get;

# Get a specific pod
my $pod = $kube->get('Pod', 'nginx', namespace => 'default')->get;

# Create a resource
my $cm = $kube->_rest->new_object(ConfigMap =>
    metadata => { name => 'my-config', namespace => 'default' },
    data     => { key => 'value' },
);
my $created = $kube->create($cm)->get;

# Patch a resource
my $patched = $kube->patch('Deployment', 'web',
    namespace => 'default',
    patch     => { spec => { replicas => 3 } },
    type      => 'merge',
)->get;

# Delete a resource
$kube->delete('Pod', 'nginx', namespace => 'default')->get;

# Watch for changes with auto-reconnect
my $watcher = $kube->watcher('Pod',
    namespace      => 'default',
    label_selector => 'app=web',
    on_added    => sub { my ($pod) = @_; say "Added: " . $pod->metadata->name },
    on_modified => sub { my ($pod) = @_; say "Modified: " . $pod->metadata->name },
    on_deleted  => sub { my ($pod) = @_; say "Deleted: " . $pod->metadata->name },
);

$loop->run;

# Filter events client-side
my $watcher = $kube->watcher('Pod',
    namespace   => 'default',
    names       => [qr/^nginx/, qr/^redis/],  # only matching names
    event_types => ['ADDED', 'DELETED'],        # skip MODIFIED
    on_added    => sub { my ($pod) = @_; say "New: " . $pod->metadata->name },
    on_deleted  => sub { my ($pod) = @_; say "Gone: " . $pod->metadata->name },
);

# Or just set the callbacks you care about - event types auto-derive
my $watcher = $kube->watcher('Pod',
    namespace => 'default',
    on_added  => sub { ... },  # only ADDED events dispatched
);
```

## Testing

```bash
# Run mock tests (no cluster needed)
prove -l t/

# Run against a real cluster (e.g. minikube)
TEST_KUBERNETES_REST_KUBECONFIG=~/.kube/config prove -lv t/
```

## Documentation

Full documentation is available on [MetaCPAN](https://metacpan.org/pod/Net::Async::Kubernetes).

## See Also

- [Kubernetes::REST](https://metacpan.org/pod/Kubernetes::REST) - Synchronous Kubernetes REST client
- [IO::K8s](https://metacpan.org/pod/IO::K8s) - Kubernetes API objects for Perl
- [IO::Async](https://metacpan.org/pod/IO::Async) - Async framework
- [Net::Async::HTTP](https://metacpan.org/pod/Net::Async::HTTP) - HTTP client used for transport

## Author

Torsten Raudssus <torsten@raudssus.de>

## License

This is free software licensed under the Apache License 2.0.
