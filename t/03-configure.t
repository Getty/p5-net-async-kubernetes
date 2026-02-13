use strict;
use warnings;
use Test::More;

use IO::Async::Loop;
use Net::Async::Kubernetes;
use Kubernetes::REST::Server;
use Kubernetes::REST::AuthToken;

my $loop = IO::Async::Loop->new;

# ============================================================================
# Constructor with server + credentials (hashref coercion)
# ============================================================================

subtest 'new with server/credentials hashrefs' => sub {
    my $kube = Net::Async::Kubernetes->new(
        server      => { endpoint => 'https://k8s.local:6443' },
        credentials => { token => 'my-token' },
    );
    isa_ok($kube, 'Net::Async::Kubernetes');
    isa_ok($kube, 'IO::Async::Notifier');
    isa_ok($kube->server, 'Kubernetes::REST::Server');
    is($kube->server->endpoint, 'https://k8s.local:6443', 'server endpoint');
    is($kube->credentials->token, 'my-token', 'credentials token');
};

# ============================================================================
# Constructor with pre-built objects
# ============================================================================

subtest 'new with server/credentials objects' => sub {
    my $server = Kubernetes::REST::Server->new(endpoint => 'https://obj.local:6443');
    my $creds  = Kubernetes::REST::AuthToken->new(token => 'obj-token');

    my $kube = Net::Async::Kubernetes->new(
        server      => $server,
        credentials => $creds,
    );
    is($kube->server, $server, 'server object passed through');
    is($kube->credentials, $creds, 'credentials object passed through');
};

# ============================================================================
# Accessors and defaults
# ============================================================================

subtest 'accessors and defaults' => sub {
    my $kube = Net::Async::Kubernetes->new(
        server      => { endpoint => 'https://test.local' },
        credentials => { token => 'x' },
    );

    is($kube->kubeconfig, undef, 'kubeconfig defaults to undef');
    is($kube->context, undef, 'context defaults to undef');
    is($kube->resource_map, undef, 'resource_map defaults to undef');
    is($kube->resource_map_from_cluster, 0, 'resource_map_from_cluster defaults to 0');
};

# ============================================================================
# resource_map passthrough
# ============================================================================

subtest 'resource_map passed to constructor' => sub {
    my %map = (Foo => '+My::Foo');
    my $kube = Net::Async::Kubernetes->new(
        server       => { endpoint => 'https://test.local' },
        credentials  => { token => 'x' },
        resource_map => \%map,
    );
    is_deeply($kube->resource_map, \%map, 'resource_map stored');
};

# ============================================================================
# Missing server/credentials
# ============================================================================

subtest 'croak without server or kubeconfig' => sub {
    my $kube = Net::Async::Kubernetes->new;
    eval { $kube->server };
    like($@, qr/server or kubeconfig required/, 'server croaks without config');

    eval { $kube->credentials };
    like($@, qr/credentials or kubeconfig required/, 'credentials croaks without config');
};

# ============================================================================
# kubeconfig loading (if available)
# ============================================================================

SKIP: {
    skip 'No kubeconfig available', 3
        unless -f ($ENV{KUBECONFIG} // "$ENV{HOME}/.kube/config");

    subtest 'new with kubeconfig' => sub {
        my $kube = Net::Async::Kubernetes->new(
            kubeconfig => ($ENV{KUBECONFIG} // "$ENV{HOME}/.kube/config"),
        );
        ok($kube->server, 'server resolved from kubeconfig');
        isa_ok($kube->server, 'Kubernetes::REST::Server');
        ok($kube->credentials, 'credentials resolved from kubeconfig');
        like($kube->server->endpoint, qr{^https?://}, 'endpoint is a URL');
    };
}

# ============================================================================
# _rest lazy builder
# ============================================================================

subtest '_rest lazy init' => sub {
    my $kube = Net::Async::Kubernetes->new(
        server      => { endpoint => 'https://test.local' },
        credentials => { token => 'test' },
    );
    my $rest = $kube->_rest;
    isa_ok($rest, 'Kubernetes::REST');
    is($rest->server->endpoint, 'https://test.local', '_rest has correct server');
};

# ============================================================================
# expand_class delegation
# ============================================================================

subtest 'expand_class delegates to _rest' => sub {
    my $kube = Net::Async::Kubernetes->new(
        server      => { endpoint => 'https://test.local' },
        credentials => { token => 'test' },
        resource_map_from_cluster => 0,
    );
    my $class = $kube->expand_class('Pod');
    like($class, qr/Pod/, 'expand_class resolves Pod');
};

# ============================================================================
# Custom resource_map in expand_class
# ============================================================================

subtest 'expand_class with custom resource_map' => sub {
    # Need to load the CRD class first
    eval {
        require IO::K8s;
        my $default_map = IO::K8s->default_resource_map;
        my $kube = Net::Async::Kubernetes->new(
            server      => { endpoint => 'https://test.local' },
            credentials => { token => 'test' },
            resource_map => {
                %$default_map,
                MyCustomThing => '+My::Custom::Thing',
            },
            resource_map_from_cluster => 0,
        );
        my $class = $kube->expand_class('MyCustomThing');
        is($class, 'My::Custom::Thing', 'custom resource_map entry resolved');
    };
    skip "IO::K8s default_resource_map not available", 1 if $@;
};

done_testing;
