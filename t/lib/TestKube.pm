package TestKube;
# Dual-mode test helper for Net::Async::Kubernetes.
# Mock mode (default): uses MockTransport for offline testing.
# Live mode (TEST_KUBERNETES_REST_KUBECONFIG set): connects to real cluster.

use strict;
use warnings;
use Exporter 'import';
our @EXPORT_OK = qw(is_live make_kube loop);

use IO::Async::Loop;
use Net::Async::Kubernetes;

my $loop;
sub loop { $loop //= IO::Async::Loop->new }

sub is_live {
    return $ENV{TEST_KUBERNETES_REST_KUBECONFIG} ? 1 : 0;
}

sub make_kube {
    if (is_live()) {
        require Kubernetes::REST::Kubeconfig;
        my $kc = Kubernetes::REST::Kubeconfig->new(
            kubeconfig_path => $ENV{TEST_KUBERNETES_REST_KUBECONFIG},
            ($ENV{TEST_KUBERNETES_REST_CONTEXT}
                ? (context_name => $ENV{TEST_KUBERNETES_REST_CONTEXT}) : ()),
        );
        my $api = $kc->api;
        my $kube = Net::Async::Kubernetes->new(
            server      => $api->server,
            credentials => $api->credentials,
            resource_map_from_cluster => 0,
        );
        loop()->add($kube);
        return $kube;
    } else {
        require MockTransport;
        MockTransport::reset();
        my $kube = Net::Async::Kubernetes->new(
            server      => { endpoint => 'https://mock.local' },
            credentials => { token => 'mock-token' },
            resource_map_from_cluster => 0,
        );
        MockTransport::install($kube);
        loop()->add($kube);
        return $kube;
    }
}

1;
