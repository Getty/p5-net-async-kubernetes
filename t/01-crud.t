use strict;
use warnings;
use Test::More;
use Test::Exception;

use IO::Async::Loop;
use Net::Async::Kubernetes;

# We test against a real cluster if TEST_KUBERNETES_REST_KUBECONFIG is set,
# otherwise we skip (no mock HTTP server for async yet)
unless ($ENV{TEST_KUBERNETES_REST_KUBECONFIG}) {
    plan skip_all => 'Set TEST_KUBERNETES_REST_KUBECONFIG for async CRUD tests';
}

my $loop = IO::Async::Loop->new;

# Build from kubeconfig
require Kubernetes::REST::Kubeconfig;
my $kc = Kubernetes::REST::Kubeconfig->new(
    kubeconfig_path => $ENV{TEST_KUBERNETES_REST_KUBECONFIG},
    ($ENV{TEST_KUBERNETES_REST_CONTEXT}
        ? (context_name => $ENV{TEST_KUBERNETES_REST_CONTEXT}) : ()),
);

my $rest_api = $kc->api;

my $kube = Net::Async::Kubernetes->new(
    server      => $rest_api->server,
    credentials => $rest_api->credentials,
    resource_map_from_cluster => 0,
);
$loop->add($kube);

# Test: list namespaces
subtest 'list namespaces' => sub {
    my $namespaces = $kube->list('Namespace')->get;
    isa_ok($namespaces, 'IO::K8s::List');
    ok(scalar @{$namespaces->items} > 0, 'got at least one namespace');
    my $default = grep { $_->metadata->name eq 'default' } @{$namespaces->items};
    ok($default, 'default namespace found');
};

# Test: list pods in kube-system
subtest 'list pods' => sub {
    my $pods = $kube->list('Pod', namespace => 'kube-system')->get;
    isa_ok($pods, 'IO::K8s::List');
    ok(scalar @{$pods->items} > 0, 'got pods in kube-system');
    for my $pod (@{$pods->items}) {
        ok($pod->metadata->name, 'pod has name: ' . $pod->metadata->name);
        last;  # just check the first one
    }
};

# Test: get a specific namespace
subtest 'get namespace' => sub {
    my $ns = $kube->get('Namespace', 'default')->get;
    ok($ns, 'got default namespace');
    is($ns->metadata->name, 'default', 'name is default');
};

# Test: create + delete namespace
subtest 'create and delete namespace' => sub {
    my $test_ns = 'kubernetes-async-test-' . int(rand(10000));

    my $ns = $rest_api->new_object(Namespace =>
        metadata => { name => $test_ns },
    );

    my $created = $kube->create($ns)->get;
    ok($created, 'namespace created');
    is($created->metadata->name, $test_ns, 'name matches');

    my $deleted = $kube->delete('Namespace', $test_ns)->get;
    ok($deleted, 'namespace deleted');
};

done_testing;
