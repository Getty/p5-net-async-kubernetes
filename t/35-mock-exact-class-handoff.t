use strict;
use warnings;
use Test::More;

use lib 't/lib';

use IO::Async::Loop;
use IO::K8s;
use JSON::MaybeXS;
use Kubernetes::REST;
use Net::Async::Kubernetes;
use MockTransport;

# karr k50 (Kubernetes::REST k42): ensure() and ensure_only() resolve a
# hashref manifest's class themselves and must hand that class to IO::K8s's
# struct_to_object exactly, with a '+'. expand_class returns a plain class
# name - a resource_map value of '+Gizmo' comes back as 'Gizmo' - and
# struct_to_object resolves a name again, to which a single-segment 'Gizmo'
# is a Kind: IO::K8s::Gizmo, which does not exist, so ensure croaked; or,
# when the short key Gizmo maps to another class, that class, and the
# manifest went to that class's endpoint.
#
# Inflating the server's answer is Kubernetes::REST's inflate_object, which
# resolved the name again the same way until 1.109 (its own k42). What this
# client sends does not depend on that; the class of what comes back does,
# and is only checked from 1.109 on.
#
# Mock-only: everything here is request routing, nothing needs a cluster.

my $loop = IO::Async::Loop->new;
my $JSON = JSON::MaybeXS->new(utf8 => 1);

my $GIZMOS = '/apis/k42.example.com/v1/namespaces/ns/gizmos';
my $GVK    = 'k42.example.com/v1/Gizmo';
my $INFLATES_EXACTLY = eval { Kubernetes::REST->VERSION('1.109'); 1 };

# The Gizmo entry is keyed by its GVK only, as a provider registers a Kind
# whose short name is taken. %extra adds further map entries.
sub make_kube {
    my (%extra) = @_;
    MockTransport::reset();
    my $kube = Net::Async::Kubernetes->new(
        server      => { endpoint => 'https://mock.local' },
        credentials => { token => 'mock-token' },
        resource_map_from_cluster => 0,
        resource_map => {
            %{ IO::K8s->default_resource_map },
            $GVK => '+Gizmo',
            %extra,
        },
    );
    MockTransport::install($kube);
    $loop->add($kube);
    return $kube;
}

sub calls {
    return [ map { "$_->{method} $_->{path}" } MockTransport::request_log ];
}

sub gizmo {
    my ($name, %extra) = @_;
    return {
        apiVersion => 'k42.example.com/v1',
        kind       => 'Gizmo',
        metadata   => { name => $name, namespace => 'ns' },
        spec       => { size => 'large' },
        %extra,
    };
}

# The manifest is absent, so ensure POSTs it: the request goes out whether or
# not the answer then inflates.
sub mock_absent_gizmo {
    MockTransport::mock_response('GET', "$GIZMOS/g1",
        { kind => 'Status', status => 'Failure', message => 'not found', code => 404 }, 404);
    MockTransport::mock_response('POST', $GIZMOS,
        gizmo('g1', metadata => { name => 'g1', namespace => 'ns', resourceVersion => '1' }));
}

sub posted_body {
    my ($post) = grep { $_->{method} eq 'POST' } MockTransport::request_log;
    return $post ? $JSON->decode($post->{content}) : undef;
}

sub check_result {
    my ($f) = @_;
    SKIP: {
        skip 'the answer inflates exactly from Kubernetes::REST 1.109 on', 1 unless $INFLATES_EXACTLY;
        isa_ok($f && eval { $f->get }, 'Gizmo', 'the result');
    }
}

subtest 'ensure: a manifest resolved through a +single-segment GVK entry is a Gizmo' => sub {
    my $kube = make_kube();
    is($kube->expand_class('Gizmo', 'k42.example.com/v1'), 'Gizmo', 'premise: expand_class drops the +');
    mock_absent_gizmo();

    my $f = eval { $kube->ensure(gizmo('g1')) };
    is($@, '', 'ensure does not croak');
    is_deeply(calls(), [ "GET $GIZMOS/g1", "POST $GIZMOS" ], 'the manifest went to the Gizmo endpoint');
    my $body = posted_body();
    is($body && $body->{apiVersion}, 'k42.example.com/v1', 'the body is the Gizmo apiVersion');
    is($body && $body->{spec}{size}, 'large', 'the body carries the spec');
    check_result($f);
};

subtest 'ensure: the short key Gizmo mapped to another class does not hijack the manifest' => sub {
    # A resolved 'Gizmo' re-read as a Kind would land on the Istio Gateway -
    # silently, with its endpoint.
    my $kube = make_kube(Gizmo => '+My::Istio::Gateway');
    mock_absent_gizmo();

    my $f = eval { $kube->ensure(gizmo('g1')) };
    is($@, '', 'ensure does not croak');
    is_deeply(calls(), [ "GET $GIZMOS/g1", "POST $GIZMOS" ], 'the manifest went to the Gizmo endpoint');
    is_deeply([ grep { /istio/ } @{ calls() } ], [], 'nothing was sent to the other group');
    check_result($f);
};

subtest 'ensure: a manifest without apiVersion keeps the class its Kind resolved to' => sub {
    # The same hand-off without an apiVersion: the Kind's short key names
    # Gizmo under another name, and Gizmo re-read as a Kind is the Istio
    # Gateway.
    my $kube = make_kube(Gadget => '+Gizmo', Gizmo => '+My::Istio::Gateway');
    mock_absent_gizmo();

    my %manifest = %{ gizmo('g1') };
    delete $manifest{apiVersion};
    $manifest{kind} = 'Gadget';
    my $f = eval { $kube->ensure(\%manifest) };
    is($@, '', 'ensure does not croak');
    is_deeply(calls(), [ "GET $GIZMOS/g1", "POST $GIZMOS" ], 'the manifest went to the Gizmo endpoint');
    check_result($f);
};

subtest 'ensure_only: the applied manifest is a Gizmo, and it is kept' => sub {
    my $kube = make_kube(Gizmo => '+My::Istio::Gateway');
    mock_absent_gizmo();
    MockTransport::mock_response('GET', "$GIZMOS?labelSelector=app=demo", {
        apiVersion => 'k42.example.com/v1', kind => 'GizmoList',
        items      => [ gizmo('g1') ],
    });

    my $f = eval {
        $kube->ensure_only(
            label      => 'app=demo',
            objects    => [ gizmo('g1') ],
            kinds      => [ $GVK ],
            namespaces => ['ns'],
        );
    };
    is($@, '', 'ensure_only does not croak');
    is_deeply([ grep { /^(?:GET|POST) / && !/labelSelector/ } @{ calls() } ],
        [ "GET $GIZMOS/g1", "POST $GIZMOS" ], 'the manifest went to the Gizmo endpoint');
    SKIP: {
        # Below 1.109, inflate_list re-reads 'Gizmo' as the Kind too: the
        # listed g1 becomes the Istio Gateway, matches nothing applied, and is
        # deleted - in networking.istio.io.
        skip 'listed items inflate exactly from Kubernetes::REST 1.109 on', 1 unless $INFLATES_EXACTLY;
        is_deeply([ grep { /^DELETE/ } @{ calls() } ], [], 'nothing was deleted');
    }
};

done_testing;
