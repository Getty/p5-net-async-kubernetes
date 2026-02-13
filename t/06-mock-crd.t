use strict;
use warnings;
use Test::More;

use lib 't/lib';
use FindBin;

use IO::Async::Loop;
use Net::Async::Kubernetes;
use MockTransport;

# Load the CRD test class from kubernetes-rest (sibling directory)
my $kr_tlib = "$FindBin::Bin/../../kubernetes-rest/t/lib";
if (-d $kr_tlib) {
    unshift @INC, $kr_tlib;
}

my $has_crd_class;
eval {
    require My::StaticWebSite;
    $has_crd_class = 1;
};

plan skip_all => 'My::StaticWebSite not available (need kubernetes-rest t/lib)'
    unless $has_crd_class;

my $loop = IO::Async::Loop->new;

sub make_crd_kube {
    MockTransport::reset();

    require IO::K8s;
    my $kube = Net::Async::Kubernetes->new(
        server      => { endpoint => 'https://mock.local' },
        credentials => { token => 'mock-token' },
        resource_map => {
            %{ IO::K8s->default_resource_map },
            StaticWebSite => '+My::StaticWebSite',
        },
        resource_map_from_cluster => 0,
    );
    MockTransport::install($kube);
    $loop->add($kube);
    return $kube;
}

# ============================================================================
# CRD class expansion
# ============================================================================

subtest 'expand_class for CRD' => sub {
    my $kube = make_crd_kube();
    my $class = $kube->expand_class('StaticWebSite');
    like($class, qr/My::StaticWebSite/, 'CRD short name expands correctly');
};

# ============================================================================
# CRD list
# ============================================================================

subtest 'list CRD resources' => sub {
    my $kube = make_crd_kube();

    MockTransport::mock_response('GET',
        '/apis/homelab.example.com/v1/namespaces/default/staticwebsites', {
        kind => 'StaticWebSiteList',
        apiVersion => 'homelab.example.com/v1',
        items => [
            { kind => 'StaticWebSite', apiVersion => 'homelab.example.com/v1',
              metadata => { name => 'my-blog', namespace => 'default' },
              spec => { domain => 'blog.example.com' }, status => {} },
            { kind => 'StaticWebSite', apiVersion => 'homelab.example.com/v1',
              metadata => { name => 'docs', namespace => 'default' },
              spec => { domain => 'docs.example.com' }, status => {} },
        ],
    });

    my $list = $kube->list('StaticWebSite', namespace => 'default')->get;
    isa_ok($list, 'IO::K8s::List');
    is(scalar @{$list->items}, 2, 'got 2 CRD items');
    is($list->items->[0]->metadata->name, 'my-blog', 'first CRD name');
    is($list->items->[1]->metadata->name, 'docs', 'second CRD name');

    my $req = MockTransport::last_request();
    like($req->{path},
        qr{/apis/homelab\.example\.com/v1/namespaces/default/staticwebsites},
        'correct CRD API path');
};

# ============================================================================
# CRD get
# ============================================================================

subtest 'get CRD resource' => sub {
    my $kube = make_crd_kube();

    MockTransport::mock_response('GET',
        '/apis/homelab.example.com/v1/namespaces/default/staticwebsites/my-blog', {
        kind => 'StaticWebSite', apiVersion => 'homelab.example.com/v1',
        metadata => { name => 'my-blog', namespace => 'default',
                       resourceVersion => '12345' },
        spec => { domain => 'blog.example.com' }, status => {},
    });

    my $site = $kube->get('StaticWebSite', 'my-blog', namespace => 'default')->get;
    is($site->metadata->name, 'my-blog', 'CRD get by name');
    is($site->metadata->namespace, 'default', 'CRD namespace');
};

# ============================================================================
# CRD create
# ============================================================================

subtest 'create CRD resource' => sub {
    my $kube = make_crd_kube();

    MockTransport::mock_response('POST',
        '/apis/homelab.example.com/v1/namespaces/default/staticwebsites', {
        kind => 'StaticWebSite', apiVersion => 'homelab.example.com/v1',
        metadata => { name => 'new-site', namespace => 'default',
                       uid => 'new-uid', resourceVersion => '1' },
        spec => { domain => 'new.example.com' }, status => {},
    });

    my $site = $kube->_rest->new_object('StaticWebSite',
        metadata => { name => 'new-site', namespace => 'default' },
        spec     => { domain => 'new.example.com' },
    );
    my $created = $kube->create($site)->get;
    is($created->metadata->name, 'new-site', 'CRD created');

    my $req = MockTransport::last_request();
    is($req->{method}, 'POST', 'used POST for create');
    ok($req->{content}, 'request had body');
};

# ============================================================================
# CRD delete
# ============================================================================

subtest 'delete CRD resource' => sub {
    my $kube = make_crd_kube();

    MockTransport::mock_response('DELETE',
        '/apis/homelab.example.com/v1/namespaces/default/staticwebsites/my-blog', {
        kind => 'Status', status => 'Success',
    });

    my $ok = $kube->delete('StaticWebSite', 'my-blog', namespace => 'default')->get;
    ok($ok, 'CRD delete succeeded');

    my $req = MockTransport::last_request();
    is($req->{method}, 'DELETE', 'used DELETE');
    like($req->{path},
        qr{/apis/homelab\.example\.com/v1/namespaces/default/staticwebsites/my-blog},
        'correct CRD delete path');
};

# ============================================================================
# CRD patch
# ============================================================================

subtest 'patch CRD resource' => sub {
    my $kube = make_crd_kube();

    MockTransport::mock_response('PATCH',
        '/apis/homelab.example.com/v1/namespaces/default/staticwebsites/my-blog', {
        kind => 'StaticWebSite', apiVersion => 'homelab.example.com/v1',
        metadata => { name => 'my-blog', namespace => 'default' },
        spec => { domain => 'updated.example.com' }, status => {},
    });

    my $patched = $kube->patch('StaticWebSite', 'my-blog',
        namespace => 'default',
        patch     => { spec => { domain => 'updated.example.com' } },
    )->get;
    is($patched->metadata->name, 'my-blog', 'CRD patched');

    my $req = MockTransport::last_request();
    is($req->{method}, 'PATCH', 'used PATCH');
};

# ============================================================================
# CRD watcher
# ============================================================================

subtest 'watch CRD resources' => sub {
    my $kube = make_crd_kube();

    MockTransport::mock_watch_events(
        '/apis/homelab.example.com/v1/namespaces/default/staticwebsites', [
        { type => 'ADDED', object => {
            kind => 'StaticWebSite', apiVersion => 'homelab.example.com/v1',
            metadata => { name => 'site-1', namespace => 'default',
                           resourceVersion => '100' },
            spec => { domain => 'one.example.com' }, status => {},
        }},
        { type => 'MODIFIED', object => {
            kind => 'StaticWebSite', apiVersion => 'homelab.example.com/v1',
            metadata => { name => 'site-1', namespace => 'default',
                           resourceVersion => '101' },
            spec => { domain => 'one-updated.example.com' }, status => {},
        }},
        { type => 'DELETED', object => {
            kind => 'StaticWebSite', apiVersion => 'homelab.example.com/v1',
            metadata => { name => 'site-1', namespace => 'default',
                           resourceVersion => '102' },
            spec => {}, status => {},
        }},
    ]);

    my @added;
    my @modified;
    my @deleted;
    my @all_events;

    my $watcher;
    $watcher = $kube->watcher('StaticWebSite',
        namespace  => 'default',
        timeout    => 5,
        on_added   => sub { push @added, $_[0] },
        on_modified => sub { push @modified, $_[0] },
        on_deleted => sub { push @deleted, $_[0] },
        on_event   => sub {
            push @all_events, $_[0];
            $watcher->stop;
            $loop->stop if @all_events >= 3;
        },
    );

    $loop->watch_time(after => 3, code => sub { $watcher->stop; $loop->stop; });
    $loop->run;

    is(scalar @added, 1, 'got 1 ADDED event');
    is($added[0]->metadata->name, 'site-1', 'ADDED CRD object name');

    is(scalar @modified, 1, 'got 1 MODIFIED event');
    is(scalar @deleted, 1, 'got 1 DELETED event');
    is(scalar @all_events, 3, 'got 3 total events via on_event');
};

done_testing;
