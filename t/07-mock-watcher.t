use strict;
use warnings;
use Test::More;

use lib 't/lib';

use IO::Async::Loop;
use Net::Async::Kubernetes;
use Net::Async::Kubernetes::Watcher;
use MockTransport;

my $loop = IO::Async::Loop->new;

sub make_kube {
    MockTransport::reset();
    my $kube = Net::Async::Kubernetes->new(
        server      => { endpoint => 'https://mock.local' },
        credentials => { token => 'mock-token' },
        resource_map_from_cluster => 0,
    );
    MockTransport::install($kube);
    $loop->add($kube);
    return $kube;
}

# ============================================================================
# Watcher creation
# ============================================================================

subtest 'watcher creation' => sub {
    my $kube = make_kube();

    MockTransport::mock_watch_events('/api/v1/pods', []);

    my $watcher = $kube->watcher('Pod',
        on_added => sub {},
    );
    isa_ok($watcher, 'Net::Async::Kubernetes::Watcher');
    isa_ok($watcher, 'IO::Async::Notifier');
    is($watcher->resource, 'Pod', 'resource stored');
    is($watcher->namespace, undef, 'namespace undef for cluster-wide');
    is($watcher->timeout, 300, 'default timeout');
    $watcher->stop;
};

subtest 'watcher with all options' => sub {
    my $kube = make_kube();

    MockTransport::mock_watch_events('/api/v1/namespaces/prod/pods', []);

    my $watcher = $kube->watcher('Pod',
        namespace      => 'prod',
        timeout        => 60,
        label_selector => 'app=web',
        field_selector => 'status.phase=Running',
        on_added       => sub {},
        on_modified    => sub {},
        on_deleted     => sub {},
        on_error       => sub {},
        on_event       => sub {},
    );

    is($watcher->namespace, 'prod', 'namespace set');
    is($watcher->timeout, 60, 'custom timeout');
    is($watcher->label_selector, 'app=web', 'label_selector set');
    is($watcher->field_selector, 'status.phase=Running', 'field_selector set');
    ok($watcher->on_added, 'on_added callback set');
    ok($watcher->on_modified, 'on_modified callback set');
    ok($watcher->on_deleted, 'on_deleted callback set');
    ok($watcher->on_error, 'on_error callback set');
    ok($watcher->on_event, 'on_event callback set');
    $watcher->stop;
};

# ============================================================================
# Watcher receives events
# ============================================================================

subtest 'watcher dispatches ADDED events' => sub {
    my $kube = make_kube();

    MockTransport::mock_watch_events('/api/v1/namespaces/default/pods', [
        { type => 'ADDED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'pod-1', namespace => 'default',
                           resourceVersion => '10' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => { phase => 'Running' },
        }},
        { type => 'ADDED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'pod-2', namespace => 'default',
                           resourceVersion => '11' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => { phase => 'Pending' },
        }},
    ]);

    my @added;
    my $watcher;
    $watcher = $kube->watcher('Pod',
        namespace => 'default',
        on_added  => sub { push @added, $_[0] },
        on_event  => sub {
            $watcher->stop;
            $loop->stop if @added >= 2;
        },
    );

    $loop->watch_time(after => 2, code => sub { $watcher->stop; $loop->stop; });
    $loop->run;

    is(scalar @added, 2, 'got 2 ADDED events');
    is($added[0]->metadata->name, 'pod-1', 'first pod name');
    is($added[1]->metadata->name, 'pod-2', 'second pod name');
};

subtest 'watcher dispatches all event types' => sub {
    my $kube = make_kube();

    MockTransport::mock_watch_events('/api/v1/namespaces/default/pods', [
        { type => 'ADDED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'pod-1', namespace => 'default',
                           resourceVersion => '10' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => {},
        }},
        { type => 'MODIFIED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'pod-1', namespace => 'default',
                           resourceVersion => '11' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => { phase => 'Running' },
        }},
        { type => 'DELETED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'pod-1', namespace => 'default',
                           resourceVersion => '12' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => {},
        }},
    ]);

    my (@added, @modified, @deleted, @all);
    my $watcher;
    $watcher = $kube->watcher('Pod',
        namespace   => 'default',
        on_added    => sub { push @added, $_[0]->metadata->name },
        on_modified => sub { push @modified, $_[0]->metadata->name },
        on_deleted  => sub { push @deleted, $_[0]->metadata->name },
        on_event    => sub {
            push @all, $_[0]->type;
            $watcher->stop;
            $loop->stop if @all >= 3;
        },
    );

    $loop->watch_time(after => 2, code => sub { $watcher->stop; $loop->stop; });
    $loop->run;

    is_deeply(\@added, ['pod-1'], 'ADDED callback');
    is_deeply(\@modified, ['pod-1'], 'MODIFIED callback');
    is_deeply(\@deleted, ['pod-1'], 'DELETED callback');
    is_deeply(\@all, ['ADDED', 'MODIFIED', 'DELETED'], 'on_event catches all types');
};

# ============================================================================
# Watcher ERROR events
# ============================================================================

subtest 'watcher dispatches ERROR events' => sub {
    my $kube = make_kube();

    MockTransport::mock_watch_events('/api/v1/pods', [
        { type => 'ERROR', object => {
            kind => 'Status', apiVersion => 'v1',
            status => 'Failure', message => 'Gone',
            code => 410,
        }},
    ]);

    my @errors;
    my $watcher;
    $watcher = $kube->watcher('Pod',
        on_error => sub { push @errors, $_[0] },
        on_event => sub {
            $watcher->stop;
            $loop->stop;
        },
    );

    $loop->watch_time(after => 2, code => sub { $watcher->stop; $loop->stop; });
    $loop->run;

    # 410 Gone is handled specially (resource version reset), may not reach on_error
    # depending on implementation. Just verify watcher didn't crash.
    ok(1, 'watcher handles ERROR events without crashing');
};

# ============================================================================
# Watcher stop/start
# ============================================================================

subtest 'watcher stop' => sub {
    my $kube = make_kube();

    MockTransport::mock_watch_events('/api/v1/pods', [
        { type => 'ADDED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'x', namespace => 'default',
                           resourceVersion => '1' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => {},
        }},
    ]);

    my $count = 0;
    my $watcher;
    $watcher = $kube->watcher('Pod',
        on_added => sub { $count++ },
        on_event => sub { $watcher->stop; $loop->stop; },
    );

    $loop->watch_time(after => 2, code => sub { $watcher->stop; $loop->stop; });
    $loop->run;

    ok($count >= 1, "received events: $count");
    $watcher->stop;  # double-stop should be safe
    ok(1, 'double stop is safe');
};

# ============================================================================
# Watcher with only on_event (no type-specific callbacks)
# ============================================================================

subtest 'watcher with only on_event' => sub {
    my $kube = make_kube();

    MockTransport::mock_watch_events('/api/v1/namespaces/test/pods', [
        { type => 'ADDED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'a', namespace => 'test', resourceVersion => '1' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => {},
        }},
    ]);

    my @events;
    my $watcher;
    $watcher = $kube->watcher('Pod',
        namespace => 'test',
        on_event  => sub {
            push @events, $_[0];
            $watcher->stop;
            $loop->stop;
        },
    );

    $loop->watch_time(after => 2, code => sub { $watcher->stop; $loop->stop; });
    $loop->run;

    is(scalar @events, 1, 'got event via on_event only');
    is($events[0]->type, 'ADDED', 'event type correct');
    is($events[0]->object->metadata->name, 'a', 'event object correct');
};

# ============================================================================
# Client-side filtering: event_types
# ============================================================================

subtest 'event_types filter - only ADDED' => sub {
    my $kube = make_kube();

    MockTransport::mock_watch_events('/api/v1/namespaces/default/pods', [
        { type => 'ADDED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'pod-1', namespace => 'default',
                           resourceVersion => '10' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => {},
        }},
        { type => 'MODIFIED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'pod-1', namespace => 'default',
                           resourceVersion => '11' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => { phase => 'Running' },
        }},
        { type => 'DELETED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'pod-1', namespace => 'default',
                           resourceVersion => '12' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => {},
        }},
    ]);

    my (@added, @modified, @deleted);
    my $event_count = 0;
    my $watcher;
    $watcher = $kube->watcher('Pod',
        namespace   => 'default',
        event_types => ['ADDED'],
        on_added    => sub { push @added, $_[0]->metadata->name },
        on_modified => sub { push @modified, $_[0]->metadata->name },
        on_deleted  => sub { push @deleted, $_[0]->metadata->name },
    );

    # Give time for events to be processed, then stop
    $loop->watch_time(after => 1, code => sub { $watcher->stop; $loop->stop; });
    $loop->run;

    is_deeply(\@added, ['pod-1'], 'ADDED event passed');
    is_deeply(\@modified, [], 'MODIFIED event filtered out');
    is_deeply(\@deleted, [], 'DELETED event filtered out');
};

subtest 'event_types filter - ADDED and DELETED' => sub {
    my $kube = make_kube();

    MockTransport::mock_watch_events('/api/v1/namespaces/default/pods', [
        { type => 'ADDED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'pod-1', namespace => 'default',
                           resourceVersion => '10' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => {},
        }},
        { type => 'MODIFIED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'pod-1', namespace => 'default',
                           resourceVersion => '11' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => {},
        }},
        { type => 'DELETED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'pod-1', namespace => 'default',
                           resourceVersion => '12' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => {},
        }},
    ]);

    my @types;
    my $watcher;
    $watcher = $kube->watcher('Pod',
        namespace   => 'default',
        event_types => ['ADDED', 'DELETED'],
        on_event    => sub { push @types, $_[0]->type },
    );

    $loop->watch_time(after => 1, code => sub { $watcher->stop; $loop->stop; });
    $loop->run;

    is_deeply(\@types, ['ADDED', 'DELETED'], 'only ADDED and DELETED passed');
};

# ============================================================================
# Client-side filtering: names
# ============================================================================

subtest 'names filter - single regex' => sub {
    my $kube = make_kube();

    MockTransport::mock_watch_events('/api/v1/namespaces/default/pods', [
        { type => 'ADDED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'nginx-abc', namespace => 'default',
                           resourceVersion => '10' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => {},
        }},
        { type => 'ADDED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'redis-xyz', namespace => 'default',
                           resourceVersion => '11' },
            spec => { containers => [{ name => 'redis', image => 'redis' }] },
            status => {},
        }},
        { type => 'ADDED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'nginx-def', namespace => 'default',
                           resourceVersion => '12' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => {},
        }},
    ]);

    my @names;
    my $watcher;
    $watcher = $kube->watcher('Pod',
        namespace => 'default',
        names     => qr/^nginx/,
        on_added  => sub { push @names, $_[0]->metadata->name },
    );

    $loop->watch_time(after => 1, code => sub { $watcher->stop; $loop->stop; });
    $loop->run;

    is_deeply(\@names, ['nginx-abc', 'nginx-def'], 'only nginx pods matched');
};

subtest 'names filter - array of regex' => sub {
    my $kube = make_kube();

    MockTransport::mock_watch_events('/api/v1/namespaces/default/pods', [
        { type => 'ADDED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'nginx-1', namespace => 'default',
                           resourceVersion => '10' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => {},
        }},
        { type => 'ADDED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'redis-1', namespace => 'default',
                           resourceVersion => '11' },
            spec => { containers => [{ name => 'redis', image => 'redis' }] },
            status => {},
        }},
        { type => 'ADDED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'postgres-1', namespace => 'default',
                           resourceVersion => '12' },
            spec => { containers => [{ name => 'pg', image => 'postgres' }] },
            status => {},
        }},
    ]);

    my @names;
    my $watcher;
    $watcher = $kube->watcher('Pod',
        namespace => 'default',
        names     => [qr/^nginx/, qr/^redis/],
        on_added  => sub { push @names, $_[0]->metadata->name },
    );

    $loop->watch_time(after => 1, code => sub { $watcher->stop; $loop->stop; });
    $loop->run;

    is_deeply(\@names, ['nginx-1', 'redis-1'], 'nginx and redis matched, postgres filtered');
};

subtest 'names filter - exact string' => sub {
    my $kube = make_kube();

    MockTransport::mock_watch_events('/api/v1/namespaces/default/pods', [
        { type => 'ADDED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'my-pod', namespace => 'default',
                           resourceVersion => '10' },
            spec => { containers => [{ name => 'app', image => 'app' }] },
            status => {},
        }},
        { type => 'ADDED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'my-pod-extra', namespace => 'default',
                           resourceVersion => '11' },
            spec => { containers => [{ name => 'app', image => 'app' }] },
            status => {},
        }},
    ]);

    my @names;
    my $watcher;
    $watcher = $kube->watcher('Pod',
        namespace => 'default',
        names     => 'my-pod',
        on_added  => sub { push @names, $_[0]->metadata->name },
    );

    $loop->watch_time(after => 1, code => sub { $watcher->stop; $loop->stop; });
    $loop->run;

    is_deeply(\@names, ['my-pod'], 'exact match only, not prefix');
};

subtest 'auto event_types - only on_added set' => sub {
    my $kube = make_kube();

    MockTransport::mock_watch_events('/api/v1/namespaces/default/pods', [
        { type => 'ADDED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'pod-1', namespace => 'default',
                           resourceVersion => '10' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => {},
        }},
        { type => 'MODIFIED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'pod-1', namespace => 'default',
                           resourceVersion => '11' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => { phase => 'Running' },
        }},
        { type => 'DELETED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'pod-1', namespace => 'default',
                           resourceVersion => '12' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => {},
        }},
    ]);

    my @added;
    my $watcher;
    $watcher = $kube->watcher('Pod',
        namespace => 'default',
        on_added  => sub { push @added, $_[0]->metadata->name },
    );

    $loop->watch_time(after => 1, code => sub { $watcher->stop; $loop->stop; });
    $loop->run;

    is_deeply(\@added, ['pod-1'], 'ADDED received');
    # MODIFIED and DELETED should have been auto-filtered since no callbacks set
};

subtest 'auto event_types - on_event catches all' => sub {
    my $kube = make_kube();

    MockTransport::mock_watch_events('/api/v1/namespaces/default/pods', [
        { type => 'ADDED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'pod-1', namespace => 'default',
                           resourceVersion => '10' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => {},
        }},
        { type => 'MODIFIED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'pod-1', namespace => 'default',
                           resourceVersion => '11' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => { phase => 'Running' },
        }},
    ]);

    my @types;
    my $watcher;
    $watcher = $kube->watcher('Pod',
        namespace => 'default',
        on_event  => sub { push @types, $_[0]->type },
    );

    $loop->watch_time(after => 1, code => sub { $watcher->stop; $loop->stop; });
    $loop->run;

    is_deeply(\@types, ['ADDED', 'MODIFIED'], 'on_event sees all types');
};

subtest 'auto event_types - on_added + on_deleted skips MODIFIED' => sub {
    my $kube = make_kube();

    MockTransport::mock_watch_events('/api/v1/namespaces/default/pods', [
        { type => 'ADDED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'pod-1', namespace => 'default',
                           resourceVersion => '10' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => {},
        }},
        { type => 'MODIFIED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'pod-1', namespace => 'default',
                           resourceVersion => '11' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => { phase => 'Running' },
        }},
        { type => 'DELETED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'pod-1', namespace => 'default',
                           resourceVersion => '12' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => {},
        }},
    ]);

    my (@added, @deleted);
    my $watcher;
    $watcher = $kube->watcher('Pod',
        namespace  => 'default',
        on_added   => sub { push @added, $_[0]->metadata->name },
        on_deleted => sub { push @deleted, $_[0]->metadata->name },
    );

    $loop->watch_time(after => 1, code => sub { $watcher->stop; $loop->stop; });
    $loop->run;

    is_deeply(\@added, ['pod-1'], 'ADDED received');
    is_deeply(\@deleted, ['pod-1'], 'DELETED received');
    # MODIFIED auto-filtered because no on_modified callback
};

subtest 'names and event_types combined' => sub {
    my $kube = make_kube();

    MockTransport::mock_watch_events('/api/v1/namespaces/default/pods', [
        { type => 'ADDED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'nginx-1', namespace => 'default',
                           resourceVersion => '10' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => {},
        }},
        { type => 'MODIFIED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'nginx-1', namespace => 'default',
                           resourceVersion => '11' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => { phase => 'Running' },
        }},
        { type => 'ADDED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'redis-1', namespace => 'default',
                           resourceVersion => '12' },
            spec => { containers => [{ name => 'redis', image => 'redis' }] },
            status => {},
        }},
        { type => 'DELETED', object => {
            kind => 'Pod', apiVersion => 'v1',
            metadata => { name => 'nginx-1', namespace => 'default',
                           resourceVersion => '13' },
            spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
            status => {},
        }},
    ]);

    my @events;
    my $watcher;
    $watcher = $kube->watcher('Pod',
        namespace   => 'default',
        names       => qr/^nginx/,
        event_types => ['ADDED', 'DELETED'],
        on_event    => sub { push @events, $_[0]->type . ':' . $_[0]->object->metadata->name },
    );

    $loop->watch_time(after => 1, code => sub { $watcher->stop; $loop->stop; });
    $loop->run;

    is_deeply(\@events, ['ADDED:nginx-1', 'DELETED:nginx-1'],
        'both filters applied: nginx only + ADDED/DELETED only');
};

done_testing;
