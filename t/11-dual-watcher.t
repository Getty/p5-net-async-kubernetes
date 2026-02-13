use strict;
use warnings;
use Test::More;

use lib 't/lib';

use TestKube qw(is_live make_kube loop);

unless (is_live()) {
    require MockTransport;
}

diag(is_live() ? "LIVE mode: testing against real cluster" : "MOCK mode: using MockTransport");

my $kube = make_kube();
my $NS   = 'perl-async-watcher-test';

# Setup: create a test namespace
if (is_live()) {
    # Cleanup from previous run
    eval { $kube->delete('Namespace', $NS)->get };
    for (1..60) {
        last unless eval { $kube->get('Namespace', $NS)->get };
        sleep 1;
    }
    # Create fresh namespace
    my $ns = $kube->_rest->new_object(Namespace =>
        metadata => { name => $NS },
    );
    $kube->create($ns)->get;
    # Wait for namespace to be active
    for (1..30) {
        my $got = eval { $kube->get('Namespace', $NS)->get };
        last if $got && ($got->status->phase // '') eq 'Active';
        sleep 1;
    }
}

# ============================================================================
# Watcher receives ADDED events
# ============================================================================

subtest 'watcher receives ADDED events' => sub {
    if (is_live()) {
        # Create a ConfigMap to trigger events
        my @events;
        my $watcher;
        $watcher = $kube->watcher('ConfigMap',
            namespace => $NS,
            on_added  => sub { push @events, { type => 'ADDED', name => $_[0]->metadata->name } },
            on_event  => sub {
                if (@events >= 1) {
                    $watcher->stop;
                    loop()->stop;
                }
            },
        );

        # Create a ConfigMap after a short delay
        loop()->watch_time(after => 1, code => sub {
            $kube->create($kube->_rest->new_object(ConfigMap =>
                metadata => { name => 'watcher-test-cm', namespace => $NS },
                data => { key => 'value' },
            ))->get;
        });

        # Timeout safety
        loop()->watch_time(after => 15, code => sub { $watcher->stop; loop()->stop; });
        loop()->run;

        ok(scalar @events >= 1, 'received at least 1 ADDED event');
        ok(grep({ $_->{name} eq 'watcher-test-cm' } @events), 'saw our ConfigMap');
    } else {
        MockTransport::mock_watch_events("/api/v1/namespaces/$NS/configmaps", [
            { type => 'ADDED', object => {
                kind => 'ConfigMap', apiVersion => 'v1',
                metadata => { name => 'watcher-test-cm', namespace => $NS,
                              resourceVersion => '10' },
                data => { key => 'value' },
            }},
            { type => 'ADDED', object => {
                kind => 'ConfigMap', apiVersion => 'v1',
                metadata => { name => 'watcher-test-cm-2', namespace => $NS,
                              resourceVersion => '11' },
                data => { key2 => 'value2' },
            }},
        ]);

        my @added;
        my $watcher;
        $watcher = $kube->watcher('ConfigMap',
            namespace => $NS,
            on_added  => sub { push @added, $_[0] },
            on_event  => sub {
                $watcher->stop;
                loop()->stop if @added >= 2;
            },
        );

        loop()->watch_time(after => 2, code => sub { $watcher->stop; loop()->stop; });
        loop()->run;

        is(scalar @added, 2, 'got 2 ADDED events');
        is($added[0]->metadata->name, 'watcher-test-cm', 'first ConfigMap name');
        is($added[1]->metadata->name, 'watcher-test-cm-2', 'second ConfigMap name');
    }
};

# ============================================================================
# Watcher receives multiple event types
# ============================================================================

subtest 'watcher dispatches all event types' => sub {
    if (is_live()) {
        my (@added, @modified, @deleted, @all);
        my $watcher;
        $watcher = $kube->watcher('ConfigMap',
            namespace   => $NS,
            on_added    => sub { push @added, $_[0]->metadata->name },
            on_modified => sub { push @modified, $_[0]->metadata->name },
            on_deleted  => sub {
                push @deleted, $_[0]->metadata->name;
                $watcher->stop;
                loop()->stop;
            },
            on_event => sub { push @all, $_[0]->type },
        );

        # Create, modify, delete a ConfigMap to trigger events
        loop()->watch_time(after => 1, code => sub {
            my $cm = $kube->create($kube->_rest->new_object(ConfigMap =>
                metadata => { name => 'lifecycle-cm', namespace => $NS },
                data => { phase => 'create' },
            ))->get;

            # Modify it
            my $fetched = $kube->get('ConfigMap', 'lifecycle-cm', namespace => $NS)->get;
            my $data = $fetched->data // {};
            $data->{phase} = 'modify';
            # Create a new object with updated data for the update call
            my $updated_cm = $kube->_rest->new_object(ConfigMap =>
                metadata => {
                    name => 'lifecycle-cm', namespace => $NS,
                    resourceVersion => $fetched->metadata->resourceVersion,
                },
                data => $data,
            );
            $kube->update($updated_cm)->get;

            # Delete it
            $kube->delete('ConfigMap', 'lifecycle-cm', namespace => $NS)->get;
        });

        loop()->watch_time(after => 15, code => sub { $watcher->stop; loop()->stop; });
        loop()->run;

        ok(scalar @added >= 1, 'got ADDED events');
        ok(scalar @all >= 2, 'got multiple event types');
    } else {
        MockTransport::mock_watch_events("/api/v1/namespaces/$NS/configmaps", [
            { type => 'ADDED', object => {
                kind => 'ConfigMap', apiVersion => 'v1',
                metadata => { name => 'lifecycle-cm', namespace => $NS,
                              resourceVersion => '20' },
                data => { phase => 'create' },
            }},
            { type => 'MODIFIED', object => {
                kind => 'ConfigMap', apiVersion => 'v1',
                metadata => { name => 'lifecycle-cm', namespace => $NS,
                              resourceVersion => '21' },
                data => { phase => 'modify' },
            }},
            { type => 'DELETED', object => {
                kind => 'ConfigMap', apiVersion => 'v1',
                metadata => { name => 'lifecycle-cm', namespace => $NS,
                              resourceVersion => '22' },
                data => {},
            }},
        ]);

        my (@added, @modified, @deleted, @all);
        my $watcher;
        $watcher = $kube->watcher('ConfigMap',
            namespace   => $NS,
            on_added    => sub { push @added, $_[0]->metadata->name },
            on_modified => sub { push @modified, $_[0]->metadata->name },
            on_deleted  => sub { push @deleted, $_[0]->metadata->name },
            on_event    => sub {
                push @all, $_[0]->type;
                $watcher->stop;
                loop()->stop if @all >= 3;
            },
        );

        loop()->watch_time(after => 2, code => sub { $watcher->stop; loop()->stop; });
        loop()->run;

        is_deeply(\@added, ['lifecycle-cm'], 'ADDED callback fired');
        is_deeply(\@modified, ['lifecycle-cm'], 'MODIFIED callback fired');
        is_deeply(\@deleted, ['lifecycle-cm'], 'DELETED callback fired');
        is_deeply(\@all, ['ADDED', 'MODIFIED', 'DELETED'], 'on_event catches all types');
    }
};

# ============================================================================
# Watcher stop/restart
# ============================================================================

subtest 'watcher stop and restart' => sub {
    if (is_live()) {
        my $count = 0;
        my $watcher;
        $watcher = $kube->watcher('ConfigMap',
            namespace => $NS,
            timeout   => 5,
            on_event  => sub { $count++; $watcher->stop; loop()->stop; },
        );

        # Create something to trigger an event
        loop()->watch_time(after => 1, code => sub {
            eval {
                $kube->create($kube->_rest->new_object(ConfigMap =>
                    metadata => { name => 'stop-test-cm', namespace => $NS },
                    data => { x => '1' },
                ))->get;
            };
        });

        loop()->watch_time(after => 10, code => sub { $watcher->stop; loop()->stop; });
        loop()->run;

        ok($count >= 1, "received events before stop: $count");

        # Stop should be safe to call multiple times
        $watcher->stop;
        ok(1, 'double stop is safe');
    } else {
        MockTransport::mock_watch_events("/api/v1/namespaces/$NS/configmaps", [
            { type => 'ADDED', object => {
                kind => 'ConfigMap', apiVersion => 'v1',
                metadata => { name => 'stop-test', namespace => $NS,
                              resourceVersion => '30' },
                data => {},
            }},
        ]);

        my $count = 0;
        my $watcher;
        $watcher = $kube->watcher('ConfigMap',
            namespace => $NS,
            on_added  => sub { $count++ },
            on_event  => sub { $watcher->stop; loop()->stop; },
        );

        loop()->watch_time(after => 2, code => sub { $watcher->stop; loop()->stop; });
        loop()->run;

        ok($count >= 1, "received events: $count");
        $watcher->stop;
        ok(1, 'double stop is safe');
    }
};

# ============================================================================
# Watcher with on_event only (no type-specific callbacks)
# ============================================================================

subtest 'watcher with on_event only' => sub {
    unless (is_live()) {
        MockTransport::mock_watch_events("/api/v1/namespaces/$NS/configmaps", [
            { type => 'ADDED', object => {
                kind => 'ConfigMap', apiVersion => 'v1',
                metadata => { name => 'event-only-cm', namespace => $NS,
                              resourceVersion => '40' },
                data => {},
            }},
        ]);
    }

    my @events;
    my $watcher;

    if (is_live()) {
        $watcher = $kube->watcher('ConfigMap',
            namespace => $NS,
            on_event  => sub {
                push @events, $_[0];
                $watcher->stop;
                loop()->stop;
            },
        );

        loop()->watch_time(after => 1, code => sub {
            eval {
                $kube->create($kube->_rest->new_object(ConfigMap =>
                    metadata => { name => 'event-only-cm', namespace => $NS },
                    data => { test => '1' },
                ))->get;
            };
        });
    } else {
        $watcher = $kube->watcher('ConfigMap',
            namespace => $NS,
            on_event  => sub {
                push @events, $_[0];
                $watcher->stop;
                loop()->stop;
            },
        );
    }

    loop()->watch_time(after => 10, code => sub { $watcher->stop; loop()->stop; });
    loop()->run;

    ok(scalar @events >= 1, 'got event via on_event only');
    is($events[0]->type, 'ADDED', 'event type is ADDED');
    ok($events[0]->object->metadata->name, 'event has object with name');
};

# ============================================================================
# Cleanup
# ============================================================================

if (is_live()) {
    diag "Cleaning up test namespace '$NS'...";
    eval { $kube->delete('ConfigMap', $_, namespace => $NS)->get }
        for qw(watcher-test-cm lifecycle-cm stop-test-cm event-only-cm);
    eval { $kube->delete('Namespace', $NS)->get };
}

done_testing;
