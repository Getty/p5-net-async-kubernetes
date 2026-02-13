use strict;
use warnings;
use Test::More;

use IO::Async::Loop;
use Net::Async::Kubernetes;

unless ($ENV{TEST_KUBERNETES_REST_KUBECONFIG}) {
    plan skip_all => 'Set TEST_KUBERNETES_REST_KUBECONFIG for watcher tests';
}

my $loop = IO::Async::Loop->new;

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

# Test: watcher receives ADDED events for existing pods
subtest 'watcher sees existing pods' => sub {
    my @events;
    my $watcher;

    $watcher = $kube->watcher('Pod',
        namespace => 'kube-system',
        timeout   => 5,
        on_added  => sub {
            my ($pod) = @_;
            push @events, { type => 'ADDED', name => $pod->metadata->name };
        },
        on_event => sub {
            my ($event) = @_;
            # After we've seen some events, stop
            if (scalar @events >= 3) {
                $watcher->stop;
                $loop->stop;
            }
        },
    );

    # Timeout safety net
    my $timer = $loop->watch_time(
        after => 10,
        code  => sub {
            $watcher->stop;
            $loop->stop;
        },
    );

    $loop->run;

    ok(scalar @events > 0, 'received ADDED events: ' . scalar @events);
    for my $ev (@events[0..min(2, $#events)]) {
        ok($ev->{name}, "pod name: $ev->{name}");
    }
};

# Test: watcher sees namespace events
subtest 'watcher sees namespaces' => sub {
    my @events;
    my $watcher;

    $watcher = $kube->watcher('Namespace',
        timeout  => 5,
        on_added => sub {
            my ($ns) = @_;
            push @events, $ns->metadata->name;
        },
        on_event => sub {
            if (scalar @events >= 3) {
                $watcher->stop;
                $loop->stop;
            }
        },
    );

    my $timer = $loop->watch_time(
        after => 10,
        code  => sub {
            $watcher->stop;
            $loop->stop;
        },
    );

    $loop->run;

    ok(scalar @events > 0, 'received namespace events: ' . scalar @events);
    my $has_default = grep { $_ eq 'default' } @events;
    ok($has_default, 'saw default namespace');
};

# Test: stop and restart
subtest 'stop and restart watcher' => sub {
    my $count = 0;
    my $watcher;

    $watcher = $kube->watcher('Namespace',
        timeout  => 3,
        on_added => sub { $count++ },
        on_event => sub {
            if ($count >= 2) {
                $watcher->stop;
                $loop->stop;
            }
        },
    );

    my $timer = $loop->watch_time(
        after => 8,
        code  => sub {
            $watcher->stop;
            $loop->stop;
        },
    );

    $loop->run;

    ok($count > 0, "received $count events before stop");

    # Restart
    my $count2 = 0;
    $watcher->stop;

    # Need fresh callbacks since we can't change them
    my $watcher2;
    $watcher2 = $kube->watcher('Namespace',
        timeout  => 3,
        on_added => sub { $count2++ },
        on_event => sub {
            if ($count2 >= 2) {
                $watcher2->stop;
                $loop->stop;
            }
        },
    );

    my $timer2 = $loop->watch_time(
        after => 8,
        code  => sub {
            $watcher2->stop;
            $loop->stop;
        },
    );

    $loop->run;

    ok($count2 > 0, "received $count2 events after restart");
};

sub min { $_[0] < $_[1] ? $_[0] : $_[1] }

done_testing;
