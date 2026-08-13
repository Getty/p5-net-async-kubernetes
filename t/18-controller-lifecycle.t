use strict;
use warnings;
use Test::More;
use Scalar::Util qw(weaken);

use lib 't/lib';

use Future;
use IO::Async::Loop;
use Net::Async::Kubernetes;
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
    return $kube;
}

sub pod_added {
    my ($name, $resource_version) = @_;
    return { type => 'ADDED', object => {
        kind => 'Pod', apiVersion => 'v1',
        metadata => {
            name => $name, namespace => 'default',
            resourceVersion => $resource_version,
        },
        spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
        status => { phase => 'Pending' },
    }};
}

subtest 'entries do not accumulate over cleanly reconciled keys' => sub {
    my $kube = make_kube();
    $loop->add($kube);

    my $key_count = 50;
    MockTransport::mock_watch_events('/api/v1/namespaces/default/pods',
        [ map { pod_added("pod-$_", 100 + $_) } 1 .. $key_count ]);

    my @reconciled;
    my $controller = $kube->controller(
        on_reconcile => sub {
            my ($ctx) = @_;
            push @reconciled, $ctx->{key};
            $loop->later(sub { $loop->stop }) if @reconciled == $key_count;
            return;
        },
    );
    $controller->watch_resource('Pod', namespace => 'default');

    my $guard = $loop->watch_time(after => 5, code => sub { $loop->stop });
    $loop->run;
    $loop->unwatch_time($guard);
    $controller->stop;

    is(scalar @reconciled, $key_count, "all $key_count keys reconciled");

    # {entries} is the workqueue index, one hashref per key. A key that
    # reconciled cleanly is neither queued, dirty nor retrying, so nothing
    # about it is still needed - keeping it grows the hash for the lifetime
    # of the process on a high churn resource.
    is(scalar keys %{ $controller->{entries} }, 0,
        'no entry left behind once every key reconciled cleanly');

    $loop->remove($kube);
};

subtest 'a failed reconcile keeps its entry and its retry state' => sub {
    my $kube = make_kube();
    $loop->add($kube);

    MockTransport::mock_watch_events('/api/v1/namespaces/default/pods',
        [ pod_added('pod-keep', 200) ]);

    my @attempts;
    my $controller;
    $controller = $kube->controller(
        on_reconcile => sub {
            my ($ctx) = @_;
            push @attempts, $ctx->{attempt};
            # stopping first keeps _schedule_retry from arming a timer, so the
            # entry is left holding nothing but its own failure state
            $ctx->{controller}->stop;
            $loop->later(sub { $loop->stop });
            return Future->fail('boom');
        },
    );
    $controller->watch_resource('Pod', namespace => 'default');

    my $guard = $loop->watch_time(after => 5, code => sub { $loop->stop });
    $loop->run;
    $loop->unwatch_time($guard);

    is_deeply(\@attempts, [1], 'reconcile ran once and failed');

    my $entry = $controller->{entries}{'default/pod-keep'};
    ok($entry, 'entry for the failed key survives');
    is($entry->{failures}, 1, 'failure count kept, so the next attempt is 2');
    ok($entry->{ctx}, 'ctx kept with the entry for the retry');

    $loop->remove($kube);
};

done_testing;
