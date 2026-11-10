#!/usr/bin/env perl
use strict;
use warnings;
use Test::More;

use lib 't/lib';

use IO::Async::Loop;
use Net::Async::Kubernetes;
use Net::Async::Kubernetes::Watcher;
use MockTransport;

# k84: a watch response that succeeds (200) can still carry Warning headers -
# a deprecated API above all. Kubernetes::REST's watch hands them to
# on_warning after every stream; the watcher has to as well, not only for a
# rejected request (status >= 400), where check_response runs anyway.
# Mock mode only.

my $loop = IO::Async::Loop->new;
my $PATH = '/api/v1/namespaces/default/pods';

my $added = { type => 'ADDED', object => {
    kind => 'Pod', apiVersion => 'v1',
    metadata => { name => 'pod-1', namespace => 'default', resourceVersion => '10' },
    spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
    status => {},
}};

sub make_kube {
    my ($on_warning) = @_;
    MockTransport::reset();
    my $kube = Net::Async::Kubernetes->new(
        server      => { endpoint => 'https://mock.local' },
        credentials => { token => 'mock-token' },
        resource_map_from_cluster => 0,
        on_warning  => $on_warning,
    );
    MockTransport::install($kube);
    $loop->add($kube);
    return $kube;
}

sub watch_requests { grep { $_->{streaming} } MockTransport::request_log() }

sub await_requests {
    my ($count) = @_;
    my $deadline = time + 5;
    $loop->loop_once(0.01) while @{[ watch_requests() ]} < $count && time < $deadline;
}

sub settle { $loop->loop_once(0) for 1 .. 3 }

subtest 'a 200 watch response delivers its Warning headers' => sub {
    my @seen;
    my $kube = make_kube(sub { push @seen, $_[0] });

    MockTransport::mock_watch_events($PATH, [$added], {
        complete => 1,
        headers  => { warning => ['299 - "v1 Pod is deprecated"', '299 - "second"'] },
    });
    my $watcher = $kube->watcher('Pod',
        namespace => 'default',
        reconnect_jitter => 0, reconnect_delay => 30, max_reconnect_delay => 30,
        # The reconnect meets an open stream, so the first response counts once.
        on_added => sub { MockTransport::mock_watch_events($PATH, [], {}) },
        on_error => sub { },
    );

    await_requests(2);
    settle();
    $watcher->stop;
    settle();

    is_deeply \@seen, ['v1 Pod is deprecated', 'second'],
        'on_warning fires once per Warning header, in order';
};

done_testing;
