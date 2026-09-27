use strict;
use warnings;
use Test::More;

use lib 't/lib';

use IO::Async::Loop;
use Net::Async::Kubernetes;
use Net::Async::Kubernetes::Watcher;
use MockTransport;

# A watch request that fails outright (transport error) or is rejected (an
# HTTP error status) is reported to on_error - or warned about without one -
# and retried with an exponential backoff, optionally capped by max_retries.
# Mock mode only.

my $loop = IO::Async::Loop->new;

# The watcher schedules a reconnect through the loop's delay_future. Replace
# it with a manual timer: every delay asked for is recorded, and the retry
# runs only when the test resolves that future, so no real time passes.
my @timers;
{
    no strict 'refs';
    no warnings 'redefine';
    my $loop_class = ref $loop;
    *{"${loop_class}::delay_future"} = sub {
        my ($self, %args) = @_;
        my $f = $self->new_future;
        push @timers, { after => $args{after}, future => $f };
        return $f;
    };
}

my $PATH = '/api/v1/namespaces/default/pods';

my $added_event = { type => 'ADDED', object => {
    kind => 'Pod', apiVersion => 'v1',
    metadata => { name => 'pod-1', namespace => 'default', resourceVersion => '10' },
    spec => { containers => [{ name => 'nginx', image => 'nginx' }] },
    status => {},
}};

# A path without mock_watch_events fails every watch request at once, with
# "Mock: no watch events for <path>".
sub make_kube {
    MockTransport::reset();
    @timers = ();
    my $kube = Net::Async::Kubernetes->new(
        server      => { endpoint => 'https://mock.local' },
        credentials => { token => 'mock-token' },
        resource_map_from_cluster => 0,
    );
    MockTransport::install($kube);
    $loop->add($kube);
    return $kube;
}

# Run what the mock deferred with $loop->later.
sub settle { $loop->loop_once(0) for 1 .. 3 }

sub watch_requests { scalar grep { $_->{streaming} } MockTransport::request_log() }

# Let the most recently scheduled reconnect happen.
sub fire_timer {
    my $timer = $timers[-1];
    return fail('a reconnect is scheduled') unless $timer && !$timer->{future}->is_ready;
    $timer->{future}->done;
    return 1;
}

sub delays { [ map { $_->{after} } @timers ] }

subtest 'a failed watch request reaches on_error with its cause' => sub {
    my $kube = make_kube();
    MockTransport::mock_watch_events($PATH, [], { fail => 'SSL connect attempt failed' });

    my @errors;
    my $watcher = $kube->watcher('Pod',
        namespace => 'default',
        on_added  => sub {},
        on_error  => sub { push @errors, $_[0] },
    );
    settle();

    is(scalar @errors, 1, 'one report for the failed request');
    my $error = $errors[0] || {};
    is($error->{kind}, 'Status', 'reported as a Status hashref, like an ERROR event');
    is($error->{status}, 'Failure', 'status is Failure');
    is($error->{reason}, 'WatchFailed', 'reason marks a failed watch request');
    is($error->{code}, 0, 'code 0: no HTTP response arrived');
    like($error->{message}, qr/^watch Pod failed, retrying in 1s: SSL connect attempt failed/,
        'message names the resource, the delay and the cause');
    is($error->{details}{kind}, 'Pod', 'details name the watched resource');
    is($error->{details}{retryAfterSeconds}, 1, 'retryAfterSeconds carries the delay');
    is_deeply(delays(), [1], 'one reconnect scheduled, after reconnect_delay');
    is(watch_requests(), 1, 'nothing is sent before the delay has passed');

    fire_timer();
    is(watch_requests(), 2, 'the reconnect sends the watch request again');
    $watcher->stop;
};

subtest 'the reconnect delay doubles up to max_reconnect_delay' => sub {
    my $kube = make_kube();
    my @errors;
    my $watcher = $kube->watcher('Pod',
        namespace => 'default',
        on_added  => sub {},
        on_error  => sub { push @errors, $_[0] },
    );
    is($watcher->reconnect_delay, 1, 'reconnect_delay defaults to 1');
    is($watcher->max_reconnect_delay, 30, 'max_reconnect_delay defaults to 30');
    is($watcher->max_retries, undef, 'max_retries unlimited by default');

    fire_timer() for 1 .. 6;
    is_deeply(delays(), [1, 2, 4, 8, 16, 30, 30], 'doubling, capped at 30s');
    is_deeply([ map { $_->{details}{retryAfterSeconds} } @errors ], [1, 2, 4, 8, 16, 30, 30],
        'every report announces its delay');
    is(watch_requests(), 7, 'one watch request per attempt');
    $watcher->stop;
};

subtest 'reconnect_delay and max_reconnect_delay are configurable' => sub {
    my $kube = make_kube();
    my $watcher = $kube->watcher('Pod',
        namespace           => 'default',
        reconnect_delay     => 0.5,
        max_reconnect_delay => 3,
        on_added            => sub {},
        on_error            => sub {},
    );
    fire_timer() for 1 .. 4;
    is_deeply(delays(), [0.5, 1, 2, 3, 3], 'configured start and cap');
    $watcher->stop;

    for my $bad (
        [ reconnect_delay     => [1, 2] ],
        [ reconnect_delay     => -1 ],
        [ max_reconnect_delay => 'soon' ],
        [ max_retries         => -1 ],
        [ max_retries         => 1.5 ],
    ) {
        my ($key, $value) = @$bad;
        my $shown = ref $value ? 'an arrayref' : "'$value'";
        eval { Net::Async::Kubernetes::Watcher->new(resource => 'Pod', $key => $value) };
        like($@, qr/^\Q$key\E must be /, "$key rejects $shown");
    }
};

subtest 'max_retries stops the watcher and says so' => sub {
    my $kube = make_kube();
    my @errors;
    my $watcher = $kube->watcher('Pod',
        namespace   => 'default',
        max_retries => 2,
        on_added    => sub {},
        on_error    => sub { push @errors, $_[0] },
    );
    fire_timer() for 1 .. 2;

    is(watch_requests(), 3, 'the first attempt plus two retries');
    is(scalar @errors, 3, 'every failed attempt is reported');
    is_deeply(delays(), [1, 2], 'no reconnect scheduled after the last retry');
    my $last = $errors[-1] || {};
    like($last->{message}, qr/^watch Pod failed, giving up after 2 retries: Mock: no watch events/,
        'the last report says the watcher gave up');
    is($last->{reason}, 'WatchFailed', 'same reason as the retried failures');
    ok(!exists $last->{details}{retryAfterSeconds}, 'no retryAfterSeconds once it gave up');
    settle();
    is(watch_requests(), 3, 'the watcher stays stopped');

    $watcher->start;
    is(watch_requests(), 4, 'start() sends the watch request again');
    is($timers[-1]{after}, 1, 'the backoff starts over');
    like($errors[-1]{message}, qr/retrying in 1s/, 'with the full retry budget');
    $watcher->stop;
};

subtest 'max_retries => 0 gives up on the first failure' => sub {
    my $kube = make_kube();
    my @errors;
    my $watcher = $kube->watcher('Pod',
        namespace   => 'default',
        max_retries => 0,
        on_added    => sub {},
        on_error    => sub { push @errors, $_[0] },
    );
    is(scalar @errors, 1, 'the failure is reported');
    like($errors[0]{message}, qr/giving up after 0 retries/, 'as giving up');
    is(scalar @timers, 0, 'no reconnect scheduled');
    $watcher->stop;
};

subtest 'a reconnect that receives data resets the backoff' => sub {
    my $kube = make_kube();
    my (@errors, @added);
    my $watcher = $kube->watcher('Pod',
        namespace => 'default',
        on_added  => sub { push @added, $_[0]->metadata->name },
        on_error  => sub { push @errors, $_[0] },
    );
    fire_timer();
    is($timers[-1]{after}, 2, 'two consecutive failures: 2s');

    MockTransport::mock_watch_events($PATH, [$added_event], { fail => 'Connection reset by peer' });
    fire_timer();
    settle();

    is_deeply(\@added, ['pod-1'], 'the reconnect got through and delivered its event');
    is($timers[-1]{after}, 1, 'the failure after it starts the backoff over');
    like($errors[-1]{message}, qr/retrying in 1s: Connection reset by peer/,
        'and is reported with its own cause');
    $watcher->stop;
};

subtest 'a watch cycle that completes cleanly resets the backoff' => sub {
    my $kube = make_kube();
    my @errors;
    # The mocked stream ends at once, without an event, which counts as a
    # failure since karr k51 (t/41 covers that rule, and a quiet stream that
    # ran its course, on a virtual clock). Off here: this is about the reset.
    my $watcher = $kube->watcher('Pod',
        namespace          => 'default',
        min_watch_duration => 0,
        on_added           => sub {},
        on_error           => sub { push @errors, $_[0] },
    );
    fire_timer();
    is($timers[-1]{after}, 2, 'two consecutive failures: 2s');

    MockTransport::mock_watch_events($PATH, [], { complete => 1 });
    fire_timer();
    $loop->loop_once(0);
    is(watch_requests(), 4, 'a clean end reconnects at once, without a delay');
    is(scalar @timers, 2, 'and schedules nothing');

    MockTransport::mock_watch_events($PATH, [], { fail => 'Connection refused' });
    settle();
    is($timers[-1]{after}, 1, 'the next failure starts the backoff over');
    is(scalar @errors, 3, 'the clean end itself was not reported');
    $watcher->stop;
};

subtest 'a rejected watch request (HTTP 403) is a failed attempt' => sub {
    my $kube = make_kube();
    MockTransport::mock_watch_events($PATH, [], { complete => 1, status => 403 });

    my @errors;
    my $watcher = $kube->watcher('Pod',
        namespace => 'default',
        on_added  => sub {},
        on_error  => sub { push @errors, $_[0] },
    );
    settle();

    is(watch_requests(), 1, 'no immediate reconnect after a rejection');
    is(scalar @errors, 1, 'the rejection is reported');
    is($errors[0]{code}, 403, 'code carries the HTTP status');
    like($errors[0]{message},
        qr/^watch Pod failed, retrying in 1s: Kubernetes API error \(watch Pod\): 403/,
        'message carries the API error');
    is_deeply(delays(), [1], 'retried after the reconnect delay');
    $watcher->stop;
};

# karr k53: the real transport never streams the body of a rejected request -
# it keeps it for check_response (k33). The mock does the same: events
# registered for a watch answered with an error status are not delivered.
subtest 'a rejected watch request delivers no events, only its error body' => sub {
    my $kube = make_kube();
    MockTransport::mock_watch_events($PATH, [$added_event], { status => 403 });

    my (@errors, @added);
    my $watcher = $kube->watcher('Pod',
        namespace => 'default',
        on_added  => sub { push @added, $_[0]->metadata->name },
        on_error  => sub { push @errors, $_[0] },
    );
    settle();

    is_deeply(\@added, [], 'the registered event is not delivered');
    is(scalar @errors, 1, 'the rejection is reported, without complete => 1');
    is($errors[0] && $errors[0]{code}, 403, 'code carries the HTTP status');
    like($errors[0] && $errors[0]{message},
        qr/Kubernetes API error \(watch Pod\): 403 \{.*"code":403/,
        'the cause carries the Status error body');
    is_deeply(delays(), [1], 'retried after the reconnect delay');
    $watcher->stop;
};

subtest 'without on_error a failed watch request warns' => sub {
    my $kube = make_kube();
    MockTransport::mock_watch_events($PATH, [], { fail => 'Connection refused' });

    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    my $watcher = $kube->watcher('Pod',
        namespace => 'default',
        on_added  => sub {},
    );
    settle();

    is(scalar @warnings, 1, 'one warning for the failed request');
    like($warnings[0] // '', qr/^watch Pod failed, retrying in 1s: Connection refused\n\z/,
        'the warning carries the report message');
    is_deeply(delays(), [1], 'and the watcher still retries');
    $watcher->stop;
};

subtest 'stop() cancels a pending reconnect, also from on_error' => sub {
    my $kube = make_kube();
    my $watcher = $kube->watcher('Pod',
        namespace => 'default',
        on_added  => sub {},
        on_error  => sub {},
    );
    is(scalar @timers, 1, 'a reconnect is pending');
    $watcher->stop;
    ok($timers[0]{future}->is_cancelled, 'stop() cancelled it');

    my $kube2 = make_kube();
    my $stopper;
    $stopper = $kube2->watcher('Pod',
        namespace => 'default',
        on_added  => sub {},
        on_error  => sub { $stopper->stop if $stopper },
    );
    fire_timer();
    ok($timers[-1]{future}->is_cancelled, 'a stop() inside on_error cancels the reconnect it announced');
    settle();
    is(watch_requests(), 2, 'nothing more is sent');
};

subtest 'start() while a reconnect is pending opens no second watch' => sub {
    my $kube = make_kube();
    my $watcher = $kube->watcher('Pod',
        namespace => 'default',
        on_added  => sub {},
        on_error  => sub {},
    );
    $watcher->start;
    is(watch_requests(), 1, 'start() during the reconnect delay is a no-op');
    fire_timer();
    is(watch_requests(), 2, 'the pending reconnect runs, once');
    $watcher->stop;
};

subtest 'watch ERROR events are unchanged' => sub {
    my $kube = make_kube();
    MockTransport::mock_watch_events($PATH, [
        { type => 'ERROR', object => {
            kind => 'Status', apiVersion => 'v1', status => 'Failure',
            reason => 'Forbidden', code => 403, message => 'pods is forbidden',
        }},
    ]);

    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    my @events;
    my $watcher = $kube->watcher('Pod',
        namespace => 'default',
        on_event  => sub { push @events, $_[0] },
    );
    settle();

    is(scalar @events, 1, 'the ERROR event is dispatched as an event');
    is($events[0] && $events[0]->object->{reason}, 'Forbidden', 'carrying the raw Status');
    is_deeply(\@warnings, [], 'an ERROR event without on_error stays silent');
    is(scalar @timers, 0, 'an ERROR event schedules no reconnect');
    $watcher->stop;
};

subtest 'controller: transport failures reach on_watch_error' => sub {
    my $kube = make_kube();
    MockTransport::mock_watch_events($PATH, [], { fail => 'Connection refused' });

    my @errors;
    my $controller = $kube->controller(
        on_reconcile   => sub { return },
        on_watch_error => sub { push @errors, [@_] },
    );
    $controller->watch_resource('Pod', namespace => 'default', max_retries => 3);
    settle();

    is(scalar @errors, 1, 'the failed watch request reached on_watch_error');
    my ($error, $ctx) = @{ $errors[0] || [] };
    is($error && $error->{reason}, 'WatchFailed', 'as the watcher report');
    like($error && $error->{message}, qr/Connection refused/, 'with its cause');
    is($ctx && $ctx->{resource}, 'Pod', 'context names the watched resource');
    is($ctx && $ctx->{controller}, $controller, 'context carries the controller');

    $controller->stop;
    ok(@timers && $timers[-1]{future}->is_cancelled, 'controller stop() cancels the watch reconnect');
    $controller->remove_from_parent;
};

done_testing;
