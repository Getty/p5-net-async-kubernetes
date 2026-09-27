package Net::Async::Kubernetes::Watcher;
# ABSTRACT: Auto-reconnecting Kubernetes watch as IO::Async::Notifier
our $VERSION = '0.009';
use strict;
use warnings;
use parent 'IO::Async::Notifier';

use Carp qw(croak);
use Scalar::Util qw(looks_like_number weaken);
use Kubernetes::REST::HTTPResponse;

sub configure {
    my ($self, %params) = @_;

    if (exists $params{kube}) {
        $self->{kube} = delete $params{kube};
        weaken($self->{kube});
    }
    # Checked here: anything else (a Controller-style arrayref of delays, a
    # word) numifies to a delay nobody asked for and the retries go quiet.
    for my $key (qw(reconnect_delay max_reconnect_delay)) {
        next unless exists $params{$key};
        my $value = delete $params{$key};
        croak "$key must be a non-negative number of seconds"
            unless defined $value && !ref $value && looks_like_number($value) && $value >= 0;
        $self->{$key} = $value;
    }
    if (exists $params{max_retries}) {
        my $value = delete $params{max_retries};
        croak "max_retries must be a non-negative integer, or undef for no limit"
            if defined $value && (ref $value || $value !~ /\A[0-9]+\z/);
        $self->{max_retries} = $value;
    }
    for my $key (qw(resource namespace timeout label_selector field_selector
                     names event_types
                     on_added on_modified on_deleted on_error on_event)) {
        if (exists $params{$key}) {
            $self->{$key} = delete $params{$key};
        }
    }

    $self->SUPER::configure(%params);
}

=method configure

Internal L<IO::Async::Notifier> configuration method. Handles initialization
of C<kube>, C<resource>, C<namespace>, C<timeout>, C<label_selector>,
C<field_selector>, C<names>, C<event_types>, C<reconnect_delay>,
C<max_reconnect_delay>, C<max_retries>, and all event callbacks
(C<on_added>, C<on_modified>, C<on_deleted>, C<on_error>, C<on_event>).
Croaks on a C<reconnect_delay> or C<max_reconnect_delay> that is not a
non-negative number, and on a C<max_retries> that is neither a non-negative
integer nor C<undef>.

=cut

# Accessors
sub kube           { $_[0]->{kube} }

=method kube

Returns the parent L<Net::Async::Kubernetes> instance.

=cut

sub resource       { $_[0]->{resource} }

=attr resource

Required. The Kubernetes resource kind to watch (e.g., C<'Pod'>,
C<'Deployment'>), or a qualified C<'group/version/Kind'> name to watch a
specific API version -- see L<Net::Async::Kubernetes/expand_class>.

=cut

sub namespace      { $_[0]->{namespace} }

=attr namespace

Optional. Namespace to watch. Omit for cluster-scoped resources or to
watch all namespaces.

=cut

sub timeout        { $_[0]->{timeout} // 300 }

=attr timeout

Server-side timeout per watch cycle in seconds. Default: 300.

=cut

sub reconnect_delay     { $_[0]->{reconnect_delay} // 1 }

=attr reconnect_delay

Seconds to wait before reconnecting after a failed watch request (see
L</on_error> for what counts as one). Default: 1. Each further consecutive
failure doubles the delay, up to L</max_reconnect_delay>. A reconnect that
gets through -- data arrives on the stream, or the watch cycle ends cleanly
-- starts the next failure at C<reconnect_delay> again. A watch cycle that
ends cleanly (the server-side L</timeout>) is not a failure and reconnects
at once.

=cut

sub max_reconnect_delay { $_[0]->{max_reconnect_delay} // 30 }

=attr max_reconnect_delay

Upper bound in seconds for the reconnect delay. Default: 30, the same cap
client-go's reflector puts on its watch backoff: a cluster that comes back is
noticed within half a minute, and one that stays away costs one request and
one report per half minute.

=cut

sub max_retries         { $_[0]->{max_retries} }

=attr max_retries

How many consecutive failed watch requests are retried before the watcher
gives up. Default: C<undef>, retry for as long as the watcher runs. When the
limit is exceeded the watcher stops and reports that it gave up (see
L</on_error>); C<start()> begins again with the full count. C<0> gives up on
the first failure.

=cut

sub label_selector { $_[0]->{label_selector} }

=attr label_selector

Optional label selector (e.g., C<'app=web,env=prod'>).

=cut

sub field_selector { $_[0]->{field_selector} }

=attr field_selector

Optional field selector (e.g., C<'status.phase=Running'>).

=cut

sub names          { $_[0]->{names} }

=attr names

Optional client-side filter for resource names. Accepts a single regex,
a string (exact match), or an arrayref of regexes/strings. Events whose
resource name does not match any of the patterns are silently dropped
before callbacks fire.

    # Single regex
    names => qr/^nginx/

    # Multiple patterns (any match passes)
    names => [qr/^nginx/, qr/^redis/]

    # Exact string match
    names => 'my-pod'

=cut

sub event_types    { $_[0]->{event_types} }

=attr event_types

Optional client-side filter for event types. Accepts an arrayref of
type strings (C<ADDED>, C<MODIFIED>, C<DELETED>, C<ERROR>). Events
whose type is not in the list are silently dropped.

    # Only ADDED and DELETED events
    event_types => ['ADDED', 'DELETED']

When not set, event types are automatically derived from which callbacks
are registered. If only C<on_added> is set, only C<ADDED> events are
dispatched. If C<on_event> is set (catch-all), all types pass through.

=cut

sub on_added       { $_[0]->{on_added} }

=attr on_added

Callback for ADDED events. Receives the inflated IO::K8s object.

=cut

sub on_modified    { $_[0]->{on_modified} }

=attr on_modified

Callback for MODIFIED events. Receives the inflated IO::K8s object.

=cut

sub on_deleted     { $_[0]->{on_deleted} }

=attr on_deleted

Callback for DELETED events. Receives the inflated IO::K8s object.

=cut

sub on_error       { $_[0]->{on_error} }

=attr on_error

Callback for errors of the watch. Receives a hashref shaped like a Kubernetes
C<Status>, in two cases:

=over 4

=item * An C<ERROR> event on the stream, for example a C<403> arriving
mid-stream: the raw C<Status> hashref the API server sent. C<410 Gone> is
handled internally and never reaches it.

=item * A failed watch request: the request fails outright (TLS support
missing, a TLS or connection error, an unreachable API server) or the API
server rejects it with an HTTP error status (C<401>, C<403>, C<5xx>). The
watcher builds this C<Status> itself. C<reason> is C<WatchFailed>, C<code>
the HTTP status of a rejected request or C<0> when no response arrived,
C<message> names the resource, what the watcher does next and the cause, and
C<details> carries C<kind> (the watched resource) and, while the watcher
retries, C<retryAfterSeconds>:

    {
        kind       => 'Status',
        apiVersion => 'v1',
        status     => 'Failure',
        reason     => 'WatchFailed',
        code       => 0,
        message    => 'watch Pod failed, retrying in 4s: Connection refused',
        details    => { kind => 'Pod', retryAfterSeconds => 4 },
    }

Once L</max_retries> is exceeded, the message says C<giving up after N
retries> instead and C<retryAfterSeconds> is absent; the watcher has
stopped by then.

=back

A failed watch request is reported whatever L</event_types> says. Without an
C<on_error> it is passed to C<warn> instead, so a watch that cannot reach its
cluster never fails silently. C<ERROR> events without an C<on_error> are
dropped, as before. The callback may call C<stop()>, which also cancels the
reconnect just announced.

=cut

sub on_event       { $_[0]->{on_event} }

=attr on_event

Catch-all callback. Receives the L<Kubernetes::REST::WatchEvent> object.
Called in addition to the type-specific callbacks.

=cut

sub _add_to_loop {
    my ($self, $loop) = @_;
    croak "kube is required" unless $self->{kube};
    croak "resource is required" unless $self->{resource};
    $self->start;
}

sub _remove_from_loop {
    my ($self, $loop) = @_;
    $self->stop;
}

sub start {
    my ($self) = @_;
    # Waiting out a reconnect delay is running too: starting now as well
    # would leave two watches once the pending reconnect fires.
    return if $self->{_watching} || $self->{_retry_future};
    $self->{_stopped} = 0;
    $self->{_failures} = 0;
    $self->_start_watch;
}

=method start

Start (or restart) the watch stream. Called automatically when the watcher
is added to the event loop. Safe to call multiple times (idempotent): a
watcher waiting to reconnect after a failed request is already running. After
C<stop()>, or after giving up on L</max_retries>, it starts over with the full
retry count.

=cut

sub stop {
    my ($self) = @_;
    $self->{_stopped} = 1;
    $self->{_watching} = 0;
    if (my $retry = delete $self->{_retry_future}) {
        $retry->cancel;
    }
    if (my $f = delete $self->{_watch_future}) {
        return if $f->is_ready;
        # Defer cancel to next loop iteration to avoid closing the HTTP
        # connection from within its own on_read handler, which triggers
        # Net::Async::HTTP's "Spurious on_read of connection while idle".
        if (my $loop = $self->loop) {
            $loop->later(sub {
                $f->cancel if !$f->is_ready;
            });
        } else {
            $f->cancel;
        }
    }
}

=method stop

Stop the watch stream and cancel the current HTTP request, or a reconnect
that is waiting out its delay. The watcher will not automatically reconnect
until C<start()> is called again.

=cut

sub _start_watch {
    my ($self) = @_;
    return if $self->{_stopped};
    return unless $self->{kube};

    $self->{_watching} = 1;
    $self->{_buffer} = '';

    my $rest = $self->kube->_rest;
    my ($class, $error) = $self->kube->_resolve_class($self->resource);
    croak $error unless defined $class;
    my $path = $rest->build_path($class,
        ($self->namespace ? (namespace => $self->namespace) : ()),
        $self->kube->_unstructured_hint($class, $self->resource),
    );

    my %params = (
        watch          => 'true',
        timeoutSeconds => $self->timeout,
    );
    $params{resourceVersion} = $self->{_resource_version}
        if defined $self->{_resource_version};
    $params{labelSelector} = $self->label_selector
        if defined $self->label_selector;
    $params{fieldSelector} = $self->field_selector
        if defined $self->field_selector;

    my $req = $rest->prepare_request('GET', $path, parameters => \%params);
    # The resolved class, handed over exactly (see the client's _exact_class).
    my $exact_class = $self->kube->_exact_class($class);

    weaken(my $weak_self = $self);

    my $f = $self->kube->_do_streaming_request($req, sub {
        my ($chunk) = @_;
        return unless $weak_self;

        # Data on the stream: this attempt got through, so a failure after
        # it starts the backoff over.
        $weak_self->{_failures} = 0;

        my $buffer = $weak_self->{_buffer};
        for my $result ($rest->process_watch_chunk($exact_class, \$buffer, $chunk)) {
            $weak_self->{_buffer} = $buffer;

            if ($result->{resourceVersion}) {
                $weak_self->{_resource_version} = $result->{resourceVersion};
            }

            my $event = $result->{event};

            if ($result->{error_code} == 410) {
                $weak_self->{_resource_version} = undef;
                return;
            }

            $weak_self->_dispatch_event($event);
        }
        $weak_self->{_buffer} = $buffer;
    });

    $f->on_done(sub {
        my ($response) = @_;
        return unless $weak_self;
        return if $weak_self->{_stopped};
        $weak_self->{_watching} = 0;
        # A rejected request (401, 403, 5xx) resolves like any response. It
        # is a failed attempt, not a watch cycle that ran its course.
        if ($response->status >= 400) {
            my $cause = eval {
                $rest->check_response($response, 'watch ' . $weak_self->resource);
                1;
            } ? 'HTTP ' . $response->status : $@;
            return $weak_self->_watch_failed($cause, $response->status);
        }
        $weak_self->{_failures} = 0;
        $weak_self->_start_watch;
    });

    $f->on_fail(sub {
        my ($error) = @_;
        return unless $weak_self;
        return if $weak_self->{_stopped};
        $weak_self->{_watching} = 0;
        $weak_self->_watch_failed($error);
    });

    $self->{_watch_future} = $f;
}

# A watch request that failed outright, or was rejected with HTTP status
# $code. Schedules the next attempt with exponential backoff - or, once
# max_retries consecutive failures have been retried, stops the watcher - and
# reports the failure either way: to on_error, else as a warning.
sub _watch_failed {
    my ($self, $cause, $code) = @_;
    my $failures = ++$self->{_failures};
    my $max_retries = $self->max_retries;
    $cause = defined $cause ? "$cause" : 'unknown error';
    $cause =~ s/\s+\z//;

    my ($delay, $next);
    if (defined $max_retries && $failures > $max_retries) {
        # Stopped before the report, so an on_error that restarts the watcher
        # is not undone right after.
        $self->stop;
        $next = sprintf('giving up after %d %s',
            $max_retries, $max_retries == 1 ? 'retry' : 'retries');
    } else {
        # The exponent is bounded so a long outage cannot overflow it to inf,
        # which reconnect_delay => 0 would turn into a NaN delay.
        my $exponent = $failures - 1;
        $exponent = 64 if $exponent > 64;
        $delay = $self->reconnect_delay * 2 ** $exponent;
        $delay = $self->max_reconnect_delay if $delay > $self->max_reconnect_delay;

        # Scheduled before the report, so an on_error that stops the watcher
        # cancels it.
        weaken(my $weak_self = $self);
        $self->{_retry_future} = $self->loop->delay_future(after => $delay)->on_done(sub {
            return unless $weak_self;
            delete $weak_self->{_retry_future};
            $weak_self->_start_watch;
        });
        $next = 'retrying in ' . $delay . 's';
    }

    my $status = {
        kind       => 'Status',
        apiVersion => 'v1',
        status     => 'Failure',
        reason     => 'WatchFailed',
        code       => $code // 0,
        message    => 'watch ' . $self->resource . ' failed, ' . $next . ': ' . $cause,
        details    => {
            kind => $self->resource,
            (defined $delay ? (retryAfterSeconds => $delay) : ()),
        },
    };

    if (my $cb = $self->on_error) {
        $cb->($status);
    } else {
        warn $status->{message} . "\n";
    }
}

sub _dispatch_event {
    my ($self, $event) = @_;
    my $type = $event->type;

    # Client-side event type filter
    # Explicit event_types wins; otherwise auto-derive from callbacks
    # (on_event is catch-all, so if set, all types pass)
    if (my $types = $self->event_types) {
        my %allowed = map { uc($_) => 1 } @$types;
        return unless $allowed{$type};
    } elsif (!$self->on_event) {
        my %has;
        $has{ADDED}    = 1 if $self->on_added;
        $has{MODIFIED} = 1 if $self->on_modified;
        $has{DELETED}  = 1 if $self->on_deleted;
        $has{ERROR}    = 1 if $self->on_error;
        return unless !%has || $has{$type};
    }

    # Client-side name filter (skip for ERROR events which have no metadata)
    if ($type ne 'ERROR' && (my $names = $self->names)) {
        my $obj_name = eval { $event->object->metadata->name } // '';
        my @patterns = ref $names eq 'ARRAY' ? @$names : ($names);
        my $matched = 0;
        for my $pat (@patterns) {
            if (ref $pat eq 'Regexp') {
                $matched = 1, last if $obj_name =~ $pat;
            } else {
                $matched = 1, last if $obj_name eq $pat;
            }
        }
        return unless $matched;
    }

    if (my $cb = $self->on_event) {
        $cb->($event);
    }

    if ($type eq 'ADDED' && $self->on_added) {
        $self->on_added->($event->object);
    } elsif ($type eq 'MODIFIED' && $self->on_modified) {
        $self->on_modified->($event->object);
    } elsif ($type eq 'DELETED' && $self->on_deleted) {
        $self->on_deleted->($event->object);
    } elsif ($type eq 'ERROR' && $self->on_error) {
        $self->on_error->($event->object);
    }
}

1;

__END__

=encoding UTF-8

=head1 SYNOPSIS

    my $watcher = $kube->watcher('Pod',
        namespace      => 'default',
        label_selector => 'app=web',
        on_added       => sub { my ($pod) = @_; say "Added: " . $pod->metadata->name },
        on_modified    => sub { my ($pod) = @_; say "Modified: " . $pod->metadata->name },
        on_deleted     => sub { my ($pod) = @_; say "Deleted: " . $pod->metadata->name },
        on_error       => sub { my ($status) = @_; warn "Error: $status->{message}" },
    );

    # Client-side filtering by name and event type
    $kube->watcher('Pod',
        namespace   => 'default',
        names       => [qr/^nginx/, qr/^redis/],  # only matching names
        event_types => ['ADDED', 'DELETED'],        # skip MODIFIED
        on_added    => sub { ... },
        on_deleted  => sub { ... },
    );

    # Watch multiple resources concurrently
    $kube->watcher('Deployment', namespace => 'production', on_modified => sub { ... });
    $kube->watcher('Service', namespace => 'production', on_added => sub { ... });

    # Stop watching
    $watcher->stop;

    # Restart
    $watcher->start;

=head1 DESCRIPTION

An L<IO::Async::Notifier> that watches a Kubernetes resource for changes.
Created via L<Net::Async::Kubernetes/watcher>.

The watcher automatically:

=over 4

=item * Reconnects when the server-side timeout expires

=item * Resumes from the last C<resourceVersion> to avoid missing events

=item * Handles 410 Gone by clearing the C<resourceVersion> and restarting

=item * Retries a failed watch request -- a transport error, or a rejection
such as C<401> or C<403> -- with an exponential backoff (1s, 2s, 4s, ... up
to 30s by default, see L</reconnect_delay>), reports every such failure to
L</on_error> or as a warning, and gives up after L</max_retries> consecutive
failures when a limit is set

=item * Filters events client-side by name patterns (C<names>) and event types (C<event_types>)

=back

=head1 SEE ALSO

L<Net::Async::Kubernetes>, L<Kubernetes::REST::WatchEvent>,
L<IO::Async::Notifier>

=cut
