package Net::Async::Kubernetes::PortForwardSession;
# ABSTRACT: Duplex websocket session for pod port-forward, exec and attach
our $VERSION = '0.010';
use strict;
use warnings;
use Carp qw(croak);

sub new {
    my ($class, %args) = @_;
    croak "ws_client required" unless $args{ws_client};
    return bless \%args, $class;
}

=method new

    my $session = Net::Async::Kubernetes::PortForwardSession->new(
        ws_client => $ws_client,
    );

Wraps an already-connected websocket client (normally a
L<Net::Async::WebSocket::Client>) as a session. C<ws_client> is required.

Callers do not normally build a session this way: the C<Future> returned by
L<Net::Async::Kubernetes/port_forward>, L<Net::Async::Kubernetes/exec>, and
L<Net::Async::Kubernetes/attach> resolves to one, already bound to the open
connection, and it is what those methods pass to an C<on_open> callback.

=cut

sub ws_client { $_[0]->{ws_client} }

sub write_channel {
    my ($self, $channel, $payload) = @_;

    croak "channel required for write_channel" unless defined $channel;
    croak "invalid channel '$channel' for write_channel"
        unless $channel =~ /^\d+$/ && $channel >= 0 && $channel <= 255;

    $payload = '' unless defined $payload;
    croak "payload must be a plain string for write_channel" if ref($payload);

    return $self->ws_client->send_binary_frame(chr($channel) . $payload);
}

=method write_channel

    $session->write_channel(0, "id\n");

Send C<$payload> as a single binary websocket frame on the given Kubernetes
stream C<$channel>, with the channel number prepended as the frame's first
byte (C<chr($channel) . $payload>). C<$channel> is required and must be an
integer between 0 and 255; C<$payload> defaults to the empty string.

C<write_channel> itself does not know what any channel number means, it only
frames whatever it is given; the meaning comes from the Kubernetes
sub-protocol of the session.

For an exec or attach session, 0 is stdin, 1 is stdout, 2 is stderr, 3
carries an error/status JSON document, and 4 carries TTY resize events (see
L</resize>).

A port-forward session has a different layout: each requested port C<$i>
(counted from 0, in the order of C<ports>) owns two channels, C<2 * $i> for
data and C<2 * $i + 1> for errors, so with two ports channel 3 is the error
channel of the second one. Data for port C<$i> goes out with
C<< write_channel(2 * $i, $bytes) >>. The first frame the server sends on each
of these channels is the port number as two little-endian bytes, not payload;
it reaches C<on_frame> unstripped.

C<on_frame>, passed to L<Net::Async::Kubernetes/exec> and friends, splits
incoming frames into C<($channel, $payload)> the same way in reverse; it
does not interpret the channel number.

Returns the L<Future> from the underlying C<ws_client>'s
C<send_binary_frame> - an L<IO::Async::Stream> write-future that completes
with no value once the frame has been flushed to the connection.

Aliased as C<write>.

=cut

sub write_stdin {
    my ($self, $payload) = @_;
    return $self->write_channel(0, $payload);
}

=method write_stdin

    $session->write_stdin("id\n");

Shortcut for C<< $session->write_channel(0, $payload) >> - writes to channel
0, the stdin stream of an exec or attach session's remote process. On a
port-forward session channel 0 is the data channel of the first port, not
stdin; use L</write_channel> there.

Returns the L<Future> from the underlying C<write_channel> call.

Aliased as C<stdin>.

=cut

{
    no warnings 'once';
    *write = \&write_channel;
    *stdin = \&write_stdin;
}

sub resize {
    my ($self, %args) = @_;

    my $width  = exists($args{width})  ? $args{width}  : $args{cols};
    my $height = exists($args{height}) ? $args{height} : $args{rows};

    croak "width required for resize" unless defined $width;
    croak "height required for resize" unless defined $height;
    croak "invalid width '$width' for resize"
        unless $width =~ /^\d+$/ && $width > 0;
    croak "invalid height '$height' for resize"
        unless $height =~ /^\d+$/ && $height > 0;

    my $payload = sprintf('{"Width":%d,"Height":%d}', $width, $height);
    return $self->write_channel(4, $payload);
}

=method resize

    $session->resize(width => 120, height => 40);
    $session->resize(cols  => 120, rows   => 40);   # cols/rows are accepted too

Send a TTY resize event on channel 4, as a C<{"Width":$width,"Height":$height}>
JSON payload. C<width> and C<height> (or their C<cols>/C<rows> aliases) are
required and must be positive integers. Only meaningful for a session opened
with C<tty =E<gt> 1>. Not for port-forward sessions, where channel 4 is the
data channel of the third port.

Returns the L<Future> from the underlying C<write_channel> call.

=cut

sub close {
    my ($self, %args) = @_;
    my $code = $args{code};
    my $payload = exists $args{payload} ? $args{payload} : '';

    croak "payload must be a plain string for close" if ref($payload);
    croak "invalid websocket close code '$code'"
        if defined($code) && ($code !~ /^\d+$/ || $code < 1000 || $code > 4999);

    my $close_payload = defined($code) ? pack('n', $code) . $payload : $payload;
    my $ret = $self->ws_client->send_close_frame($close_payload);
    $self->ws_client->close_when_empty if $self->ws_client->can('close_when_empty');
    return $ret;
}

=method close

    $session->close(code => 1000, payload => 'bye');
    $session->close;   # close frame with no code

Send a websocket close frame, ending the session. C<code>, if given, must be
a websocket close code between 1000 and 4999; it is packed as a 16-bit
big-endian prefix (C<pack('n', $code)>) ahead of C<payload>, which defaults
to the empty string. Once the close frame is sent, the underlying transport
is also asked to close the connection once its write buffer drains
(C<close_when_empty>), if the transport supports that method.

Returns the L<Future> from the underlying C<ws_client>'s C<send_close_frame>
- an L<IO::Async::Stream> write-future that completes with no value once the
close frame has been flushed to the connection.

=cut

1;

__END__

=encoding UTF-8

=head1 SYNOPSIS

    my $session = $kube->exec('Pod', 'my-pod',
        namespace => 'default',
        command   => ['sh'],
        tty       => 1,
        on_frame  => sub {
            my ($channel, $payload) = @_;
            print $payload if $channel == 1;   # stdout
        },
    )->get;

    $session->write_stdin("id\n");
    $session->resize(width => 120, height => 40);
    $session->close(code => 1000);

=head1 DESCRIPTION

A duplex session handle for the Kubernetes exec/attach/port-forward
websocket sub-protocol (C<v4.channel.k8s.io> by default). Every frame on the
wire carries a channel number as its first byte, identifying which logical
stream the rest of the frame belongs to. For exec and attach these are stdin,
stdout, stderr, error/status and TTY resize (channels 0 to 4); for
port-forward they are a data and an error channel per forwarded port
(C<2 * $i> and C<2 * $i + 1>), each opened by a two-byte port-number frame.
C<write_channel> is the primitive that builds such a frame; C<write_stdin>
and C<resize> are convenience wrappers over channels 0 and 4 respectively,
meaningful for exec and attach only.

Callers do not normally construct this class themselves: it is what
L<Net::Async::Kubernetes/port_forward>, L<Net::Async::Kubernetes/exec>, and
L<Net::Async::Kubernetes/attach> resolve their returned C<Future> with, and
what they pass to an C<on_open> callback if one was given. Incoming frames
are not read through this object; they arrive via the C<on_frame> callback
passed to those methods, already decoded into C<($channel, $payload)> pairs.

=head1 SEE ALSO

L<Net::Async::Kubernetes>, L<Net::Async::WebSocket::Client>,
L<IO::Async::Stream>

=cut
