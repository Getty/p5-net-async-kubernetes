package Net::Async::Kubernetes::PortForwardSession;
# ABSTRACT: Duplex websocket session for pod port-forward, exec and attach
our $VERSION = '0.008';
use strict;
use warnings;
use Carp qw(croak);

sub new {
    my ($class, %args) = @_;
    croak "ws_client required" unless $args{ws_client};
    return bless \%args, $class;
}

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

sub write_stdin {
    my ($self, $payload) = @_;
    return $self->write_channel(0, $payload);
}

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

1;
