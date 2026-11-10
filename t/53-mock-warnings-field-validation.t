#!/usr/bin/env perl
use strict;
use warnings;
use Test::More;

use Future;
use URI;
use JSON::MaybeXS;
use HTTP::Response;
use IO::Async::Loop;
use Net::Async::HTTP;
use Net::Async::Kubernetes;

# Kubernetes::REST's on_warning and field_validation reach the async client
# (k84). The API server reports what it did not do as asked - a dropped
# field, a deprecated API - only in Warning response headers, so the async
# transport has to hand the response headers on; and fieldValidation is a
# query parameter of every write, which the async write path has to send.
#
# The HTTP layer is faked at Net::Async::HTTP::do_request, as in
# t/52-list-paging.t, so the assertions see the URI on the wire and the
# headers the transport receives.

my $loop = IO::Async::Loop->new;
my $JSON = JSON::MaybeXS->new(utf8 => 1);

sub make_kube {
    my (%extra) = @_;
    my $kube = Net::Async::Kubernetes->new(
        server      => { endpoint => 'https://mock.local' },
        credentials => { token => 'mock-token' },
        resource_map_from_cluster => 0,
        %extra,
    );
    $loop->add($kube);
    return $kube;
}

my $pod_body = {
    kind => 'Pod', apiVersion => 'v1',
    metadata => { name => 'web', namespace => 'default' },
    spec => { containers => [{ name => 'c', image => 'nginx' }] },
};

sub pod { return make_kube()->new_object(Pod => $pod_body) }

# Answers every request with $status and $headers (name/value pairs, a name
# may repeat); returns the URIs requested.
sub with_response {
    my ($status, $headers, $code) = @_;
    my @uris;
    no warnings 'redefine';
    local *Net::Async::HTTP::do_request = sub {
        my ($http, %req) = @_;
        push @uris, $req{uri};
        return Future->done(HTTP::Response->new(
            $status, 'X', ['Content-Type' => 'application/json', @$headers],
            $JSON->encode($status < 400 ? $pod_body : { kind => 'Status', message => 'nope', code => $status })));
    };
    $code->();
    return @uris;
}

sub query { return { URI->new("$_[0]")->query_form } }

subtest 'Warning headers reach on_warning' => sub {
    my @seen;
    my $kube = make_kube(on_warning => sub { push @seen, [@_] });
    with_response(200, [
        Warning => '299 - "unknown field \"spec.nodeIP\""',
        Warning => '299 - "second"',
    ], sub { $kube->get('Pod', 'web', namespace => 'default')->get });

    is scalar @seen, 2, 'one call per Warning header';
    is $seen[0][0], 'unknown field "spec.nodeIP"', 'the quoted text, unescaped';
    is $seen[1][0], 'second', 'in header order';
    ok defined $seen[0][1], 'a context is passed along';
};

subtest 'a Warning on an error response is delivered before the failure' => sub {
    my @seen;
    my $kube = make_kube(on_warning => sub { push @seen, $_[0] });
    with_response(404, [Warning => '299 - "gone soon"'], sub {
        my $f = $kube->get('Pod', 'web', namespace => 'default');
        ok $f->is_failed, 'the 404 fails the Future';
    });
    is_deeply \@seen, ['gone soon'], 'warning delivered';
};

subtest 'no on_warning, no Warning header: nothing happens' => sub {
    my $kube = make_kube();
    with_response(200, [Warning => '299 - "ignored"'], sub {
        ok $kube->get('Pod', 'web', namespace => 'default')->get, 'works without on_warning';
    });
    my @seen;
    $kube = make_kube(on_warning => sub { push @seen, $_[0] });
    with_response(200, [], sub { $kube->get('Pod', 'web', namespace => 'default')->get });
    is scalar @seen, 0, 'no header, no call';
};

subtest 'a streaming response hands its headers on too' => sub {
    my $kube = make_kube();
    my $http_resp = HTTP::Response->new(200, 'OK', ['Warning' => '299 - "w"', 'X-Other' => 'a']);
    no warnings 'redefine';
    local *Net::Async::HTTP::do_request = sub {
        my ($http, %req) = @_;
        my $handler = $req{on_header}->($http_resp);
        $handler->('chunk');
        return Future->done($handler->());
    };
    my $req = $kube->rest->prepare_request('GET', '/api/v1/pods');
    my $res = $kube->_do_streaming_request($req, sub { })->get;
    is_deeply $res->headers, { warning => ['299 - "w"'], 'x-other' => ['a'] },
        'lower-cased names, arrayref of values';
};

subtest 'fieldValidation on writes' => sub {
    my $default = make_kube(field_validation => 'Warn');
    my $plain   = make_kube();

    my @u = with_response(200, [], sub { $plain->create(pod())->get });
    is_deeply query($u[0]), {}, 'create: nothing sent without a default';

    @u = with_response(200, [], sub { $default->create(pod())->get });
    is_deeply query($u[0]), { fieldValidation => 'Warn' }, 'create: the client default';

    @u = with_response(200, [], sub { $default->create(pod(), fieldValidation => 'Strict')->get });
    is_deeply query($u[0]), { fieldValidation => 'Strict' }, 'create: the call wins';

    @u = with_response(200, [], sub { $default->update(pod(), fieldValidation => 'Ignore')->get });
    is_deeply query($u[0]), { fieldValidation => 'Ignore' }, 'update';

    @u = with_response(200, [], sub { $default->update_status(pod(), fieldValidation => 'Strict')->get });
    is_deeply query($u[0]), { fieldValidation => 'Strict' }, 'update_status';
    like $u[0], qr{/status\?}, '... on the status subresource';

    @u = with_response(200, [], sub {
        $default->patch('Pod', 'web', namespace => 'default', patch => {}, fieldValidation => 'Strict')->get });
    is_deeply query($u[0]), { fieldValidation => 'Strict' }, 'patch by name';

    @u = with_response(200, [], sub { $default->patch(pod(), patch => {})->get });
    is_deeply query($u[0]), { fieldValidation => 'Warn' }, 'patch by object: the client default';

    @u = with_response(200, [], sub {
        $default->patch_status(pod(), patch => {}, fieldValidation => 'Ignore')->get });
    is_deeply query($u[0]), { fieldValidation => 'Ignore' }, 'patch_status';
};

subtest 'ensure creates with the client default' => sub {
    my $kube = make_kube(field_validation => 'Strict');
    my (@methods, @u);
    no warnings 'redefine';
    local *Net::Async::HTTP::do_request = sub {
        my ($http, %req) = @_;
        push @methods, $req{method};
        push @u, $req{uri};
        my $status = $req{method} eq 'GET' ? 404 : 201;
        return Future->done(HTTP::Response->new($status, 'X', ['Content-Type' => 'application/json'],
            $JSON->encode($status == 404 ? { kind => 'Status', code => 404 } : $pod_body)));
    };
    $kube->ensure(pod())->get;
    is_deeply \@methods, [qw(GET POST)], 'GET, then create';
    is_deeply query($u[1]), { fieldValidation => 'Strict' }, 'the create carries the default';
};

# Scripted answers, one per request in order; returns [ method, uri ] pairs.
sub with_script {
    my ($script, $code) = @_;
    my @log;
    my @queue = @$script;
    no warnings 'redefine';
    local *Net::Async::HTTP::do_request = sub {
        my ($http, %req) = @_;
        push @log, [ $req{method}, $req{uri} ];
        my $status = shift @queue or die "no answer left for $req{method}\n";
        my $body = $status == 409 ? { kind => 'Status', code => 409, message => 'Conflict' }
                 : { %$pod_body, metadata => { %{ $pod_body->{metadata} }, resourceVersion => '7' } };
        return Future->done(HTTP::Response->new($status, 'X',
            ['Content-Type' => 'application/json'], $JSON->encode($body)));
    };
    $code->();
    return @log;
}

subtest 'ensure updates with the client default' => sub {
    my $kube = make_kube(field_validation => 'Strict');
    my @log = with_script([200, 200], sub { $kube->ensure(pod())->get });
    is_deeply [ map { $_->[0] } @log ], [qw(GET PUT)], 'GET, then PUT';
    is_deeply query($log[1][1]), { fieldValidation => 'Strict' }, 'the PUT carries the default';

    $kube = make_kube();
    @log = with_script([200, 200], sub { $kube->ensure(pod())->get });
    is_deeply query($log[1][1]), {}, 'without a default, nothing is sent';
};

subtest 'the retry after a 409 in ensure takes the client default' => sub {
    my $kube = make_kube(field_validation => 'Warn');
    my @log = with_script([200, 409, 200, 200], sub { $kube->ensure(pod())->get });
    is_deeply [ map { $_->[0] } @log ], [qw(GET PUT GET PUT)], 'GET, PUT(409), GET, PUT';
    is_deeply query($log[1][1]), { fieldValidation => 'Warn' }, 'the first PUT';
    is_deeply query($log[3][1]), { fieldValidation => 'Warn' }, 'the retried PUT';
};

subtest 'bad fieldValidation or options are refused before a request' => sub {
    my $kube = make_kube();
    my $sent = 0;
    no warnings 'redefine';
    local *Net::Async::HTTP::do_request = sub { $sent++; Future->done(HTTP::Response->new(200)) };

    my $f = $kube->create(pod(), fieldValidation => 'Loud');
    ok $f->is_failed, 'create: failed Future';
    like(($f->failure)[0], qr/^Unknown fieldValidation value for create\(\) \(use: Strict, Warn, Ignore\)/,
        'create: names the allowed values');
    unlike(($f->failure)[0], qr/Loud/, '... not the value given');

    $f = $kube->create(pod(), bogus => 1);
    like(($f->failure)[0], qr/^Unknown argument 'bogus' to create\(\) \(allowed: fieldValidation\)/,
        'create: unknown option');
    $f = $kube->create(pod(), 'fieldValidation');
    like(($f->failure)[0], qr/^Invalid arguments to create\(\)/, 'create: odd list');

    eval { $kube->update(pod(), fieldValidation => 'Loud') };
    like $@, qr/^Unknown fieldValidation value for update\(\)/, 'update croaks, as for its other bad input';

    $f = $kube->patch('Pod', 'web', patch => {}, fieldValidation => 'Loud');
    like(($f->failure)[0], qr/^Unknown fieldValidation value for patch\(\)/, 'patch: failed Future');
    $f = $kube->patch_status(pod(), patch => {}, fieldValidation => 'Loud');
    like(($f->failure)[0], qr/^Unknown fieldValidation value for patch_status\(\)/, 'patch_status: failed Future');

    is $sent, 0, 'no request left the client';

    eval { make_kube(field_validation => 'Loud')->rest };
    like $@, qr/Unknown field_validation value/, 'a bad client default is refused';
    eval { make_kube(on_warning => 'x')->rest };
    like $@, qr/on_warning must be a code reference/, 'a non-code on_warning is refused';
};

done_testing;
