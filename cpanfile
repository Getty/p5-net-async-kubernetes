requires 'perl', '5.020';

requires 'Carp';
requires 'File::Temp';
requires 'Future', '0.47';
requires 'IO::Async', '0.80';
requires 'IO::Async::Loop';
requires 'IO::Async::Notifier';
requires 'IO::Async::SSL', '0.12';
requires 'IO::K8s', '1.112';
requires 'IO::Socket::SSL';
requires 'Kubernetes::REST', '1.111';
requires 'Net::Async::HTTP', '0.49';
requires 'Net::Async::WebSocket::Client', '0.14';
requires 'Protocol::WebSocket';
requires 'Scalar::Util';
requires 'URI';

on test => sub {
    requires 'File::Spec';
    requires 'FindBin';
    requires 'JSON::MaybeXS';
    requires 'MIME::Base64';
    requires 'Test::Exception';
    requires 'Test::More', '0.98';
};
