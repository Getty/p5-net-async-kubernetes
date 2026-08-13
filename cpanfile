requires 'perl', '5.020';

requires 'IO::Async', '0.80';
requires 'IO::Async::Loop';
requires 'IO::Async::Notifier';
requires 'Net::Async::HTTP', '0.49';
requires 'Net::Async::WebSocket::Client', '0.14';
requires 'Future', '0.47';
requires 'URI';
requires 'IO::Socket::SSL';
requires 'Protocol::WebSocket';

requires 'Kubernetes::REST', '1.107';
requires 'IO::K8s', '1.106';

on test => sub {
    requires 'Test::More', '0.98';
    requires 'Test::Exception';
};
