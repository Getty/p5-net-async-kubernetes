#!/usr/bin/env perl
# Example: Watch pods across all namespaces using Net::Async::Kubernetes
use strict;
use warnings;
$| = 1;
use IO::Async::Loop;
use Net::Async::Kubernetes;

my $loop = IO::Async::Loop->new;

my $kube = Net::Async::Kubernetes->new(
    kubeconfig => $ENV{KUBECONFIG} // "$ENV{HOME}/.kube/config",
);
$loop->add($kube);

# List pods first (async)
print "Listing pods in kube-system...\n";
my $pods = $kube->list('Pod', namespace => 'kube-system')->get;
printf "Found %d pods\n\n", scalar @{$pods->items};

# Watch pods across all namespaces
print "Watching all pods (Ctrl+C to stop)...\n\n";
printf "%-20s  %-10s  %s/%s\n", "TIMESTAMP", "TYPE", "NAMESPACE", "NAME";
print "-" x 70, "\n";

$kube->watcher('Pod',
    on_added => sub {
        my ($pod) = @_;
        printf "%-20s  %-10s  %s/%s\n",
            scalar localtime, 'ADDED',
            $pod->metadata->namespace // '-', $pod->metadata->name;
    },
    on_modified => sub {
        my ($pod) = @_;
        my $phase = eval { $pod->status->phase } // '?';
        printf "%-20s  %-10s  %s/%s  (%s)\n",
            scalar localtime, 'MODIFIED',
            $pod->metadata->namespace // '-', $pod->metadata->name, $phase;
    },
    on_deleted => sub {
        my ($pod) = @_;
        printf "%-20s  %-10s  %s/%s\n",
            scalar localtime, 'DELETED',
            $pod->metadata->namespace // '-', $pod->metadata->name;
    },
);

$loop->run;
