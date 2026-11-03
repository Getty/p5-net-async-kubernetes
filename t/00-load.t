#!/usr/bin/env perl
use strict;
use warnings;
use Test::More;

use_ok('Net::Async::Kubernetes');
use_ok('Net::Async::Kubernetes::Watcher');
use_ok('Net::Async::Kubernetes::Controller');
use_ok('Net::Async::Kubernetes::PortForwardSession');

done_testing;
