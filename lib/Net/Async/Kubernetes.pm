package Net::Async::Kubernetes;
# ABSTRACT: Async Kubernetes client for IO::Async
our $VERSION = '0.009';
use strict;
use warnings;
use parent 'IO::Async::Notifier';

use Carp qw(croak);
use Scalar::Util qw(blessed);
use IO::Socket::SSL;
use File::Temp ();
use Future;
use URI;
use Protocol::WebSocket::Request;
use Kubernetes::REST;
use Kubernetes::REST::Server;
use Kubernetes::REST::AuthToken;
use Kubernetes::REST::HTTPRequest;
use Kubernetes::REST::HTTPResponse;
use Kubernetes::REST::WatchEvent;
use Kubernetes::REST::LogEvent;
use Net::Async::Kubernetes::PortForwardSession;
use Net::Async::Kubernetes::Watcher;
use Net::Async::Kubernetes::Controller;

sub configure {
    my ($self, %params) = @_;

    if (exists $params{kubeconfig}) {
        $self->{kubeconfig} = delete $params{kubeconfig};
    }
    if (exists $params{context}) {
        $self->{context} = delete $params{context};
    }
    if (exists $params{server}) {
        my $val = delete $params{server};
        $self->{server} = (blessed($val) && $val->isa('Kubernetes::REST::Server'))
            ? $val
            : Kubernetes::REST::Server->new($val);
    }
    if (exists $params{credentials}) {
        my $val = delete $params{credentials};
        if (blessed($val) && $val->can('token')) {
            $self->{credentials} = $val;
        } elsif (ref($val) eq 'HASH') {
            $self->{credentials} = Kubernetes::REST::AuthToken->new($val);
        } else {
            $self->{credentials} = $val;
        }
    }
    if (exists $params{resource_map}) {
        $self->{resource_map} = delete $params{resource_map};
    }
    if (exists $params{resource_map_from_cluster}) {
        $self->{resource_map_from_cluster} = delete $params{resource_map_from_cluster};
    }

    # Resolve server/credentials via Kubeconfig (handles kubeconfig files
    # and in-cluster service account auto-detection)
    if (!$self->{server}) {
        require Kubernetes::REST::Kubeconfig;
        my $kc = Kubernetes::REST::Kubeconfig->new(
            ($self->{kubeconfig} ? (kubeconfig_path => $self->{kubeconfig}) : ()),
            ($self->{context}    ? (context_name    => $self->{context})    : ()),
        );
        if ($self->{kubeconfig}) {
            # Explicit kubeconfig — must resolve or croak
            my $api = $kc->api;
            $self->{server}      = $api->server;
            $self->{credentials} = $api->credentials;
        } elsif (my $api = eval { $kc->api }) {
            # Auto-detect: kubeconfig default path or in-cluster
            $self->{server}      = $api->server;
            $self->{credentials} = $api->credentials;
        }
    }

    $self->SUPER::configure(%params);
}

=method configure

Internal L<IO::Async::Notifier> configuration method. Handles initialization
of C<kubeconfig>, C<context>, C<server>, C<credentials>, C<resource_map>,
and C<resource_map_from_cluster> parameters.

If C<kubeconfig> is provided without explicit C<server> or C<credentials>,
they are loaded automatically via L<Kubernetes::REST::Kubeconfig>.

When running inside a Kubernetes pod (no C<kubeconfig> or C<server> set),
the service account token at
C</var/run/secrets/kubernetes.io/serviceaccount/token> is used
automatically for in-cluster authentication.

=cut

# Accessors
sub kubeconfig               { $_[0]->{kubeconfig} }

=attr kubeconfig

Path to kubeconfig file. If provided, C<server> and C<credentials> are
extracted automatically (via L<Kubernetes::REST::Kubeconfig>).

=cut

sub context                  { $_[0]->{context} }

=attr context

Kubernetes context to use from the kubeconfig. Defaults to current-context.

=cut

sub resource_map             { $_[0]->{resource_map} }

=attr resource_map

Optional. Custom resource map for short class names.

=cut

sub resource_map_from_cluster { $_[0]->{resource_map_from_cluster} // 0 }

=attr resource_map_from_cluster

Optional boolean. Load resource map from cluster OpenAPI spec.
Defaults to false.

=cut

sub server {
    my ($self) = @_;
    $self->{server} // croak "server or kubeconfig required";
}

=method server

Returns the L<Kubernetes::REST::Server> instance. Croaks if neither C<server>
nor C<kubeconfig> was provided during initialization.

=cut

sub credentials {
    my ($self) = @_;
    $self->{credentials} // croak "credentials or kubeconfig required";
}

=method credentials

Returns the credentials object (typically L<Kubernetes::REST::AuthToken>).
Croaks if neither C<credentials> nor C<kubeconfig> was provided during
initialization.

=cut

sub rest {
    my ($self) = @_;
    $self->{_rest} //= Kubernetes::REST->new(
        server      => $self->server,
        credentials => $self->credentials,
        resource_map_from_cluster => $self->resource_map_from_cluster,
        ($self->resource_map ? (resource_map => $self->resource_map) : ()),
    );
}

=method rest

    my $rest = $kube->rest;

Returns the underlying lazily-built L<Kubernetes::REST> instance used for
request building and response processing. Exposed for advanced use -- most
callers want the higher-level CRUD methods instead. The private C<_rest>
accessor used throughout the internals returns this same cached instance.

=cut

# Lazy internal Kubernetes::REST for request building + response processing
sub _rest { $_[0]->rest }

sub new_object {
    my ($self, @args) = @_;
    return $self->rest->new_object(@args);
}

=method new_object

    my $cm = $kube->new_object(ConfigMap =>
        metadata => { name => 'my-config' },
        data     => { key => 'value' },
    );

Builds a typed L<IO::K8s> object from a short class name (e.g. C<'Pod'>,
C<'ConfigMap'>) and either a hashref or a hash of attributes. Delegates to
L<Kubernetes::REST/new_object>. This is the public path for constructing the
objects passed to C<create> and C<update>.

=cut

# Lazy Net::Async::HTTP instance
sub _http {
    my ($self) = @_;
    unless ($self->{_http}) {
        require Net::Async::HTTP;
        $self->{_http} = Net::Async::HTTP->new(
            user_agent => 'Net::Async::Kubernetes Perl Client',
            max_connections_per_host => 0,
        );
    }
    return $self->{_http};
}

# SSL options derived from server config, passed to every HTTP request
sub _ssl_options {
    my ($self) = @_;
    return @{$self->{_ssl_options}} if $self->{_ssl_options};

    my $server = $self->server;
    my @opts;

    if ($server->ssl_verify_server) {
        push @opts, SSL_verify_mode => SSL_VERIFY_PEER;
    } else {
        push @opts, SSL_verify_mode => SSL_VERIFY_NONE;
    }

    push @opts, SSL_ca_file   => $server->ssl_ca_file   if $server->ssl_ca_file;
    push @opts, SSL_cert_file => $server->ssl_cert_file  if $server->ssl_cert_file;
    push @opts, SSL_key_file  => $server->ssl_key_file   if $server->ssl_key_file;
    my $ca_pem = $server->ssl_ca_pem;
    if (defined $ca_pem && length $ca_pem) {
        push @opts, SSL_ca_file => $self->_materialize_ssl_pem(ca => $ca_pem);
    }
    my $cert_pem = $server->ssl_cert_pem;
    if (defined $cert_pem && length $cert_pem) {
        push @opts, SSL_cert_file => $self->_materialize_ssl_pem(cert => $cert_pem);
    }
    my $key_pem = $server->ssl_key_pem;
    if (defined $key_pem && length $key_pem) {
        push @opts, SSL_key_file => $self->_materialize_ssl_pem(key => $key_pem);
    }

    $self->{_ssl_options} = \@opts;
    return @opts;
}

sub _materialize_ssl_pem {
    my ($self, $kind, $pem) = @_;

    my $fh = File::Temp->new(
        SUFFIX => ".$kind.pem",
        UNLINK => 1,
    );
    print {$fh} $pem;
    close $fh;

    push @{ $self->{_ssl_tempfiles} ||= [] }, $fh;
    return $fh->filename;
}

# IO::K8s::expand_class fails closed: an unknown, malformed or mismatched
# apiVersion yields undef instead of a bare-name guess. Passing that undef on to
# build_path dies with "argument is not a module name", naming neither the
# resource nor the reason, so every call site guards the result with this.
sub _unknown_resource_error {
    my ($self, $short_class) = @_;
    return sprintf(
        "unknown resource '%s': no IO::K8s class for this apiVersion/kind"
            . " (add it to resource_map if it is a CRD)",
        defined $short_class ? $short_class : '(undef)',
    );
}

sub expand_class {
    my ($self, @args) = @_;
    my $class = $self->_rest->expand_class(@args);
    croak $self->_unknown_resource_error($args[0]) unless defined $class;
    return $class;
}

=method expand_class

    my $full_class = $kube->expand_class('Pod');
    # Returns 'IO::K8s::Api::Core::V1::Pod'

Expands a short resource name (e.g., C<'Pod'>, C<'Deployment'>) to its full
IO::K8s class name. Delegates to L<Kubernetes::REST/expand_class>.

The name may also be qualified as C<'group/version/Kind'>, which resolves to
that exact API version instead of the historical default the bare Kind name
carries -- the two forms can and do point at different classes:

    $kube->expand_class('HorizontalPodAutoscaler');
    # 'IO::K8s::Api::Autoscaling::V2::HorizontalPodAutoscaler' (bare-name default)

    $kube->expand_class('autoscaling/v1/HorizontalPodAutoscaler');
    # 'IO::K8s::Api::Autoscaling::V1::HorizontalPodAutoscaler' (pinned to v1)

This qualified form is accepted anywhere a resource name is, including
C<list>, C<get>, and C<watcher>.

Croaks when the name cannot be resolved to an IO::K8s class. This is the
synchronous counterpart of the C<Future>-returning methods below, which report
the same condition as a failed L<Future>.

=cut

sub _add_to_loop {
    my ($self, $loop) = @_;
    $self->add_child($self->_http);
}

# ============================================================================
# ASYNC CRUD METHODS - return Futures
# ============================================================================

sub list {
    my ($self, $short_class, %args) = @_;

    my $rest = $self->_rest;
    my $class = $rest->expand_class($short_class)
        // return Future->fail($self->_unknown_resource_error($short_class));

    # Selectors are query parameters; build_path only knows path segments and
    # would drop them silently, turning a filtered list into a full one.
    my %params;
    for my $selector (qw(labelSelector fieldSelector)) {
        my $value = delete $args{$selector};
        $params{$selector} = $value if defined $value;
    }

    my $path = $rest->build_path($class, %args);
    my $req = $rest->prepare_request('GET', $path,
        %params ? (parameters => \%params) : (),
    );

    return $self->_do_request($req)->then(sub {
        my ($response) = @_;
        $rest->check_response($response, "list $short_class");
        return Future->done($rest->inflate_list($class, $response));
    });
}

=method list

    my $future = $kube->list('Pod', namespace => 'default');
    my $list = $future->get;
    my @pods = @{ $list->items };

    my $future = $kube->list('Pod', labelSelector => 'app=web');

List resources of the given type. Returns a L<Future> that resolves to an
L<IO::K8s::List>. Its C<items> accessor holds the ArrayRef of inflated
IO::K8s objects.

C<labelSelector> and C<fieldSelector> are sent as query parameters, so
filtering happens server-side rather than on the list that comes back.

Arguments:

=over 4

=item C<$short_class> - Resource type (e.g., C<'Pod'>, C<'Deployment'>), or a
qualified C<'group/version/Kind'> name to pin a specific API version -- see
L</expand_class>

=item C<%args> - Optional parameters: C<namespace>, C<labelSelector>,
C<fieldSelector>, etc.

=back

=cut

sub get {
    my ($self, $short_class, @rest_args) = @_;

    my $rest = $self->_rest;
    my %args;
    if (@rest_args == 1) {
        $args{name} = $rest_args[0];
    } elsif (@rest_args >= 2 && $rest_args[0] !~ /^(name|namespace)$/) {
        $args{name} = shift @rest_args;
        %args = (%args, @rest_args);
    } elsif (@rest_args % 2 == 0) {
        %args = @rest_args;
    } else {
        return Future->fail("Invalid arguments to get()");
    }

    my $class = $rest->expand_class($short_class)
        // return Future->fail($self->_unknown_resource_error($short_class));
    return Future->fail("name required for get") unless $args{name};

    my $path = $rest->build_path($class, %args);
    my $req = $rest->prepare_request('GET', $path);

    return $self->_do_request($req)->then(sub {
        my ($response) = @_;
        $rest->check_response($response, "get $short_class");
        return Future->done($rest->inflate_object($class, $response));
    });
}

=method get

    my $future = $kube->get('Pod', 'nginx', namespace => 'default');
    my $pod = $future->get;

Get a single resource by name. Returns a L<Future> that resolves to an
inflated IO::K8s object.

Arguments:

=over 4

=item C<$short_class> - Resource type (e.g., C<'Pod'>), or a qualified
C<'group/version/Kind'> name -- see L</expand_class>

=item C<$name> - Resource name (required)

=item C<%args> - Optional parameters (C<namespace>, etc.)

=back

=cut

sub create {
    my ($self, $object) = @_;

    my $rest = $self->_rest;
    my $class = ref($object);
    my $namespace = $object->can('metadata') && $object->metadata
        ? $object->metadata->namespace
        : undef;

    my $path = $rest->build_path($class, namespace => $namespace);
    my $req = $rest->prepare_request('POST', $path, body => $object->TO_JSON);

    return $self->_do_request($req)->then(sub {
        my ($response) = @_;
        $rest->check_response($response, "create " . ref($object));
        return Future->done($rest->inflate_object($class, $response));
    });
}

=method create

    my $future = $kube->create($pod_object);
    my $created = $future->get;

Create a resource from an IO::K8s object. Returns a L<Future> that resolves
to the created object with server-populated fields (C<resourceVersion>, etc.).

Arguments:

=over 4

=item C<$object> - IO::K8s object instance (e.g., C<IO::K8s::Api::Core::V1::Pod>)

=back

=cut

sub update {
    my ($self, $object) = @_;

    my $rest = $self->_rest;
    my $class = ref($object);
    my $metadata = $object->metadata or croak "object must have metadata";
    my $name = $metadata->name or croak "object must have metadata.name";
    my $namespace = $metadata->namespace;

    my $path = $rest->build_path($class, name => $name, namespace => $namespace);
    my $req = $rest->prepare_request('PUT', $path, body => $object->TO_JSON);

    return $self->_do_request($req)->then(sub {
        my ($response) = @_;
        $rest->check_response($response, "update " . ref($object));
        return Future->done($rest->inflate_object($class, $response));
    });
}

=method update

    my $future = $kube->update($modified_pod);
    my $updated = $future->get;

Update an existing resource. The object must have C<metadata.name> (and
C<metadata.namespace> if namespaced). Returns a L<Future> that resolves to
the updated object.

Arguments:

=over 4

=item C<$object> - Modified IO::K8s object with updated fields

=back

=cut

sub update_status {
    my ($self, $object) = @_;

    my $rest = $self->_rest;
    my $class = ref($object);
    my $metadata = $object->metadata or croak "object must have metadata";
    my $name = $metadata->name or croak "object must have metadata.name";
    my $namespace = $metadata->namespace;

    my $path = $rest->build_path($class,
        name        => $name,
        namespace   => $namespace,
        subresource => 'status',
    );
    my $req = $rest->prepare_request('PUT', $path, body => $object->TO_JSON);

    return $self->_do_request($req)->then(sub {
        my ($response) = @_;
        $rest->check_response($response, "update_status $class");
        return Future->done($rest->inflate_object($class, $response));
    });
}

=method update_status

    my $future = $kube->update_status($node);
    my $updated = $future->get;

Replace a resource's B<status> through the C</status> subresource. The whole
object is sent, as with L</update>, but the server only stores the C<status>
it carries and leaves C<spec> and C<metadata> untouched. Returns a L<Future>
that resolves to the updated object.

Needs a current C<resourceVersion> and fails with a 409 conflict if the
object changed on the server in the meantime. A missing C<metadata> or
C<metadata.name> croaks synchronously, as with L</update>. Prefer
L</patch_status> when you are setting individual status fields.

Arguments:

=over 4

=item C<$object> - IO::K8s object with C<metadata.name> (and C<namespace> if
namespaced) and the desired C<status>

=back

=cut

# Argument handling shared by patch() and patch_status(): the object form and
# both class+name forms, the required patch document and the patch type.
# Returns (undef, $class, $name, $namespace, $patch, $content_type), or just
# the failure message, which the caller turns into a failed Future. $label
# names the calling method in those messages; $default_type is the patch type
# used when the caller passes none.
sub _patch_args {
    my ($self, $label, $default_type, $class_or_object, @rest_args) = @_;

    my $rest = $self->_rest;
    my ($class, $name, $namespace, $patch, $patch_type);

    if (ref($class_or_object) && blessed($class_or_object)) {
        my $object = $class_or_object;
        $class = ref($object);
        my $metadata = $object->metadata or return "object must have metadata";
        $name = $metadata->name or return "object must have metadata.name";
        $namespace = $metadata->namespace;
        my %args = @rest_args;
        $patch = $args{patch} // return "$label requires 'patch' parameter";
        $patch_type = $args{type} // $default_type;
    } else {
        my %args;
        if (@rest_args >= 1 && !ref($rest_args[0]) && $rest_args[0] !~ /^(name|namespace|patch|type)$/) {
            $args{name} = shift @rest_args;
            %args = (%args, @rest_args);
        } elsif (@rest_args % 2 == 0) {
            %args = @rest_args;
        } else {
            return "Invalid arguments to $label()";
        }

        $class = $rest->expand_class($class_or_object)
            // return $self->_unknown_resource_error($class_or_object);
        $name = $args{name} or return "name required for $label";
        $namespace = $args{namespace};
        $patch = $args{patch} // return "$label requires 'patch' parameter";
        $patch_type = $args{type} // $default_type;
    }

    my %patch_types = (
        strategic => 'application/strategic-merge-patch+json',
        merge     => 'application/merge-patch+json',
        json      => 'application/json-patch+json',
    );
    my $content_type = $patch_types{$patch_type}
        // return "Unknown patch type '$patch_type'";

    return (undef, $class, $name, $namespace, $patch, $content_type);
}

sub patch {
    my ($self, $class_or_object, @rest_args) = @_;

    my $rest = $self->_rest;
    my ($error, $class, $name, $namespace, $patch, $content_type)
        = $self->_patch_args('patch', 'strategic', $class_or_object, @rest_args);
    return Future->fail($error) if defined $error;

    my $path = $rest->build_path($class, name => $name, namespace => $namespace);
    my $req = $rest->prepare_request('PATCH', $path,
        body => $patch, content_type => $content_type);

    return $self->_do_request($req)->then(sub {
        my ($response) = @_;
        $rest->check_response($response, "patch $class");
        return Future->done($rest->inflate_object($class, $response));
    });
}

=method patch

    # By class and name
    my $future = $kube->patch('Pod', 'nginx',
        namespace => 'default',
        patch     => { metadata => { labels => { env => 'prod' } } },
        type      => 'strategic',  # or 'merge', 'json'
    );

    # Or by object
    my $future = $kube->patch($pod_object,
        patch => { spec => { replicas => 3 } },
    );

Patch an existing resource. Returns a L<Future> that resolves to the patched
object.

Arguments:

=over 4

=item C<$class_or_object> - Resource class name or IO::K8s object

=item C<name> - Resource name (required unless passing object)

=item C<namespace> - Namespace (if namespaced)

=item C<patch> - HashRef of changes to apply (required)

=item C<type> - Patch type: C<'strategic'> (default), C<'merge'>, or C<'json'>

=back

=cut

sub patch_status {
    my ($self, $class_or_object, @rest_args) = @_;

    my $rest = $self->_rest;
    my ($error, $class, $name, $namespace, $patch, $content_type)
        = $self->_patch_args('patch_status', 'merge', $class_or_object, @rest_args);
    return Future->fail($error) if defined $error;

    my $path = $rest->build_path($class,
        name        => $name,
        namespace   => $namespace,
        subresource => 'status',
    );
    my $req = $rest->prepare_request('PATCH', $path,
        body => $patch, content_type => $content_type);

    return $self->_do_request($req)->then(sub {
        my ($response) = @_;
        $rest->check_response($response, "patch_status $class");
        return Future->done($rest->inflate_object($class, $response));
    });
}

=method patch_status

    # By class and name
    my $future = $kube->patch_status('OCPNode', 'cp-1',
        namespace => 'ocp',
        patch     => { status => { phase => 'Ready' } },
    );

    # Or by object
    my $future = $kube->patch_status($node,
        patch => { status => { phase => 'Ready' } },
    );

Partially update a resource's B<status> through the C</status> subresource.
Once a CustomResourceDefinition declares C<subresources: { status: {} }>, the
API server drops the C<status> stanza from every write to the main endpoint
and still answers 2xx -- so a C<status> written via L</patch> or L</update>
is silently discarded. This method writes to C</status> instead.

Takes the same call forms and arguments as L</patch> (object, or class plus
name in either the shorthand or fully-keyed form). The patch document is
sent unchanged and carries its own C<status> key.

The default patch type is C<merge>, not C<strategic> as in L</patch>: custom
resources reject strategic merge patch with a 415, and C<merge> works for
built-in kinds too. Pass C<type =E<gt> 'strategic'> explicitly to patch the
status of a built-in resource when you need array merge semantics.

Returns a L<Future> that resolves to the patched object; bad arguments, an
unknown patch C<type>, or a server error fail the Future, as with L</patch>.

Arguments:

=over 4

=item C<$class_or_object> - Resource class name or IO::K8s object

=item C<name> - Resource name (required unless passing object)

=item C<namespace> - Namespace (if namespaced)

=item C<patch> - HashRef with a C<status> key (or ArrayRef of operations when
C<type> is C<json>)

=item C<type> - Patch type: C<'merge'> (default), C<'strategic'>, or C<'json'>

=back

=cut

sub delete {
    my ($self, $class_or_object, @rest_args) = @_;

    my $rest = $self->_rest;
    my ($class, $name, $namespace);

    if (ref($class_or_object)) {
        my $object = $class_or_object;
        $class = ref($object);
        my $metadata = $object->metadata or return Future->fail("object must have metadata");
        $name = $metadata->name or return Future->fail("object must have metadata.name");
        $namespace = $metadata->namespace;
    } else {
        my %args;
        if (@rest_args == 1) {
            $args{name} = $rest_args[0];
        } elsif (@rest_args >= 2 && $rest_args[0] !~ /^(name|namespace)$/) {
            $args{name} = shift @rest_args;
            %args = (%args, @rest_args);
        } elsif (@rest_args % 2 == 0) {
            %args = @rest_args;
        } else {
            return Future->fail("Invalid arguments to delete()");
        }

        $class = $rest->expand_class($class_or_object)
            // return Future->fail($self->_unknown_resource_error($class_or_object));
        $name = $args{name} or return Future->fail("name required for delete");
        $namespace = $args{namespace};
    }

    my $path = $rest->build_path($class, name => $name, namespace => $namespace);
    my $req = $rest->prepare_request('DELETE', $path);

    return $self->_do_request($req)->then(sub {
        my ($response) = @_;
        $rest->check_response($response, "delete $class");
        return Future->done(1);
    });
}

=method delete

    # By class and name
    my $future = $kube->delete('Pod', 'nginx', namespace => 'default');
    $future->get;

    # Or by object
    my $future = $kube->delete($pod_object);
    $future->get;

Delete a resource. Returns a L<Future> that resolves to C<1> on success.

Arguments:

=over 4

=item C<$class_or_object> - Resource class name or IO::K8s object

=item C<$name> - Resource name (required unless passing object)

=item C<%args> - Optional parameters (C<namespace>, etc.)

=back

=cut

# One request through the Kubernetes::REST seam, resolving with the unchecked
# Kubernetes::REST::HTTPResponse -- for ensure(), which branches on the status
# code (404 absent, 409 conflict). check_response would fold that code into an
# error string it could only be read back out of with a regex.
sub _request_unchecked {
    my ($self, $method, $path, %opts) = @_;
    return $self->_do_request($self->_rest->prepare_request($method, $path, %opts));
}

# Shared hashref handling for ensure() and ensure_only(): turns a manifest into
# a typed object. A manifest's apiVersion is authoritative - with one, the
# class is resolved as that exact group/version/Kind, and an apiVersion no
# class serves croaks instead of falling back to the version the bare Kind
# happens to map to (HorizontalPodAutoscaler alone means autoscaling/v2, a
# different endpoint and schema than an autoscaling/v1 manifest). Without an
# apiVersion the bare Kind resolves as it always did. $label only appears in
# croak messages.
sub _manifest_to_object {
    my ($self, $label, $manifest) = @_;
    my $kind = $manifest->{kind} or croak "$label: hashref must have 'kind'";
    my $api_version = $manifest->{apiVersion};
    my $rest = $self->_rest;

    return $rest->k8s->struct_to_object($self->expand_class($kind), $manifest)
        unless defined $api_version && length $api_version;

    my $class = $rest->expand_class($kind, $api_version)
        // croak "$label: no IO::K8s class for apiVersion '$api_version', kind '$kind'"
            . " (add it to resource_map if it is a CRD)";
    return $rest->k8s->struct_to_object($class, $manifest);
}

sub ensure {
    my ($self, $object) = @_;

    my $rest = $self->_rest;
    $object = $self->_manifest_to_object('ensure', $object) if ref($object) eq 'HASH';
    croak "ensure requires an IO::K8s object or hashref" unless blessed($object);

    my $class = ref($object);
    (my $kind = $class) =~ s/.*:://;
    my $metadata = $object->metadata or croak "object must have metadata";
    my $name = $metadata->name or croak "object must have metadata.name";
    my $namespace = $metadata->namespace;
    my $path = $rest->build_path($class, name => $name, namespace => $namespace);

    # GET the object as the server has it now; $context names the step.
    my $fetch = sub {
        my ($context) = @_;
        return $self->_request_unchecked('GET', $path)->then(sub {
            my ($response) = @_;
            $rest->check_response($response, "$context $kind/$name");
            return Future->done($rest->inflate_object($class, $response));
        });
    };

    # PUT at the server's resourceVersion. A 409 means the object changed
    # between GET and PUT: fetch it once more and retry once, no further.
    my $replace = sub {
        my ($existing) = @_;
        $metadata->resourceVersion($existing->metadata->resourceVersion);
        return $self->_request_unchecked('PUT', $path, body => $object->TO_JSON)->then(sub {
            my ($response) = @_;
            if ($response->status == 409) {
                return $fetch->('ensure refetch')->then(sub {
                    my ($current) = @_;
                    $metadata->resourceVersion($current->metadata->resourceVersion);
                    return $self->update($object);
                });
            }
            $rest->check_response($response, "update $class");
            return Future->done($rest->inflate_object($class, $response));
        });
    };

    # POST. A 409 means it was created by someone else after our GET: take
    # that one (PVC) or update it at its resourceVersion, without a retry.
    my $create = sub {
        my $collection = $rest->build_path($class, namespace => $namespace);
        return $self->_request_unchecked('POST', $collection, body => $object->TO_JSON)->then(sub {
            my ($response) = @_;
            if ($response->status == 409) {
                return $fetch->('ensure post-409 get')->then(sub {
                    my ($current) = @_;
                    return Future->done($current) if $kind eq 'PersistentVolumeClaim';
                    $metadata->resourceVersion($current->metadata->resourceVersion);
                    return $self->update($object);
                });
            }
            $rest->check_response($response, "create $class");
            return Future->done($rest->inflate_object($class, $response));
        });
    };

    return $self->_request_unchecked('GET', $path)->then(sub {
        my ($response) = @_;
        return $create->() if $response->status == 404;
        $rest->check_response($response, "ensure get $kind/$name");
        my $existing = $rest->inflate_object($class, $response);

        # An existing claim is never rewritten.
        return Future->done($existing) if $kind eq 'PersistentVolumeClaim';

        # A Job's pod template is immutable: a running or succeeded Job stays,
        # any other is replaced. A failing delete does not stop the create.
        if ($kind eq 'Job') {
            my $status = $existing->status;
            return Future->done($existing)
                if $status && ($status->succeeded || $status->active);
            return Future->call(sub { $self->delete($existing) })
                ->else(sub { Future->done })
                ->then(sub { $self->create($object) });
        }

        return $replace->($existing);
    });
}

=method ensure

    my $future = $kube->ensure($pod);
    my $obj = $future->get;

    # or from a plain hashref (treated as a Kubernetes manifest):
    my $future = $kube->ensure({
        apiVersion => 'v1',
        kind       => 'Secret',
        metadata   => { name => 'foo', namespace => 'default' },
        stringData => { password => 'hunter2' },
    });

Idempotent create-or-update. GETs the object by kind/name/namespace: if it is
missing, creates it; if it exists, updates it at the server's
C<resourceVersion>, which this method writes back into the object passed in.
Returns a L<Future> that resolves to the resulting IO::K8s object.

Accepts a typed IO::K8s object or a plain hashref; a hashref must carry a
C<kind> field and uses manifest-style camelCase keys (C<stringData>, not
C<string_data>).

A hashref's C<apiVersion>, when present, selects the class: an
C<autoscaling/v1> HorizontalPodAutoscaler stays C<autoscaling/v1> and goes to
that endpoint, although the bare Kind resolves to C<autoscaling/v2>. A hashref
without C<apiVersion> (or with an empty one) resolves by its Kind alone, as
L</expand_class> does.

Handles the create/update race: a 409 on update (something else changed the
object between GET and PUT) refetches once and retries the update; a 409 on
create (something else created it between GET and POST) refetches and
updates instead.

Two kinds get special handling because their spec is immutable after
creation: an existing C<PersistentVolumeClaim> is left unchanged, and an
existing C<Job> is left unchanged while it is active or has succeeded, and
deleted and recreated otherwise.

Errors that are known before any request is made -- a hashref without
C<kind>, a value that is neither an object nor a hashref, an object missing
C<metadata>/C<metadata.name>, an unknown C<kind>, or an C<apiVersion> that
resolves to no known class (the message names both the Kind and the
C<apiVersion>) -- croak synchronously, as with L</update>. Anything that
goes wrong during the request flow itself fails the Future instead.

Arguments:

=over 4

=item C<$object> - IO::K8s object or hashref manifest (must have C<kind> if a
hashref)

=back

=cut

sub ensure_all {
    my ($self, @objects) = @_;

    # Strictly one after another: object N+1 is only started once object N
    # is done, so a later object may rely on an earlier one (a Namespace and
    # what lives in it). Any error, a croak from ensure() included, fails
    # the chain and nothing after it is started.
    my @results;
    my $f = Future->done;
    for my $object (@objects) {
        $f = $f->then(sub {
            return $self->ensure($object);
        })->then(sub {
            push @results, @_;
            return Future->done;
        });
    }

    return $f->then(sub { Future->done(@results) });
}

=method ensure_all

    my $future = $kube->ensure_all(@objects);
    my @results = $future->get;

Batch form of L</ensure>. Applies each object in order, one at a time --
object N+1 is only started once object N has resolved, so a later object may
depend on an earlier one (a Namespace before what lives in it). Returns a
L<Future> that resolves to the list of results in input order.

If any object fails -- including a croak from L</ensure>, which becomes a
failure here -- the Future fails and no later object is started.
C<ensure_all> itself never croaks synchronously.

Arguments:

=over 4

=item C<@objects> - IO::K8s objects or hashref manifests, as accepted by
L</ensure>

=back

=cut

sub ensure_only {
    my ($self, %args) = @_;

    my $label      = $args{label} or croak "ensure_only requires 'label'";
    my @objects    = @{ $args{objects} || [] };
    my @kinds      = @{ $args{kinds} || [] };
    my @namespaces = @{ $args{namespaces} || [undef] };

    # Every hashref is resolved before the first request, so one that cannot
    # be (no kind, an apiVersion no class serves) stops the whole call.
    for my $object (@objects) {
        $object = $self->_manifest_to_object('ensure_only', $object)
            if ref($object) eq 'HASH';
    }

    # (Kind, namespace, name), taken from the object on both sides - never
    # from the kinds entry, which may be qualified ('autoscaling/v1/...') and
    # would then match nothing, deleting the objects just applied. The Kind is
    # the object's own kind(): class-derived for a typed object, instance data
    # for IO::K8s::Unstructured, whose class name says nothing about its Kind.
    # No version in the key: the same resource listed through another
    # version's class is still the same resource.
    my $key_of = sub {
        my ($object) = @_;
        my $kind = $object->can('kind') ? $object->kind : undef;
        ($kind = ref $object) =~ s/.*::// unless defined $kind;
        my $metadata = $object->metadata;
        return join("\0", $kind, $metadata->namespace // '', $metadata->name);
    };

    return $self->ensure_all(@objects)->then(sub {
        my @applied = @_;
        my %expected = map { $key_of->($_) => 1 } @objects;

        # One Kind x namespace after another. A list that fails is skipped,
        # a delete that fails is ignored -- as in the synchronous client.
        my $f = Future->done;
        for my $kind (@kinds) {
            for my $namespace (@namespaces) {
                $f = $f->then(sub {
                    return Future->call(sub {
                        $self->list($kind,
                            labelSelector => $label,
                            (defined $namespace ? (namespace => $namespace) : ()),
                        );
                    })->else(sub {
                        return Future->done(undef);
                    })->then(sub {
                        my ($list) = @_;
                        my $deletes = Future->done;
                        return $deletes unless $list;
                        for my $item (@{ $list->items }) {
                            next if $expected{ $key_of->($item) };
                            $deletes = $deletes->then(sub {
                                return Future->call(sub { $self->delete($item) })
                                    ->else(sub { Future->done });
                            });
                        }
                        return $deletes;
                    });
                });
            }
        }

        return $f->then(sub { Future->done(@applied) });
    });
}

=method ensure_only

    my $future = $kube->ensure_only(
        label      => 'app.kubernetes.io/component=queen',
        objects    => \@objects,
        kinds      => [qw(Role RoleBinding ClusterRoleBinding)],
        namespaces => ['default', 'kube-system', undef],
    );
    my @applied = $future->get;

Like L</ensure_all>, but also deletes anything matching the label selector in
the given kinds and namespaces that is not present in C<objects>. Use this
for resources where stale objects must not survive (e.g. RBAC). Croaks
synchronously if C<label> is missing.

Hashrefs in C<objects> are resolved as in L</ensure>, all of them before the
first request: one without C<kind> or with an C<apiVersion> no class serves
croaks, and nothing is applied or deleted.

Applies C<objects> via L</ensure_all>, then for each kind in C<kinds> and
each namespace in C<namespaces>, lists resources of that kind carrying the
label and deletes any that do not match one of the just-applied objects by
Kind, namespace and name. The Kind is each object's own C<kind>, so a
qualified C<'group/version/Kind'> entry in C<kinds> still recognises the
objects it lists rather than deleting them. The version is not compared: an
object applied as C<autoscaling/v1> is kept when the listing goes through
C<autoscaling/v2>. A C<namespaces> entry of C<undef> scans cluster-scoped
resources; if C<namespaces> is omitted, only cluster-scoped resources are
scanned. A list or delete request that fails is skipped or ignored, as in the
synchronous client.

Returns a L<Future> that resolves to the list of applied objects (from
L</ensure_all>).

Arguments:

=over 4

=item C<label> - Label selector matching stale objects to delete (required)

=item C<objects> - ArrayRef of objects/hashrefs to apply, as for L</ensure_all>

=item C<kinds> - ArrayRef of resource kinds to scan for stale objects

=item C<namespaces> - ArrayRef of namespaces to scan, C<undef> for
cluster-scoped; defaults to cluster-scoped only

=back

=cut

sub log {
    my ($self, $short_class, @rest_args) = @_;

    my $rest = $self->_rest;
    my %args;

    # Support: log('Pod', 'name', ...) and log('Pod', name => 'name', ...)
    if (@rest_args >= 1
        && !ref($rest_args[0])
        && $rest_args[0] !~ /^(name|namespace|container|follow|tailLines|sinceSeconds|sinceTime|timestamps|previous|limitBytes|on_line)$/
    ) {
        $args{name} = shift @rest_args;
        return Future->fail("Invalid arguments to log()") if @rest_args % 2;
        %args = (%args, @rest_args);
    } elsif (@rest_args % 2 == 0) {
        %args = @rest_args;
    } else {
        return Future->fail("Invalid arguments to log()");
    }

    return Future->fail("name required for log") unless $args{name};

    my $on_line       = delete $args{on_line};
    my $container     = delete $args{container};
    my $follow        = delete $args{follow};
    my $tail_lines    = delete $args{tailLines};
    my $since_seconds = delete $args{sinceSeconds};
    my $since_time    = delete $args{sinceTime};
    my $timestamps    = delete $args{timestamps};
    my $previous      = delete $args{previous};
    my $limit_bytes   = delete $args{limitBytes};

    my $class = $rest->expand_class($short_class)
        // return Future->fail($self->_unknown_resource_error($short_class));
    my $path = $rest->build_path($class, %args) . '/log';

    my %params;
    $params{container}    = $container     if defined $container;
    $params{follow}       = 'true'         if $follow;
    $params{tailLines}    = $tail_lines    if defined $tail_lines;
    $params{sinceSeconds} = $since_seconds if defined $since_seconds;
    $params{sinceTime}    = $since_time    if defined $since_time;
    $params{timestamps}   = 'true'         if $timestamps;
    $params{previous}     = 'true'         if $previous;
    $params{limitBytes}   = $limit_bytes   if defined $limit_bytes;

    if ($on_line) {
        my $req = $rest->prepare_request('GET', $path, parameters => \%params);
        my $buffer = '';

        return $self->_do_streaming_request($req, sub {
            my ($chunk) = @_;
            for my $event ($rest->process_log_chunk(\$buffer, $chunk)) {
                $on_line->($event);
            }
        })->then(sub {
            my ($response) = @_;
            $rest->check_response($response, "log $short_class");
            if (length $buffer) {
                $on_line->(Kubernetes::REST::LogEvent->new(line => $buffer));
            }
            return Future->done(undef);
        });
    }

    my $req = $rest->prepare_request('GET', $path,
        %params ? (parameters => \%params) : (),
    );
    return $self->_do_request($req)->then(sub {
        my ($response) = @_;
        $rest->check_response($response, "log $short_class");
        return Future->done($response->content);
    });
}

=method log

    # One-shot mode (Future resolves to full text)
    my $text = $kube->log('Pod', 'my-pod',
        namespace => 'default',
        tailLines => 100,
    )->get;

    # Streaming mode (Future resolves when stream ends)
    $kube->log('Pod', 'my-pod',
        namespace => 'default',
        follow    => 1,
        on_line   => sub {
            my ($event) = @_;  # Kubernetes::REST::LogEvent
            say $event->line;
        },
    )->get;

Retrieve logs from a pod.

Without C<on_line>, returns a L<Future> that resolves to the full log text.

With C<on_line>, opens a streaming request and invokes the callback once per
line with L<Kubernetes::REST::LogEvent> objects. The returned L<Future>
resolves when the stream ends.

=cut

sub port_forward {
    my ($self, $short_class, @rest_args) = @_;

    my $rest = $self->_rest;
    my %args;

    # Support: port_forward('Pod', 'name', ...) and port_forward('Pod', name => 'name', ...)
    if (@rest_args >= 1
        && !ref($rest_args[0])
        && $rest_args[0] !~ /^(name|namespace|ports|subprotocol|on_open|on_frame|on_close|on_error)$/
    ) {
        $args{name} = shift @rest_args;
        return Future->fail("Invalid arguments to port_forward()") if @rest_args % 2;
        %args = (%args, @rest_args);
    } elsif (@rest_args % 2 == 0) {
        %args = @rest_args;
    } else {
        return Future->fail("Invalid arguments to port_forward()");
    }

    return Future->fail("name required for port_forward") unless $args{name};

    my $ports = delete $args{ports};
    return Future->fail("ports required for port_forward") unless defined $ports;
    $ports = [$ports] unless ref($ports) eq 'ARRAY';
    return Future->fail("ports required for port_forward") unless @$ports;
    for my $p (@$ports) {
        return Future->fail("invalid port '$p' for port_forward")
            unless defined($p) && $p =~ /^\d+$/ && $p > 0 && $p <= 65535;
    }

    my $subprotocol = delete $args{subprotocol} // 'v4.channel.k8s.io';
    my $on_open  = delete $args{on_open};
    my $on_frame = delete $args{on_frame};
    my $on_close = delete $args{on_close};
    my $on_error = delete $args{on_error};

    my $class = $rest->expand_class($short_class)
        // return Future->fail($self->_unknown_resource_error($short_class));
    my $path = $rest->build_path($class, %args) . '/portforward';

    # Keep compatibility with Kubernetes::REST >= 1.100 by expanding repeated
    # ports query params here instead of relying on arrayref parameter support.
    my $query = join('&', map { "ports=$_" } @$ports);
    my $path_with_query = $query ? "$path?$query" : $path;

    my $req = $rest->prepare_request('GET', $path_with_query,
        headers    => {
            Accept                   => '*/*',
            Connection               => 'Upgrade',
            Upgrade                  => 'websocket',
            'Sec-WebSocket-Protocol' => $subprotocol,
        },
    );

    return $self->_do_duplex_request($req,
        caller   => 'port_forward',
        on_open  => $on_open,
        on_frame => $on_frame,
        on_close => $on_close,
        on_error => $on_error,
    );
}

=method port_forward

    my $f = $kube->port_forward('Pod', 'my-pod',
        namespace => 'default',
        ports     => [8080, 8443],
        on_frame  => sub { my ($channel, $payload) = @_; ... },
    );
    my $session = $f->get;

Create an async pod port-forward session request.

Returns a L<Future> that resolves to the duplex session object returned by the
transport backend. The default transport returns a
L<Net::Async::Kubernetes::PortForwardSession> object.

The session helper supports C<write_channel>, C<write_stdin>, C<resize>, and
C<close>.

C<on_open> receives the created session object.

C<on_frame> receives C<($channel, $payload)> where the first byte of each
binary websocket frame is decoded as Kubernetes channel id.

=cut

sub exec {
    my ($self, $short_class, @rest_args) = @_;

    my $rest = $self->_rest;
    my %args;

    # Support: exec('Pod', 'name', ...) and exec('Pod', name => 'name', ...)
    if (@rest_args >= 1
        && !ref($rest_args[0])
        && $rest_args[0] !~ /^(name|namespace|command|container|stdin|stdout|stderr|tty|subprotocol|on_open|on_frame|on_close|on_error)$/
    ) {
        $args{name} = shift @rest_args;
        return Future->fail("Invalid arguments to exec()") if @rest_args % 2;
        %args = (%args, @rest_args);
    } elsif (@rest_args % 2 == 0) {
        %args = @rest_args;
    } else {
        return Future->fail("Invalid arguments to exec()");
    }

    return Future->fail("name required for exec") unless $args{name};

    my $command = delete $args{command};
    return Future->fail("command required for exec") unless defined $command;
    $command = [$command] unless ref($command) eq 'ARRAY';
    return Future->fail("command required for exec") unless @$command;
    for my $part (@$command) {
        return Future->fail("invalid command element for exec")
            unless defined($part) && !ref($part) && length $part;
    }

    my $container = delete $args{container};
    my $stdin  = delete($args{stdin})  ? 1 : 0;
    my $stdout = exists($args{stdout}) ? (delete($args{stdout}) ? 1 : 0) : 1;
    my $stderr = exists($args{stderr}) ? (delete($args{stderr}) ? 1 : 0) : 1;
    my $tty    = delete($args{tty})    ? 1 : 0;

    my $subprotocol = delete $args{subprotocol} // 'v4.channel.k8s.io';
    my $on_open  = delete $args{on_open};
    my $on_frame = delete $args{on_frame};
    my $on_close = delete $args{on_close};
    my $on_error = delete $args{on_error};

    my $class = $rest->expand_class($short_class)
        // return Future->fail($self->_unknown_resource_error($short_class));
    my $path = $rest->build_path($class, %args) . '/exec';

    my %params = (
        command => $command,
        stdin   => $stdin  ? 'true' : 'false',
        stdout  => $stdout ? 'true' : 'false',
        stderr  => $stderr ? 'true' : 'false',
        tty     => $tty    ? 'true' : 'false',
    );
    $params{container} = $container if defined $container;

    my $req = $rest->prepare_request('GET', $path,
        parameters => \%params,
        headers    => {
            Accept                   => '*/*',
            Connection               => 'Upgrade',
            Upgrade                  => 'websocket',
            'Sec-WebSocket-Protocol' => $subprotocol,
        },
    );

    return $self->_do_duplex_request($req,
        caller   => 'exec',
        on_open  => $on_open,
        on_frame => $on_frame,
        on_close => $on_close,
        on_error => $on_error,
    );
}

=method exec

    my $f = $kube->exec('Pod', 'my-pod',
        namespace => 'default',
        command   => ['sh', '-c', 'id'],
        on_frame  => sub { my ($channel, $payload) = @_; ... },
    );
    my $session = $f->get;

Create an async pod exec session request.

Returns a L<Future> that resolves to the duplex session object returned by the
transport backend. The default transport returns a
L<Net::Async::Kubernetes::PortForwardSession> object.

The session helper supports C<write_channel>, C<write_stdin>, C<resize>, and
C<close>.

C<on_open> receives the created session object.

C<on_frame> receives C<($channel, $payload)> where the first byte of each
binary websocket frame is decoded as Kubernetes channel id.

=cut

sub attach {
    my ($self, $short_class, @rest_args) = @_;

    my $rest = $self->_rest;
    my %args;

    # Support: attach('Pod', 'name', ...) and attach('Pod', name => 'name', ...)
    if (@rest_args >= 1
        && !ref($rest_args[0])
        && $rest_args[0] !~ /^(name|namespace|container|stdin|stdout|stderr|tty|subprotocol|on_open|on_frame|on_close|on_error)$/
    ) {
        $args{name} = shift @rest_args;
        return Future->fail("Invalid arguments to attach()") if @rest_args % 2;
        %args = (%args, @rest_args);
    } elsif (@rest_args % 2 == 0) {
        %args = @rest_args;
    } else {
        return Future->fail("Invalid arguments to attach()");
    }

    return Future->fail("name required for attach") unless $args{name};

    my $container = delete $args{container};
    my $stdin  = delete($args{stdin})  ? 1 : 0;
    my $stdout = exists($args{stdout}) ? (delete($args{stdout}) ? 1 : 0) : 1;
    my $stderr = exists($args{stderr}) ? (delete($args{stderr}) ? 1 : 0) : 1;
    my $tty    = delete($args{tty})    ? 1 : 0;

    my $subprotocol = delete $args{subprotocol} // 'v4.channel.k8s.io';
    my $on_open  = delete $args{on_open};
    my $on_frame = delete $args{on_frame};
    my $on_close = delete $args{on_close};
    my $on_error = delete $args{on_error};

    my $class = $rest->expand_class($short_class)
        // return Future->fail($self->_unknown_resource_error($short_class));
    my $path = $rest->build_path($class, %args) . '/attach';

    my %params = (
        stdin   => $stdin  ? 'true' : 'false',
        stdout  => $stdout ? 'true' : 'false',
        stderr  => $stderr ? 'true' : 'false',
        tty     => $tty    ? 'true' : 'false',
    );
    $params{container} = $container if defined $container;

    my $req = $rest->prepare_request('GET', $path,
        parameters => \%params,
        headers    => {
            Accept                   => '*/*',
            Connection               => 'Upgrade',
            Upgrade                  => 'websocket',
            'Sec-WebSocket-Protocol' => $subprotocol,
        },
    );

    return $self->_do_duplex_request($req,
        caller   => 'attach',
        on_open  => $on_open,
        on_frame => $on_frame,
        on_close => $on_close,
        on_error => $on_error,
    );
}

=method attach

    my $f = $kube->attach('Pod', 'my-pod',
        namespace => 'default',
        container => 'app',
        stdin     => 1,
        stdout    => 1,
        stderr    => 1,
        tty       => 0,
        on_frame  => sub { my ($channel, $payload) = @_; ... },
    );
    my $session = $f->get;

Create an async pod attach session request.

Returns a L<Future> that resolves to the duplex session object returned by the
transport backend. The default transport returns a
L<Net::Async::Kubernetes::PortForwardSession> object.

The session helper supports C<write_channel>, C<write_stdin>, C<resize>, and
C<close>.

C<on_open> receives the created session object.

C<on_frame> receives C<($channel, $payload)> where the first byte of each
binary websocket frame is decoded as Kubernetes channel id.

=cut

sub cp_to_pod {
    my ($self, $short_class, @rest_args) = @_;

    my $loop = eval { $self->loop };
    return Future->fail("cp_to_pod requires Net::Async::Kubernetes to be added to an IO::Async::Loop")
        unless $loop;

    my %args;
    if (@rest_args >= 1
        && !ref($rest_args[0])
        && $rest_args[0] !~ /^(name|namespace|container|local|remote|chunk_size)$/
    ) {
        $args{name} = shift @rest_args;
        return Future->fail("Invalid arguments to cp_to_pod()") if @rest_args % 2;
        %args = (%args, @rest_args);
    } elsif (@rest_args % 2 == 0) {
        %args = @rest_args;
    } else {
        return Future->fail("Invalid arguments to cp_to_pod()");
    }

    return Future->fail("name required for cp_to_pod") unless $args{name};

    my $local = delete $args{local};
    my $remote = delete $args{remote};
    return Future->fail("local path required for cp_to_pod") unless defined $local && length $local;
    return Future->fail("remote path required for cp_to_pod") unless defined $remote && length $remote;
    return Future->fail("local file '$local' does not exist for cp_to_pod") unless -e $local;
    return Future->fail("local path '$local' is not a file for cp_to_pod") unless -f $local;

    my $chunk_size = delete($args{chunk_size}) // 64 * 1024;
    return Future->fail("invalid chunk_size '$chunk_size' for cp_to_pod")
        unless defined($chunk_size) && $chunk_size =~ /^\d+$/ && $chunk_size > 0;

    open my $fh, '<:raw', $local
        or return Future->fail("cannot read local file '$local' for cp_to_pod: $!");
    local $/ = undef;
    my $bytes = <$fh>;
    close $fh;
    $bytes = '' unless defined $bytes;

    my $size = length($bytes);
    my $stderr = '';
    my $status_payload = '';
    my $done = $loop->new_future;

    return $self->exec($short_class, $args{name},
        namespace => $args{namespace},
        (defined($args{container}) ? (container => $args{container}) : ()),
        command   => ['sh', '-c', 'head -c "$1" > "$2"', 'k8s-cp', $size, $remote],
        stdin     => 1,
        stdout    => 0,
        stderr    => 1,
        tty       => 0,
        on_frame  => sub {
            my ($channel, $payload) = @_;
            $stderr .= $payload if $channel == 2;
            $status_payload .= $payload if $channel == 3;
        },
        on_close  => sub {
            return if $done->is_ready;
            if ($status_payload =~ /"status"\s*:\s*"Failure"/i) {
                $done->fail("cp_to_pod failed: $status_payload");
            } else {
                $done->done({
                    local   => $local,
                    remote  => $remote,
                    bytes   => $size,
                    stderr  => $stderr,
                    status  => $status_payload,
                });
            }
        },
        on_error  => sub {
            my ($err) = @_;
            $done->fail("cp_to_pod transport error: $err") unless $done->is_ready;
        },
    )->then(sub {
        my ($session) = @_;
        return $self->_send_stdin_chunks($session, $bytes, $chunk_size)
            ->then(sub { return $done; });
    });
}

=method cp_to_pod

    my $f = $kube->cp_to_pod('Pod', 'my-pod',
        namespace => 'default',
        container => 'app',
        local     => '/tmp/local.txt',
        remote    => '/tmp/remote.txt',
    );
    my $result = $f->get;

Copy a single local file into a pod. Reads the entire local file into memory,
then runs C<sh -c 'head -c "$1" > "$2"'> inside the pod via C<exec()> and
streams the bytes over stdin.

This is a single-file copy, not a tar-based transfer: there is no recursive
directory copy, and the whole file is held in memory, so it is not suitable
for very large files.

Returns a L<Future> resolving to a hashref containing C<local>, C<remote>,
C<bytes>, C<stderr>, and C<status>.

=cut

sub cp_from_pod {
    my ($self, $short_class, @rest_args) = @_;

    my $loop = eval { $self->loop };
    return Future->fail("cp_from_pod requires Net::Async::Kubernetes to be added to an IO::Async::Loop")
        unless $loop;

    my %args;
    if (@rest_args >= 1
        && !ref($rest_args[0])
        && $rest_args[0] !~ /^(name|namespace|container|local|remote)$/
    ) {
        $args{name} = shift @rest_args;
        return Future->fail("Invalid arguments to cp_from_pod()") if @rest_args % 2;
        %args = (%args, @rest_args);
    } elsif (@rest_args % 2 == 0) {
        %args = @rest_args;
    } else {
        return Future->fail("Invalid arguments to cp_from_pod()");
    }

    return Future->fail("name required for cp_from_pod") unless $args{name};

    my $local = delete $args{local};
    my $remote = delete $args{remote};
    return Future->fail("local path required for cp_from_pod") unless defined $local && length $local;
    return Future->fail("remote path required for cp_from_pod") unless defined $remote && length $remote;
    return Future->fail("local path '$local' is a directory for cp_from_pod") if -d $local;

    my $stdout = '';
    my $stderr = '';
    my $status_payload = '';
    my $done = $loop->new_future;

    return $self->exec($short_class, $args{name},
        namespace => $args{namespace},
        (defined($args{container}) ? (container => $args{container}) : ()),
        command   => ['cat', $remote],
        stdin     => 0,
        stdout    => 1,
        stderr    => 1,
        tty       => 0,
        on_frame  => sub {
            my ($channel, $payload) = @_;
            $stdout .= $payload if $channel == 1;
            $stderr .= $payload if $channel == 2;
            $status_payload .= $payload if $channel == 3;
        },
        on_close  => sub {
            return if $done->is_ready;
            if ($status_payload =~ /"status"\s*:\s*"Failure"/i) {
                $done->fail("cp_from_pod failed: $status_payload");
                return;
            }

            open my $fh, '>:raw', $local
                or do {
                    $done->fail("cannot write local file '$local' for cp_from_pod: $!");
                    return;
                };
            print {$fh} $stdout;
            close $fh;

            $done->done({
                local   => $local,
                remote  => $remote,
                bytes   => length($stdout),
                stderr  => $stderr,
                status  => $status_payload,
            });
        },
        on_error  => sub {
            my ($err) = @_;
            $done->fail("cp_from_pod transport error: $err") unless $done->is_ready;
        },
    )->then(sub { return $done; });
}

=method cp_from_pod

    my $f = $kube->cp_from_pod('Pod', 'my-pod',
        namespace => 'default',
        container => 'app',
        remote    => '/tmp/remote.txt',
        local     => '/tmp/local.txt',
    );
    my $result = $f->get;

Copy a single file out of a pod. Runs C<cat $remote> inside the pod via
C<exec()>, buffers the entire stdout stream in memory, then writes it to the
local file.

This is a single-file copy, not a tar-based transfer: there is no recursive
directory copy, and the whole file is held in memory, so it is not suitable
for very large files.

Returns a L<Future> resolving to a hashref containing C<local>, C<remote>,
C<bytes>, C<stderr>, and C<status>.

=cut

sub _send_stdin_chunks {
    my ($self, $session, $bytes, $chunk_size) = @_;

    my $f = Future->done;
    my $len = length($bytes // '');
    for (my $off = 0; $off < $len; $off += $chunk_size) {
        my $chunk = substr($bytes, $off, $chunk_size);
        $f = $f->then(sub {
            return $session->write_stdin($chunk);
        });
    }

    return $f;
}

# ============================================================================
# WATCHER FACTORY
# ============================================================================

sub watcher {
    my ($self, $resource, %args) = @_;

    my $watcher = Net::Async::Kubernetes::Watcher->new(
        kube     => $self,
        resource => $resource,
        %args,
    );

    $self->add_child($watcher);
    return $watcher;
}

=method watcher

    my $watcher = $kube->watcher('Pod',
        namespace      => 'default',
        label_selector => 'app=web',
        on_added       => sub { my ($pod) = @_; ... },
        on_modified    => sub { my ($pod) = @_; ... },
        on_deleted     => sub { my ($pod) = @_; ... },
    );

Create and register a L<Net::Async::Kubernetes::Watcher> for the specified
resource type. The watcher is added as a child notifier and will start
automatically when the parent is added to a loop.

Returns the watcher object.

Arguments:

=over 4

=item C<$resource> - Resource type to watch (e.g., C<'Pod'>, C<'Deployment'>)

=item C<%args> - Watcher parameters (C<namespace>, C<label_selector>, callbacks, etc.)

=back

See L<Net::Async::Kubernetes::Watcher> for all available parameters.

=cut

sub controller {
    my ($self, %args) = @_;

    my $controller = Net::Async::Kubernetes::Controller->new(
        kube => $self,
        %args,
    );

    $self->add_child($controller);
    return $controller;
}

=method controller

    my $controller = $kube->controller(
        on_reconcile => sub {
            my ($ctx) = @_;
            ...
        },
    );

Create and register a L<Net::Async::Kubernetes::Controller> runtime bound to
this client. The controller is added as a child notifier and can register
resource watches, queue reconcile work, and patch object status.

Returns the controller object.

=cut

# ============================================================================
# HTTP TRANSPORT
# ============================================================================

sub _do_request {
    my ($self, $req) = @_;

    my $uri = URI->new($req->url);

    my @content_args;
    if (defined $req->content) {
        my $ct = $req->headers->{'Content-Type'} // 'application/json';
        @content_args = (content => $req->content, content_type => $ct);
    }

    return $self->_http->do_request(
        method  => $req->method,
        uri     => $uri,
        headers => $req->headers,
        @content_args,
        $self->_ssl_options,
    )->then(sub {
        my ($response) = @_;
        return Future->done(Kubernetes::REST::HTTPResponse->new(
            status  => $response->code,
            content => $response->decoded_content // $response->content // '',
        ));
    });
}

sub _do_streaming_request {
    my ($self, $req, $on_chunk) = @_;

    my $uri = URI->new($req->url);

    return $self->_http->do_request(
        method  => $req->method,
        uri     => $uri,
        headers => $req->headers,
        on_header => sub {
            my ($response) = @_;
            return sub {
                my ($chunk) = @_;
                if (defined $chunk) {
                    $on_chunk->($chunk);
                }
            };
        },
        $self->_ssl_options,
    )->then(sub {
        my ($response) = @_;
        return Future->done(Kubernetes::REST::HTTPResponse->new(
            status  => $response->code,
            content => '',
        ));
    });
}

sub _do_duplex_request {
    my ($self, $req, %callbacks) = @_;
    my $caller_name = delete($callbacks{caller}) // 'duplex request';
    my $loop = eval { $self->loop };
    return Future->fail("$caller_name requires Net::Async::Kubernetes to be added to an IO::Async::Loop")
        unless $loop;

    my $on_open  = $callbacks{on_open};
    my $on_frame = $callbacks{on_frame};
    my $on_close = $callbacks{on_close};
    my $on_error = $callbacks{on_error};

    my $ws_url = $self->_build_websocket_url($req->url);
    my $ws_req = $self->_build_websocket_request($req);

    my $client;
    my $session;
    my $close_notified = 0;

    my $detach_client = sub {
        return unless $client;
        return unless $client->can('parent');
        return unless $client->parent && $client->parent == $self;
        $self->remove_child($client);
    };

    my $notify_error = sub {
        my ($err) = @_;
        return unless ref($on_error) eq 'CODE';
        my $ok = eval { $on_error->($err); 1 };
        return if $ok;
        warn $@;
    };

    my $notify_close = sub {
        return if $close_notified++;
        if (ref($on_close) eq 'CODE') {
            my $ok = eval { $on_close->(@_); 1 };
            $notify_error->($@) unless $ok;
        }
        $detach_client->();
    };

    my $dispatch_frame = sub {
        my ($bytes) = @_;
        return unless ref($on_frame) eq 'CODE';
        return unless defined $bytes;
        return unless length $bytes;

        my $channel = ord(substr($bytes, 0, 1));
        my $payload = substr($bytes, 1);
        my $ok = eval { $on_frame->($channel, $payload); 1 };
        $notify_error->($@) unless $ok;
    };

    $client = $self->_make_websocket_client(
        on_binary_frame => sub {
            my (undef, $bytes) = @_;
            $dispatch_frame->($bytes);
        },
        on_text_frame => sub {
            my (undef, $text) = @_;
            return unless defined $text;
            my $bytes = $text;
            utf8::encode($bytes) if utf8::is_utf8($bytes);
            $dispatch_frame->($bytes);
        },
        on_close_frame => sub {
            my (undef, $payload) = @_;
            $notify_close->($payload);
        },
        on_read_error => sub {
            my (undef, $errno, $msg) = @_;
            my $err = defined $msg && length $msg ? $msg : ($errno // 'websocket read error');
            $notify_error->($err);
        },
        on_write_error => sub {
            my (undef, $errno, $msg) = @_;
            my $err = defined $msg && length $msg ? $msg : ($errno // 'websocket write error');
            $notify_error->($err);
        },
        on_closed => sub {
            $notify_close->();
        },
    );

    $self->add_child($client);

    return $client->connect(
        url => $ws_url,
        req => $ws_req,
        $self->_ssl_options,
    )->then(sub {
        $session = Net::Async::Kubernetes::PortForwardSession->new(
            ws_client => $client,
        );

        if (ref($on_open) eq 'CODE') {
            my $ok = eval { $on_open->($session); 1 };
            $notify_error->($@) unless $ok;
        }

        return Future->done($session);
    })->else(sub {
        my ($error) = @_;
        $notify_error->($error);
        $detach_client->();
        return Future->fail($error);
    });
}

sub _build_websocket_url {
    my ($self, $url) = @_;
    $url =~ s/^https:/wss:/i;
    $url =~ s/^http:/ws:/i;
    return $url;
}

sub _build_websocket_request {
    my ($self, $req) = @_;
    my $headers = $req->headers || {};

    my @extra_headers;
    my $subprotocol;

    for my $name (keys %$headers) {
        my $value = $headers->{$name};
        next unless defined $value;

        my $lc = lc($name);
        if ($lc eq 'sec-websocket-protocol') {
            $subprotocol = $value;
            next;
        }
        next if $lc eq 'connection';
        next if $lc eq 'upgrade';
        next if $lc eq 'host';
        next if $lc eq 'sec-websocket-key';
        next if $lc eq 'sec-websocket-version';

        push @extra_headers, $name, $value;
    }

    return Protocol::WebSocket::Request->new(
        headers => \@extra_headers,
        (defined $subprotocol ? (subprotocol => $subprotocol) : ()),
    );
}

sub _make_websocket_client {
    my ($self, %args) = @_;
    require Net::Async::WebSocket::Client;
    return Net::Async::WebSocket::Client->new(%args);
}

1;

__END__

=encoding UTF-8

=head1 SYNOPSIS

    use IO::Async::Loop;
    use Net::Async::Kubernetes;

    my $loop = IO::Async::Loop->new;

    # From kubeconfig (easiest)
    my $kube = Net::Async::Kubernetes->new(
        kubeconfig => "$ENV{HOME}/.kube/config",
    );
    $loop->add($kube);

    # In-cluster: auto-detects service account token (no config needed)
    my $kube = Net::Async::Kubernetes->new;
    $loop->add($kube);

    # Or with explicit server/credentials
    my $kube = Net::Async::Kubernetes->new(
        server      => { endpoint => 'https://kubernetes.local:6443' },
        credentials => { token => $token },
    );
    $loop->add($kube);

    # Future-based CRUD
    my $pods = $kube->list('Pod', namespace => 'default')->get;

    my $pod = $kube->get('Pod', 'nginx', namespace => 'default')->get;

    my $patched = $kube->patch('Pod', 'nginx',
        namespace => 'default',
        patch     => { metadata => { labels => { env => 'staging' } } },
    )->get;

    $kube->delete('Pod', 'nginx', namespace => 'default')->get;

    # Pod logs (one-shot)
    my $text = $kube->log('Pod', 'nginx',
        namespace => 'default',
        tailLines => 100,
    )->get;

    # Pod logs (streaming)
    $kube->log('Pod', 'nginx',
        namespace => 'default',
        follow    => 1,
        on_line   => sub { my ($event) = @_; say $event->line },
    )->get;

    # Port-forward (built-in websocket duplex support)
    my $pf = $kube->port_forward('Pod', 'nginx',
        namespace => 'default',
        ports     => [8080],
        on_frame  => sub { my ($channel, $payload) = @_; ... },
    )->get;

    $pf->write_channel(0, "GET / HTTP/1.1\r\n\r\n");
    $pf->close(code => 1000);

    # Pod exec (websocket duplex)
    my $exec = $kube->exec('Pod', 'nginx',
        namespace => 'default',
        command   => ['sh', '-c', 'id'],
        on_frame  => sub { my ($channel, $payload) = @_; ... },
    )->get;
    $exec->write_stdin("id\n");
    $exec->resize(width => 120, height => 40);

    # Pod attach (websocket duplex)
    my $attach = $kube->attach('Pod', 'nginx',
        namespace => 'default',
        container => 'app',
        stdin     => 1,
        stdout    => 1,
        stderr    => 1,
        tty       => 0,
        on_frame  => sub { my ($channel, $payload) = @_; ... },
    )->get;
    $attach->write_stdin("help\n");

    # Copy local file to pod and back (built on exec)
    $kube->cp_to_pod('Pod', 'nginx',
        namespace => 'default',
        local     => '/tmp/local.txt',
        remote    => '/tmp/remote.txt',
    )->get;
    $kube->cp_from_pod('Pod', 'nginx',
        namespace => 'default',
        remote    => '/tmp/remote.txt',
        local     => '/tmp/downloaded.txt',
    )->get;

    # Watcher with auto-reconnect
    my $watcher = $kube->watcher('Pod',
        namespace   => 'default',
        on_added    => sub { my ($pod) = @_; say "Added: " . $pod->metadata->name },
        on_modified => sub { my ($pod) = @_; say "Modified: " . $pod->metadata->name },
        on_deleted  => sub { my ($pod) = @_; say "Deleted: " . $pod->metadata->name },
    );

    $loop->run;

=head1 DESCRIPTION

C<Net::Async::Kubernetes> is an async Kubernetes client built on L<IO::Async>.
It extends L<IO::Async::Notifier> and uses L<Net::Async::HTTP> for
non-blocking HTTP communication, plus L<Net::Async::WebSocket::Client> for
duplex subresources like pod port-forward.

All CRUD, log, port-forward, exec, attach, and cp helper methods return L<Future> objects. The
L<Net::Async::Kubernetes::Watcher>
provides auto-reconnecting event streaming with separate callbacks per
event type.

Request preparation and response processing are delegated to
L<Kubernetes::REST>, so the same IO::K8s object inflation, short class
names, and CRD support are available.

Authentication is automatically resolved in the following order:

=over 4

=item 1. Explicit C<server> and C<credentials> parameters

=item 2. C<kubeconfig> file (via L<Kubernetes::REST::Kubeconfig>)

=item 3. In-cluster service account token at
C</var/run/secrets/kubernetes.io/serviceaccount/token> (automatic when
running inside a Kubernetes pod)

=back

=head1 SEE ALSO

L<Net::Async::Kubernetes::Watcher>, L<Net::Async::Kubernetes::Controller>,
L<Net::Async::Kubernetes::PortForwardSession>, L<Kubernetes::REST>,
L<IO::Async>, L<IO::K8s>, L<Net::Async::WebSocket::Client>

=cut
