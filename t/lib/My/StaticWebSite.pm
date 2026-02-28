package My::StaticWebSite;
# Example CRD class for testing custom resource support.

use IO::K8s::APIObject
    api_version     => 'homelab.example.com/v1',
    resource_plural => 'staticwebsites';

with 'IO::K8s::Role::Namespaced';

k8s spec   => { Str => 1 };
k8s status => { Str => 1 };

1;
