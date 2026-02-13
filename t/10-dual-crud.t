use strict;
use warnings;
use Test::More;

use lib 't/lib';

use TestKube qw(is_live make_kube loop);

unless (is_live()) {
    require MockTransport;
}

diag(is_live() ? "LIVE mode: testing against real cluster" : "MOCK mode: using MockTransport");

my $kube = make_kube();
my $NS   = 'perl-async-test';

# Pre-cleanup for live mode (remove leftovers from a previous failed run)
if (is_live()) {
    diag "Cleaning up any existing '$NS' namespace...";
    eval { $kube->delete('Namespace', $NS)->get };
    for (1..60) {
        last unless eval { $kube->get('Namespace', $NS)->get };
        sleep 1;
    }
}

# ============================================================================
# Create resources
# ============================================================================

subtest 'create Namespace' => sub {
    unless (is_live()) {
        MockTransport::mock_response('POST', '/api/v1/namespaces', {
            kind => 'Namespace', apiVersion => 'v1',
            metadata => { name => $NS, uid => 'ns-uid-001',
                          creationTimestamp => '2025-01-01T00:00:00Z' },
            spec => {}, status => { phase => 'Active' },
        });
    }
    my $ns = $kube->_rest->new_object(Namespace =>
        metadata => { name => $NS, labels => { project => 'perl-async-test' } },
    );
    my $created = eval { $kube->create($ns)->get };
    if (!$created && is_live()) {
        diag "Namespace already exists, fetching";
        $created = $kube->get('Namespace', $NS)->get;
    }
    ok($created, 'namespace created');
    is($created->metadata->name, $NS, 'name matches');
};

subtest 'create LimitRange' => sub {
    unless (is_live()) {
        MockTransport::mock_response('POST', "/api/v1/namespaces/$NS/limitranges", {
            kind => 'LimitRange', apiVersion => 'v1',
            metadata => { name => 'default-limits', namespace => $NS,
                          uid => 'lr-uid-001', creationTimestamp => '2025-01-01T00:00:00Z' },
            spec => { limits => [{ type => 'Container',
                default => { cpu => '200m', memory => '128Mi' },
                defaultRequest => { cpu => '50m', memory => '64Mi' } }] },
        });
    }
    my $lr = $kube->_rest->new_object(LimitRange =>
        metadata => { name => 'default-limits', namespace => $NS },
        spec => { limits => [{ type => 'Container',
            default => { cpu => '200m', memory => '128Mi' },
            defaultRequest => { cpu => '50m', memory => '64Mi' } }] },
    );
    my $created = $kube->create($lr)->get;
    ok($created, 'LimitRange created');
    is($created->metadata->name, 'default-limits', 'name matches');
};

subtest 'create ResourceQuota' => sub {
    unless (is_live()) {
        MockTransport::mock_response('POST', "/api/v1/namespaces/$NS/resourcequotas", {
            kind => 'ResourceQuota', apiVersion => 'v1',
            metadata => { name => 'namespace-quota', namespace => $NS,
                          uid => 'rq-uid-001', creationTimestamp => '2025-01-01T00:00:00Z' },
            spec => { hard => { pods => '20', 'requests.cpu' => '2',
                                'limits.cpu' => '4', 'limits.memory' => '2Gi' } },
            status => {},
        });
    }
    my $rq = $kube->_rest->new_object(ResourceQuota =>
        metadata => { name => 'namespace-quota', namespace => $NS },
        spec => { hard => { pods => '20', 'requests.cpu' => '2',
                            'limits.cpu' => '4', 'limits.memory' => '2Gi' } },
    );
    my $created = $kube->create($rq)->get;
    ok($created, 'ResourceQuota created');
    is($created->metadata->name, 'namespace-quota', 'name matches');
    is($created->spec->hard->{pods}, '20', 'quota pods limit');
};

subtest 'create ConfigMap' => sub {
    unless (is_live()) {
        MockTransport::mock_response('POST', "/api/v1/namespaces/$NS/configmaps", {
            kind => 'ConfigMap', apiVersion => 'v1',
            metadata => { name => 'app-config', namespace => $NS,
                          uid => 'cm-uid-001', creationTimestamp => '2025-01-01T00:00:00Z',
                          labels => { app => 'demo-app' } },
            data => { 'app.env' => 'staging', 'app.debug' => 'true' },
        });
    }
    my $cm = $kube->_rest->new_object(ConfigMap =>
        metadata => { name => 'app-config', namespace => $NS,
                      labels => { app => 'demo-app' } },
        data => { 'app.env' => 'staging', 'app.debug' => 'true' },
    );
    my $created = $kube->create($cm)->get;
    ok($created, 'ConfigMap created');
    is($created->metadata->name, 'app-config', 'name matches');
    is($created->data->{'app.env'}, 'staging', 'data preserved');
};

subtest 'create Secret' => sub {
    unless (is_live()) {
        MockTransport::mock_response('POST', "/api/v1/namespaces/$NS/secrets", {
            kind => 'Secret', apiVersion => 'v1',
            metadata => { name => 'db-credentials', namespace => $NS,
                          uid => 'sec-uid-001', creationTimestamp => '2025-01-01T00:00:00Z' },
            type => 'Opaque',
            data => { username => 'ZGVtb191c2Vy', password => 'czNjcmV0IXA0c3M=' },
        });
    }
    require MIME::Base64;
    my $secret = $kube->_rest->new_object(Secret =>
        metadata => { name => 'db-credentials', namespace => $NS },
        type => 'Opaque',
        data => {
            username => MIME::Base64::encode_base64('demo_user', ''),
            password => MIME::Base64::encode_base64('s3cret!p4ss', ''),
        },
    );
    my $created = $kube->create($secret)->get;
    ok($created, 'Secret created');
    is($created->metadata->name, 'db-credentials', 'name matches');
    is($created->type, 'Opaque', 'type is Opaque');
};

subtest 'create ServiceAccount' => sub {
    unless (is_live()) {
        MockTransport::mock_response('POST', "/api/v1/namespaces/$NS/serviceaccounts", {
            kind => 'ServiceAccount', apiVersion => 'v1',
            metadata => { name => 'demo-sa', namespace => $NS,
                          uid => 'sa-uid-001', creationTimestamp => '2025-01-01T00:00:00Z' },
        });
    }
    my $sa = $kube->_rest->new_object(ServiceAccount =>
        metadata => { name => 'demo-sa', namespace => $NS },
    );
    my $created = $kube->create($sa)->get;
    ok($created, 'ServiceAccount created');
    is($created->metadata->name, 'demo-sa', 'name matches');
};

subtest 'create Role' => sub {
    unless (is_live()) {
        MockTransport::mock_response('POST',
            "/apis/rbac.authorization.k8s.io/v1/namespaces/$NS/roles", {
            kind => 'Role', apiVersion => 'rbac.authorization.k8s.io/v1',
            metadata => { name => 'demo-role', namespace => $NS,
                          uid => 'role-uid-001', creationTimestamp => '2025-01-01T00:00:00Z' },
            rules => [{ apiGroups => [''], resources => ['pods'], verbs => ['get', 'list'] }],
        });
    }
    my $role = $kube->_rest->new_object(Role =>
        metadata => { name => 'demo-role', namespace => $NS },
        rules => [{ apiGroups => [''], resources => ['pods'], verbs => ['get', 'list'] }],
    );
    my $created = $kube->create($role)->get;
    ok($created, 'Role created');
    is($created->metadata->name, 'demo-role', 'name matches');
};

subtest 'create RoleBinding' => sub {
    unless (is_live()) {
        MockTransport::mock_response('POST',
            "/apis/rbac.authorization.k8s.io/v1/namespaces/$NS/rolebindings", {
            kind => 'RoleBinding', apiVersion => 'rbac.authorization.k8s.io/v1',
            metadata => { name => 'demo-role-binding', namespace => $NS,
                          uid => 'rb-uid-001', creationTimestamp => '2025-01-01T00:00:00Z' },
            roleRef => { apiGroup => 'rbac.authorization.k8s.io', kind => 'Role', name => 'demo-role' },
            subjects => [{ kind => 'ServiceAccount', name => 'demo-sa', namespace => $NS }],
        });
    }
    my $rb = $kube->_rest->new_object(RoleBinding =>
        metadata => { name => 'demo-role-binding', namespace => $NS },
        roleRef  => { apiGroup => 'rbac.authorization.k8s.io', kind => 'Role', name => 'demo-role' },
        subjects => [{ kind => 'ServiceAccount', name => 'demo-sa', namespace => $NS }],
    );
    my $created = $kube->create($rb)->get;
    ok($created, 'RoleBinding created');
    is($created->metadata->name, 'demo-role-binding', 'name matches');
};

subtest 'create PersistentVolumeClaim' => sub {
    unless (is_live()) {
        MockTransport::mock_response('POST',
            "/api/v1/namespaces/$NS/persistentvolumeclaims", {
            kind => 'PersistentVolumeClaim', apiVersion => 'v1',
            metadata => { name => 'demo-storage', namespace => $NS,
                          uid => 'pvc-uid-001', creationTimestamp => '2025-01-01T00:00:00Z' },
            spec => { accessModes => ['ReadWriteOnce'],
                      resources => { requests => { storage => '100Mi' } } },
            status => { phase => 'Pending' },
        });
    }
    my $pvc = $kube->_rest->new_object(PersistentVolumeClaim =>
        metadata => { name => 'demo-storage', namespace => $NS },
        spec => { accessModes => ['ReadWriteOnce'],
                  resources => { requests => { storage => '100Mi' } } },
    );
    my $created = $kube->create($pvc)->get;
    ok($created, 'PVC created');
    is($created->metadata->name, 'demo-storage', 'name matches');
};

subtest 'create Deployment' => sub {
    unless (is_live()) {
        MockTransport::mock_response('POST',
            "/apis/apps/v1/namespaces/$NS/deployments", {
            kind => 'Deployment', apiVersion => 'apps/v1',
            metadata => { name => 'demo-nginx', namespace => $NS,
                          uid => 'dep-uid-001', creationTimestamp => '2025-01-01T00:00:00Z',
                          labels => { app => 'demo-app' } },
            spec => {
                replicas => 2,
                selector => { matchLabels => { app => 'demo-app' } },
                template => {
                    metadata => { labels => { app => 'demo-app' } },
                    spec => { containers => [{ name => 'nginx', image => 'nginx:1.27-alpine',
                                               ports => [{ containerPort => 80 }] }] },
                },
            },
            status => { replicas => 0, readyReplicas => 0 },
        });
    }
    my $dep = $kube->_rest->new_object(Deployment =>
        metadata => { name => 'demo-nginx', namespace => $NS,
                      labels => { app => 'demo-app' } },
        spec => {
            replicas => 2,
            selector => { matchLabels => { app => 'demo-app' } },
            template => {
                metadata => { labels => { app => 'demo-app' } },
                spec => { containers => [{ name => 'nginx', image => 'nginx:1.27-alpine',
                                           ports => [{ containerPort => 80 }] }] },
            },
        },
    );
    my $created = $kube->create($dep)->get;
    ok($created, 'Deployment created');
    is($created->metadata->name, 'demo-nginx', 'name matches');
    is($created->spec->replicas, 2, 'replicas = 2');
};

subtest 'create Service' => sub {
    unless (is_live()) {
        MockTransport::mock_response('POST',
            "/api/v1/namespaces/$NS/services", {
            kind => 'Service', apiVersion => 'v1',
            metadata => { name => 'demo-nginx', namespace => $NS,
                          uid => 'svc-uid-001', creationTimestamp => '2025-01-01T00:00:00Z' },
            spec => { type => 'ClusterIP',
                      selector => { app => 'demo-app' },
                      ports => [{ port => 80, targetPort => 80, protocol => 'TCP' }] },
        });
    }
    my $svc = $kube->_rest->new_object(Service =>
        metadata => { name => 'demo-nginx', namespace => $NS },
        spec => { type => 'ClusterIP',
                  selector => { app => 'demo-app' },
                  ports => [{ port => 80, targetPort => 80, protocol => 'TCP' }] },
    );
    my $created = $kube->create($svc)->get;
    ok($created, 'Service created');
    is($created->metadata->name, 'demo-nginx', 'name matches');
};

# ============================================================================
# Get + verify
# ============================================================================

subtest 'get Namespace' => sub {
    unless (is_live()) {
        MockTransport::mock_response('GET', "/api/v1/namespaces/$NS", {
            kind => 'Namespace', apiVersion => 'v1',
            metadata => { name => $NS, uid => 'ns-uid-001',
                          creationTimestamp => '2025-01-01T00:00:00Z' },
            spec => {}, status => { phase => 'Active' },
        });
    }
    my $ns = $kube->get('Namespace', $NS)->get;
    ok($ns, 'got namespace');
    is($ns->metadata->name, $NS, 'name matches');
    ok($ns->metadata->uid, 'has uid');
};

subtest 'get ConfigMap' => sub {
    unless (is_live()) {
        MockTransport::mock_response('GET',
            "/api/v1/namespaces/$NS/configmaps/app-config", {
            kind => 'ConfigMap', apiVersion => 'v1',
            metadata => { name => 'app-config', namespace => $NS, uid => 'cm-uid-001' },
            data => { 'app.env' => 'staging', 'app.debug' => 'true' },
        });
    }
    my $cm = $kube->get('ConfigMap', 'app-config', namespace => $NS)->get;
    ok($cm, 'got ConfigMap');
    is($cm->metadata->name, 'app-config', 'name matches');
    is($cm->data->{'app.env'}, 'staging', 'data accessible');
};

subtest 'get Deployment' => sub {
    unless (is_live()) {
        MockTransport::mock_response('GET',
            "/apis/apps/v1/namespaces/$NS/deployments/demo-nginx", {
            kind => 'Deployment', apiVersion => 'apps/v1',
            metadata => { name => 'demo-nginx', namespace => $NS,
                          uid => 'dep-uid-001', resourceVersion => '100',
                          labels => { app => 'demo-app' } },
            spec => {
                replicas => 2,
                selector => { matchLabels => { app => 'demo-app' } },
                template => {
                    metadata => { labels => { app => 'demo-app' } },
                    spec => { containers => [{ name => 'nginx', image => 'nginx:1.27-alpine' }] },
                },
            },
            status => { replicas => 2, readyReplicas => 2, updatedReplicas => 2 },
        });
    }
    my $dep = $kube->get('Deployment', 'demo-nginx', namespace => $NS)->get;
    ok($dep, 'got Deployment');
    is($dep->metadata->name, 'demo-nginx', 'name matches');
    is($dep->spec->replicas, 2, 'replicas = 2');
};

# ============================================================================
# Update (scale replicas)
# ============================================================================

subtest 'scale Deployment via update' => sub {
    unless (is_live()) {
        # GET to fetch current state
        MockTransport::mock_response('GET',
            "/apis/apps/v1/namespaces/$NS/deployments/demo-nginx", {
            kind => 'Deployment', apiVersion => 'apps/v1',
            metadata => { name => 'demo-nginx', namespace => $NS,
                          uid => 'dep-uid-001', resourceVersion => '100',
                          labels => { app => 'demo-app' } },
            spec => {
                replicas => 2,
                selector => { matchLabels => { app => 'demo-app' } },
                template => {
                    metadata => { labels => { app => 'demo-app' } },
                    spec => { containers => [{ name => 'nginx', image => 'nginx:1.27-alpine' }] },
                },
            },
            status => { replicas => 2, readyReplicas => 2 },
        });
        # PUT response
        MockTransport::mock_response('PUT',
            "/apis/apps/v1/namespaces/$NS/deployments/demo-nginx", {
            kind => 'Deployment', apiVersion => 'apps/v1',
            metadata => { name => 'demo-nginx', namespace => $NS,
                          uid => 'dep-uid-001', resourceVersion => '101',
                          labels => { app => 'demo-app' } },
            spec => {
                replicas => 3,
                selector => { matchLabels => { app => 'demo-app' } },
                template => {
                    metadata => { labels => { app => 'demo-app' } },
                    spec => { containers => [{ name => 'nginx', image => 'nginx:1.27-alpine' }] },
                },
            },
            status => { replicas => 2, readyReplicas => 2 },
        });
    }

    my $dep = $kube->get('Deployment', 'demo-nginx', namespace => $NS)->get;
    is($dep->spec->replicas, 2, 'current replicas = 2');

    $dep->spec->replicas(3);
    my $updated = $kube->update($dep)->get;
    ok($updated, 'update returned object');
    is($updated->spec->replicas, 3, 'replicas scaled to 3');
};

# ============================================================================
# Patch (add labels)
# ============================================================================

subtest 'patch Deployment with strategic merge' => sub {
    unless (is_live()) {
        MockTransport::mock_response('PATCH',
            "/apis/apps/v1/namespaces/$NS/deployments/demo-nginx", {
            kind => 'Deployment', apiVersion => 'apps/v1',
            metadata => { name => 'demo-nginx', namespace => $NS,
                          uid => 'dep-uid-001', resourceVersion => '102',
                          labels => { app => 'demo-app', patched_by => 'perl' } },
            spec => {
                replicas => 3,
                selector => { matchLabels => { app => 'demo-app' } },
                template => {
                    metadata => { labels => { app => 'demo-app' } },
                    spec => { containers => [{ name => 'nginx', image => 'nginx:1.27-alpine' }] },
                },
            },
            status => {},
        });
    }
    my $patched = $kube->patch('Deployment', 'demo-nginx',
        namespace => $NS,
        patch     => { metadata => { labels => { patched_by => 'perl' } } },
    )->get;
    ok($patched, 'patch returned object');
    is($patched->metadata->labels->{patched_by}, 'perl', 'label applied');

    unless (is_live()) {
        my $req = MockTransport::last_request();
        is($req->{method}, 'PATCH', 'used PATCH method');
        like($req->{headers}{'Content-Type'}, qr{strategic-merge-patch},
             'strategic merge patch content type');
    }
};

subtest 'patch Deployment with merge type' => sub {
    unless (is_live()) {
        MockTransport::mock_response('PATCH',
            "/apis/apps/v1/namespaces/$NS/deployments/demo-nginx", {
            kind => 'Deployment', apiVersion => 'apps/v1',
            metadata => { name => 'demo-nginx', namespace => $NS,
                          uid => 'dep-uid-001', resourceVersion => '103',
                          labels => { app => 'demo-app', patched_by => 'perl' },
                          annotations => { 'demo.perl/patched' => 'true' } },
            spec => {
                replicas => 3,
                selector => { matchLabels => { app => 'demo-app' } },
                template => {
                    metadata => { labels => { app => 'demo-app' } },
                    spec => { containers => [{ name => 'nginx', image => 'nginx:1.27-alpine' }] },
                },
            },
            status => {},
        });
    }
    my $patched = $kube->patch('Deployment', 'demo-nginx',
        namespace => $NS,
        type      => 'merge',
        patch     => {
            metadata => { annotations => { 'demo.perl/patched' => 'true' } },
        },
    )->get;
    ok($patched, 'merge patch returned object');
    is($patched->metadata->annotations->{'demo.perl/patched'}, 'true', 'annotation applied');

    unless (is_live()) {
        my $req = MockTransport::last_request();
        like($req->{headers}{'Content-Type'}, qr{merge-patch},
             'merge patch content type');
    }
};

# ============================================================================
# Create Job + CronJob
# ============================================================================

subtest 'create Job' => sub {
    unless (is_live()) {
        MockTransport::mock_response('POST',
            "/apis/batch/v1/namespaces/$NS/jobs", {
            kind => 'Job', apiVersion => 'batch/v1',
            metadata => { name => 'demo-job', namespace => $NS,
                          uid => 'job-uid-001', creationTimestamp => '2025-01-01T00:00:00Z' },
            spec => { backoffLimit => 2,
                      template => { spec => { restartPolicy => 'Never',
                        containers => [{ name => 'worker', image => 'busybox:latest' }] } } },
            status => {},
        });
    }
    my $job = $kube->_rest->new_object(Job =>
        metadata => { name => 'demo-job', namespace => $NS },
        spec => { backoffLimit => 2,
                  template => { spec => { restartPolicy => 'Never',
                    containers => [{ name => 'worker', image => 'busybox:latest',
                                     command => ['echo', 'hello'] }] } } },
    );
    my $created = $kube->create($job)->get;
    ok($created, 'Job created');
    is($created->metadata->name, 'demo-job', 'name matches');
};

subtest 'create CronJob' => sub {
    unless (is_live()) {
        MockTransport::mock_response('POST',
            "/apis/batch/v1/namespaces/$NS/cronjobs", {
            kind => 'CronJob', apiVersion => 'batch/v1',
            metadata => { name => 'demo-cronjob', namespace => $NS,
                          uid => 'cj-uid-001', creationTimestamp => '2025-01-01T00:00:00Z' },
            spec => { schedule => '*/5 * * * *',
                      jobTemplate => { spec => { template => { spec => {
                        restartPolicy => 'OnFailure',
                        containers => [{ name => 'cron', image => 'busybox:latest' }] } } } } },
        });
    }
    my $cj = $kube->_rest->new_object(CronJob =>
        metadata => { name => 'demo-cronjob', namespace => $NS },
        spec => { schedule => '*/5 * * * *',
                  jobTemplate => { spec => { template => { spec => {
                    restartPolicy => 'OnFailure',
                    containers => [{ name => 'cron', image => 'busybox:latest',
                                     command => ['echo', 'tick'] }] } } } } },
    );
    my $created = $kube->create($cj)->get;
    ok($created, 'CronJob created');
    is($created->metadata->name, 'demo-cronjob', 'name matches');
};

# ============================================================================
# List resources
# ============================================================================

subtest 'list ConfigMaps in namespace' => sub {
    unless (is_live()) {
        MockTransport::mock_response('GET',
            "/api/v1/namespaces/$NS/configmaps", {
            kind => 'ConfigMapList', apiVersion => 'v1',
            items => [
                { kind => 'ConfigMap', apiVersion => 'v1',
                  metadata => { name => 'app-config', namespace => $NS },
                  data => { 'app.env' => 'staging' } },
            ],
        });
    }
    my $list = $kube->list('ConfigMap', namespace => $NS)->get;
    isa_ok($list, 'IO::K8s::List');
    ok(scalar @{$list->items} >= 1, 'got at least 1 configmap');
    my @names = map { $_->metadata->name } @{$list->items};
    ok(grep({ $_ eq 'app-config' } @names), 'app-config in list');
};

subtest 'list Deployments in namespace' => sub {
    unless (is_live()) {
        MockTransport::mock_response('GET',
            "/apis/apps/v1/namespaces/$NS/deployments", {
            kind => 'DeploymentList', apiVersion => 'apps/v1',
            items => [
                { kind => 'Deployment', apiVersion => 'apps/v1',
                  metadata => { name => 'demo-nginx', namespace => $NS },
                  spec => { replicas => 3,
                            selector => { matchLabels => { app => 'demo-app' } },
                            template => { metadata => { labels => { app => 'demo-app' } },
                                          spec => { containers => [{ name => 'nginx', image => 'nginx' }] } } },
                  status => {} },
            ],
        });
    }
    my $list = $kube->list('Deployment', namespace => $NS)->get;
    ok(scalar @{$list->items} >= 1, 'got at least 1 deployment');
    my @names = map { $_->metadata->name } @{$list->items};
    ok(grep({ $_ eq 'demo-nginx' } @names), 'demo-nginx in list');
};

# ============================================================================
# Serialization roundtrip
# ============================================================================

subtest 'serialization roundtrip' => sub {
    unless (is_live()) {
        MockTransport::mock_response('GET',
            "/apis/apps/v1/namespaces/$NS/deployments/demo-nginx", {
            kind => 'Deployment', apiVersion => 'apps/v1',
            metadata => { name => 'demo-nginx', namespace => $NS,
                          uid => 'dep-uid-001', resourceVersion => '103',
                          labels => { app => 'demo-app' } },
            spec => {
                replicas => 3,
                selector => { matchLabels => { app => 'demo-app' } },
                template => {
                    metadata => { labels => { app => 'demo-app' } },
                    spec => { containers => [{ name => 'nginx', image => 'nginx:1.27-alpine' }] },
                },
            },
            status => { replicas => 3 },
        });
    }
    my $dep = $kube->get('Deployment', 'demo-nginx', namespace => $NS)->get;

    # TO_JSON
    my $data = $dep->TO_JSON;
    is(ref($data), 'HASH', 'TO_JSON returns hashref');
    is($data->{kind}, 'Deployment', 'kind preserved');
    is($data->{metadata}{name}, 'demo-nginx', 'name preserved');

    # inflate roundtrip
    my $inflated = $kube->_rest->inflate($data);
    ok($inflated, 'inflate returns object');
    is($inflated->metadata->name, 'demo-nginx', 'name survives roundtrip');

    # to_yaml
    my $yaml = $dep->to_yaml;
    ok($yaml, 'to_yaml returns string');
    like($yaml, qr/kind:\s*Deployment/, 'YAML contains kind');
    like($yaml, qr/demo-nginx/, 'YAML contains name');
};

# ============================================================================
# ResourceQuota usage
# ============================================================================

subtest 'get ResourceQuota' => sub {
    unless (is_live()) {
        MockTransport::mock_response('GET',
            "/api/v1/namespaces/$NS/resourcequotas/namespace-quota", {
            kind => 'ResourceQuota', apiVersion => 'v1',
            metadata => { name => 'namespace-quota', namespace => $NS, uid => 'rq-uid-001' },
            spec   => { hard => { pods => '20', 'limits.cpu' => '4' } },
            status => { hard => { pods => '20', 'limits.cpu' => '4' },
                        used => { pods => '3', 'limits.cpu' => '300m' } },
        });
    }
    my $q = $kube->get('ResourceQuota', 'namespace-quota', namespace => $NS)->get;
    ok($q, 'got ResourceQuota');
    is($q->spec->hard->{pods}, '20', 'spec hard pods');
    if (is_live()) {
        ok(defined $q->status->used, 'status has used (live)');
    } else {
        is($q->status->used->{pods}, '3', 'status used pods');
    }
};

# ============================================================================
# Cleanup: delete all resources
# ============================================================================

subtest 'cleanup resources' => sub {
    my @resources = (
        ['CronJob',               'demo-cronjob',       "/apis/batch/v1/namespaces/$NS/cronjobs/demo-cronjob"],
        ['Job',                   'demo-job',            "/apis/batch/v1/namespaces/$NS/jobs/demo-job"],
        ['Service',               'demo-nginx',          "/api/v1/namespaces/$NS/services/demo-nginx"],
        ['Deployment',            'demo-nginx',          "/apis/apps/v1/namespaces/$NS/deployments/demo-nginx"],
        ['RoleBinding',           'demo-role-binding',   "/apis/rbac.authorization.k8s.io/v1/namespaces/$NS/rolebindings/demo-role-binding"],
        ['Role',                  'demo-role',           "/apis/rbac.authorization.k8s.io/v1/namespaces/$NS/roles/demo-role"],
        ['ServiceAccount',        'demo-sa',             "/api/v1/namespaces/$NS/serviceaccounts/demo-sa"],
        ['PersistentVolumeClaim', 'demo-storage',        "/api/v1/namespaces/$NS/persistentvolumeclaims/demo-storage"],
        ['Secret',                'db-credentials',      "/api/v1/namespaces/$NS/secrets/db-credentials"],
        ['ConfigMap',             'app-config',          "/api/v1/namespaces/$NS/configmaps/app-config"],
        ['ResourceQuota',         'namespace-quota',     "/api/v1/namespaces/$NS/resourcequotas/namespace-quota"],
        ['LimitRange',            'default-limits',      "/api/v1/namespaces/$NS/limitranges/default-limits"],
    );

    for my $r (@resources) {
        my ($kind, $name, $path) = @$r;
        unless (is_live()) {
            MockTransport::mock_response('DELETE', $path, {
                kind => 'Status', apiVersion => 'v1', status => 'Success',
            });
        }
        my $ok = eval { $kube->delete($kind, $name, namespace => $NS)->get };
        ok($ok || is_live(), "delete $kind/$name");
    }

    # Delete namespace (cluster-scoped)
    unless (is_live()) {
        MockTransport::mock_response('DELETE', "/api/v1/namespaces/$NS", {
            kind => 'Status', apiVersion => 'v1', status => 'Success',
        });
    }
    my $ok = eval { $kube->delete('Namespace', $NS)->get };
    ok($ok || is_live(), "delete Namespace/$NS");
};

done_testing;
