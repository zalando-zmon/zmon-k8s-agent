import os
import itertools
import psycopg2

from zmon_agent.discovery.kubernetes.base import BaseDiscovery, STATEFULSET_TYPE

POSTGRESQL_CLUSTER_TYPE = 'postgresql_cluster'
POSTGRESQL_CLUSTER_MEMBER_TYPE = 'postgresql_cluster_member'
POSTGRESQL_DATABASE_TYPE = 'postgresql_database'
POSTGRESQL_DATABASE_REPLICA_TYPE = 'postgresql_database_replica'
POSTGRESQL_DEFAULT_PORT = 5432
POSTGRESQL_CONNECT_TIMEOUT = os.environ.get('ZMON_AGENT_POSTGRESQL_CONNECT_TIMEOUT', 2)


class PostgresDiscovery(BaseDiscovery):

    def __init__(self, region, infrastructure_account):
        super().__init__(region, infrastructure_account)
        self.postgres_user = os.environ.get('ZMON_AGENT_POSTGRES_USER')
        self.postgres_pass = os.environ.get('ZMON_AGENT_POSTGRES_PASS')
        self.hosted_zone_format_string = os.environ.get('ZMON_HOSTED_ZONE_FORMAT_STRING', '{}.{}.example.org')

    def provides(self):
        return [
            POSTGRESQL_CLUSTER_TYPE,
            POSTGRESQL_CLUSTER_MEMBER_TYPE,
            POSTGRESQL_DATABASE_TYPE,
            POSTGRESQL_DATABASE_REPLICA_TYPE,
        ]

    def requires(self):
        return [STATEFULSET_TYPE]

    def filter_queries(self):
        return [
            {
                'created_by': self.agent_type,
                'kube_cluster': self.cluster_id,
                'type': t,
            } for t in self.provides()
        ]

    def entities(self, dependencies):
        deps = list(dependencies.values())
        postgresql_cluster_entities, pce = itertools.tee(self.get_postgresql_clusters(deps))
        postgresql_cluster_member_entities = self.get_postgresql_cluster_members()
        postgresql_database_entities = self.get_postgresql_databases(pce)

        return list(itertools.chain(
            postgresql_cluster_entities,
            postgresql_cluster_member_entities,
            postgresql_database_entities,
        ))

    def get_postgresql_clusters(self, statefulsets):
        ssets = [ss for ss in statefulsets]
        # TODO in theory clusters should be discovered using CRDs
        services = self.get_all(self.kube_client.get_services, self.namespace)

        for service in services:
            obj = service.obj

            labels = obj['metadata'].get('labels', None)
            if not labels:
                continue
            version = labels.get('version')

            # we skip non-Spilos and replica services
            if labels.get('application') != 'spilo' or labels.get('spilo-role') == 'replica':
                continue

            service_namespace = obj['metadata']['namespace']
            service_dns_name = '{}.{}.svc.cluster.local'.format(service.name, service_namespace)

            statefulset_error = ''
            statefulset = [ss for ss in ssets if ss['version'] == version]
            ss = {}

            if not statefulset:  # can happen when the replica count is 0.In this case we don't have a running cluster.
                statefulset_error = 'There is no statefulset attached'
            else:
                ss = statefulset[0]

            yield {
                'id': 'pg-{}[{}]'.format(service.name, self.cluster_id),
                'type': POSTGRESQL_CLUSTER_TYPE,
                'kube_cluster': self.cluster_id,
                'account_alias': self.alias,
                'environment': self.environment,
                'created_by': self.agent_type,
                'infrastructure_account': self.infrastructure_account,
                'region': self.region,
                'spilo_cluster': version,
                'application': "spilo",
                'version': version,
                'dnsname': service_dns_name,
                'shards': {
                    'postgres': '{}:{}/postgres'.format(service_dns_name, POSTGRESQL_DEFAULT_PORT)
                },
                'expected_replica_count': ss.get('replicas', 0),
                'current_replica_count': ss.get('actual_replicas', 0),
                'statefulset_error': statefulset_error,
                'deeplink1': '{}/#/status/{}'.format(
                    self.hosted_zone_format_string.format('pgui', self.alias), version),
                'icon1': 'fa-server',
                'deeplink2': '{}/#/clusters/{}'.format(
                    self.hosted_zone_format_string.format('pgview', self.alias), version),
                'icon2': 'fa-line-chart'
            }

    def get_postgresql_cluster_members(self):
        pods = self.get_all(self.kube_client.get_pods, self.namespace)
        pvcs = self.get_all(self.kube_client.get_persistentvolumeclaims, self.namespace)
        pvs = self.get_all(self.kube_client.get_persistentvolumes, self.namespace)

        for pod in pods:
            obj = pod.obj

            # TODO: filter in the API call
            labels = obj['metadata'].get('labels', {})
            if labels.get('application') != 'spilo' or labels.get('version') is None:
                continue

            pod_number = pod.name.split('-')[-1]
            pod_namespace = obj['metadata']['namespace']
            service_dns_name = '{}.{}.svc.cluster.local'.format(labels['version'], pod_namespace)

            container = obj['spec']['containers'][0]  # we don't assume more than one container
            cluster_name = [env['value'] for env in container['env'] if env['name'] == 'SCOPE'][0]

            ebs_volume_id = ''
            # unfortunately, there appears to be no way of filtering these on the server side :(
            try:
                pvc_name = obj['spec']['volumes'][0]['persistentVolumeClaim']['claimName']  # assume only one PVC
                for pvc in pvcs:
                    if pvc.name == pvc_name:
                        for pv in pvs:
                            if pv.name == pvc.obj['spec']['volumeName']:
                                ebs_volume_id = pv.obj['spec']['awsElasticBlockStore']['volumeID'].split('/')[-1]
                                break  # only one matching item is expected, so when found, we can leave the loop
                        break
            except KeyError:
                pass

            yield {
                'id': 'pg-{}-{}[{}]'.format(service_dns_name, pod_number, self.cluster_id),
                'type': POSTGRESQL_CLUSTER_MEMBER_TYPE,
                'kube_cluster': self.cluster_id,
                'account_alias': self.alias,
                'environment': self.environment,
                'created_by': self.agent_type,
                'infrastructure_account': self.infrastructure_account,
                'region': self.region,
                'cluster_dns_name': service_dns_name,
                'pod': pod.name,
                'pod_phase': obj['status']['phase'],
                'image': container['image'],
                'container_name': container['name'],
                'ip': obj['status'].get('podIP', ''),
                'spilo_cluster': cluster_name,
                'spilo_role': labels.get('spilo-role', ''),
                'application': 'spilo',
                'version': cluster_name,
                'volume': ebs_volume_id,
                'deeplink1': '{}/#/status/{}'.format(
                    self.hosted_zone_format_string.format('pgui', self.alias), cluster_name),
                'icon1': 'fa-server',
                'deeplink2': '{}/#/clusters/{}/{}'.format(
                    self.hosted_zone_format_string.format('pgview', self.alias), cluster_name, pod.name),
                'icon2': 'fa-line-chart'
            }

    def get_postgresql_databases(self, postgresql_clusters):
        if not (self.postgres_user and self.postgres_pass):
            return

        for pgcluster in postgresql_clusters:
            dbnames = self.list_postgres_databases(host=pgcluster['dnsname'],
                                                   port=POSTGRESQL_DEFAULT_PORT,
                                                   user=self.postgres_user,
                                                   password=self.postgres_pass,
                                                   dbname='postgres',
                                                   sslmode='require')
            for db in dbnames:
                yield {
                    'id': '{}-{}'.format(db, pgcluster['id']),
                    'type': POSTGRESQL_DATABASE_TYPE,
                    'kube_cluster': self.cluster_id,
                    'alias': self.alias,
                    'environment': self.environment,
                    'created_by': self.agent_type,
                    'infrastructure_account': self.infrastructure_account,
                    'region': self.region,
                    'version': pgcluster['version'],
                    'postgresql_cluster': pgcluster['id'],
                    'database_name': db,
                    'shards': {
                        db: '{}:{}/{}'.format(pgcluster['dnsname'], POSTGRESQL_DEFAULT_PORT, db)
                    },
                    'role': 'master'
                }

                if pgcluster['expected_replica_count'] > 1:  # the first k8s replica is the master itself
                    name_parts = pgcluster['dnsname'].split('.')
                    repl_dnsname = '.'.join([name_parts[0] + '-repl'] + name_parts[1:])
                    yield {
                        'id': '{}-repl-{}'.format(db, pgcluster['id']),
                        'type': POSTGRESQL_DATABASE_REPLICA_TYPE,
                        'kube_cluster': self.cluster_id,
                        'alias': self.alias,
                        'environment': self.environment,
                        'created_by': self.agent_type,
                        'infrastructure_account': self.infrastructure_account,
                        'region': self.region,
                        'version': pgcluster['version'],
                        'postgresql_cluster': pgcluster['id'],
                        'database_name': db,
                        'shards': {
                            db: '{}:{}/{}'.format(repl_dnsname, POSTGRESQL_DEFAULT_PORT, db)
                        },
                        'role': 'replica'
                    }

    def list_postgres_databases(self, *args, **kwargs):
        try:
            kwargs.update({'connect_timeout': POSTGRESQL_CONNECT_TIMEOUT})

            conn = psycopg2.connect(*args, **kwargs)

            cur = conn.cursor()
            cur.execute("""
                SELECT datname
                  FROM pg_database
                 WHERE datname NOT IN('postgres', 'template0', 'template1')
            """)
            return [row[0] for row in cur.fetchall()]
        except Exception:
            self.logger.exception("Failed to list DBs on %s", kwargs.get('host', '{no host specified}'))
            return []
