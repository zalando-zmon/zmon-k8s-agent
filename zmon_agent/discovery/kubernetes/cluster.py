"""Kubernetes Discovery class used by agent core"""

import itertools

from zmon_agent.discovery.kubernetes.base import BaseDiscovery, \
        POD_TYPE, CONTAINER_TYPE, NODE_TYPE, REPLICASET_TYPE, \
        STATEFULSET_TYPE, DAEMONSET_TYPE, INGRESS_TYPE, SERVICE_TYPE, \
        PROTECTED_FIELDS, SKIPPED_ANNOTATIONS, INSTANCE_TYPE_LABEL


class KubeDiscovery(BaseDiscovery):

    def __init__(self, region, infrastructure_account):
        super().__init__(region, infrastructure_account)

    def requires(self):
        return []

    def provides(self):
        return [
            POD_TYPE, CONTAINER_TYPE, NODE_TYPE, REPLICASET_TYPE, STATEFULSET_TYPE,
            DAEMONSET_TYPE, INGRESS_TYPE, SERVICE_TYPE,
        ]

    def filter_queries(self):
        return [
            {
                'created_by': self.agent_type,
                'kube_cluster': self.cluster_id,
                'type': t,
            } for t in self.provides()
        ]

    def entities(self, dependencies):

        pod_container_entities = list(self.get_cluster_pods_and_containers())
        # Pass pod_entities in order to get node_pod_count!
        node_entities = self.get_cluster_nodes(pod_container_entities)

        service_entities = self.get_cluster_services()
        replicaset_entities = self.get_cluster_replicasets()
        daemonset_entities = self.get_cluster_daemonsets()
        statefulset_entities = self.get_cluster_statefulsets()

        ingress_entities = self.get_cluster_ingresses()

        return list(itertools.chain(
            pod_container_entities, node_entities, service_entities, replicaset_entities,
            daemonset_entities, ingress_entities, statefulset_entities))

    def entity_labels(self, obj, *sources):
        result = {}

        for key in sources:
            for label, val in obj['metadata'].get(key, {}).items():
                if label in PROTECTED_FIELDS:
                    self.logger.warning('Skipping label [{}:{}] as it is in Protected entity fields {}'.format(
                        label, val, PROTECTED_FIELDS))
                elif label in SKIPPED_ANNOTATIONS:
                    pass
                else:
                    result[label] = val

        return result

    def get_cluster_pods_and_containers(self):
        """
        Return all Pods as ZMON entities.
        """

        pods = self.get_all(self.kube_client.get_pods, self.namespace)

        for pod in pods:
            if not pod.ready:
                continue

            obj = pod.obj

            containers = obj['spec'].get('containers', [])
            container_statuses = {c['name']: c for c in obj['status']['containerStatuses']}
            conditions = {c['type']: c['status'] for c in obj['status']['conditions']}

            pod_labels = self.entity_labels(obj, 'labels')
            pod_annotations = self.entity_labels(obj, 'annotations')

            # Properties shared between pod entity and container entity
            shared_properties = {
                'kube_cluster': self.cluster_id,
                'alias': self.alias,
                'environment': self.environment,
                'created_by': self.agent_type,
                'infrastructure_account': self.infrastructure_account,
                'region': self.region,

                'ip': obj['status'].get('podIP', ''),
                'host': obj['status'].get('podIP', ''),

                'pod_name': pod.name,
                'pod_namespace': obj['metadata']['namespace'],
                'pod_host_ip': obj['status'].get('hostIP', ''),
                'pod_node_name': obj['spec']['nodeName'],

                'pod_phase': obj['status'].get('phase'),
                'pod_initialized': conditions.get('Initialized', False),
                'pod_ready': conditions.get('Ready', True),
                'pod_scheduled': conditions.get('PodScheduled', False)
            }

            pod_entity = {
                'id': 'pod-{}-{}[{}]'.format(pod.name, pod.namespace, self.cluster_id),
                'type': POD_TYPE,
                'containers': {}
            }

            pod_entity.update(shared_properties)
            pod_entity.update(pod_labels)
            pod_entity.update(pod_annotations)

            for container in containers:
                container_name = container['name']
                container_image = container['image']
                container_ready = container_statuses.get(container['name'], {}).get('ready', False)
                container_restarts = container_statuses.get(container['name'], {}).get('restartCount', 0)
                container_ports = [p['containerPort'] for p in container.get('ports', []) if 'containerPort' in p]

                container_entity = {
                    'id': 'container-{}-{}-{}[{}]'.format(pod.name, pod.namespace, container_name, self.cluster_id),
                    'type': CONTAINER_TYPE,
                    'container_name': container_name,
                    'container_image': container_image,
                    'container_ready': container_ready,
                    'container_restarts': container_restarts,
                    'container_ports': container_ports
                }
                pod_entity['containers'][container_name] = {
                    'image': container_image,
                    'ready': container_ready,
                    'restarts': container_restarts,
                    'ports': container_ports
                }

                container_entity.update(pod_labels)
                container_entity.update(shared_properties)
                yield container_entity

            yield pod_entity

    def get_cluster_services(self):
        endpoints = self.get_all(self.kube_client.get_endpoints, self.namespace)
        # number of endpoints per service
        endpoints_map = {e.name: len(e.obj['subsets']) for e in endpoints if e.obj.get('subsets')}

        services = self.get_all(self.kube_client.get_services, self.namespace)

        for service in services:
            obj = service.obj

            host = obj['spec'].get('clusterIP', None)
            service_type = obj['spec']['type']
            if service_type == 'LoadBalancer':
                ingress = obj['status'].get('loadBalancer', {}).get('ingress', [])
                hostname = ingress[0].get('hostname') if ingress else None
                if hostname:
                    host = hostname
            elif service_type == 'ExternalName':
                host = obj['spec']['externalName']

            entity = {
                'id': 'service-{}-{}[{}]'.format(service.name, service.namespace, self.cluster_id),
                'type': SERVICE_TYPE,
                'kube_cluster': self.cluster_id,
                'alias': self.alias,
                'environment': self.environment,
                'created_by': self.agent_type,
                'infrastructure_account': self.infrastructure_account,
                'region': self.region,

                'ip': obj['spec'].get('clusterIP', None),
                'host': host,
                'port': next(iter(obj['spec'].get('ports', [])), None),  # Assume first port is the used one.

                'service_name': service.name,
                'service_namespace': obj['metadata']['namespace'],
                'service_type': service_type,
                'service_ports': obj['spec'].get('ports', None),  # Could be useful when multiple ports are exposed.

                'endpoints_count': endpoints_map.get(service.name, 0),
            }

            entity.update(self.entity_labels(obj, 'labels', 'annotations'))

            yield entity

    def get_cluster_nodes(self, pod_entities=None):
        nodes = self.kube_client.get_nodes()

        if not pod_entities:
            self.logger.warning('No pods supplied, Nodes will not show pod count!')

        node_pod_count = {}
        for pod in pod_entities:
            if pod['type'] == POD_TYPE:
                name = pod.get('pod_node_name')
                if name:
                    node_pod_count[name] = node_pod_count.get(name, 0) + 1

        for node in nodes:
            obj = node.obj

            addresses = {address['type']: address['address'] for address in obj['status']['addresses']}
            ip = addresses.get('ExternalIP', addresses.get('InternalIP', ''))
            host = node.obj['metadata']['labels'].get('kubernetes.io/hostname', ip)
            instance_type = node.obj['metadata']['labels'].get(INSTANCE_TYPE_LABEL, '')
            statuses = {condition['type']: condition['status'] for condition in obj['status']['conditions']}

            entity = {
                'id': 'node-{}[{}]'.format(node.name, self.cluster_id),
                'type': NODE_TYPE,
                'kube_cluster': self.cluster_id,
                'alias': self.alias,
                'environment': self.environment,
                'created_by': self.agent_type,
                'infrastructure_account': self.infrastructure_account,
                'region': self.region,

                'ip': ip,
                'host': host,
                'external_ip': addresses.get('ExternalIP', ''),
                'internal_ip': addresses.get('InternalIP', ''),

                'node_name': node.name,
                'node_type': instance_type,
                'instance_type': instance_type,
                'pod_count': node_pod_count.get(node.name, 0),
                'pod_capacity': obj['status'].get('capacity', {}).get('pods', 0),
                'memory_capacity': obj['status'].get('capacity', {}).get('memory', 0),
                'pod_allocatable': obj['status'].get('allocatable', {}).get('pods', 0),
                'memory_allocatable': obj['status'].get('allocatable', {}).get('memory', 0),
                'image_count': len(obj['status'].get('images', [])),

                'container_runtime_version': obj['status']['nodeInfo']['containerRuntimeVersion'],
                'os_image': obj['status']['nodeInfo']['osImage'],
                'kernel_version': obj['status']['nodeInfo']['kernelVersion'],
                'kube_proxy_version': obj['status']['nodeInfo']['kubeProxyVersion'],
                'kubelet_version': obj['status']['nodeInfo']['kubeletVersion'],

                'node_ready': statuses.get('Ready', False),
                'node_out_of_disk': statuses.get('OutOfDisk', False),
                'node_memory_pressure': statuses.get('MemoryPressure', False),
                'node_disk_pressure': statuses.get('DiskPressure', False),
            }

            entity.update(self.entity_labels(obj, 'labels', 'annotations'))

            yield entity

    def get_cluster_replicasets(self):
        replicasets = self.get_all(self.kube_client.get_replicasets, self.namespace)

        for replicaset in replicasets:
            obj = replicaset.obj

            containers = obj['spec']['template']['spec']['containers']

            entity = {
                'id': 'replicaset-{}-{}[{}]'.format(replicaset.name, replicaset.namespace, self.cluster_id),
                'type': REPLICASET_TYPE,
                'kube_cluster': self.cluster_id,
                'alias': self.alias,
                'environment': self.environment,
                'created_by': self.agent_type,
                'infrastructure_account': self.infrastructure_account,
                'region': self.region,

                'replicaset_name': replicaset.name,
                'replicaset_namespace': obj['metadata']['namespace'],

                'containers': {c['name']: c.get('image', '') for c in containers if 'name' in c},

                'replicas': obj['spec'].get('replicas', 0),
                'ready_replicas': obj['status'].get('readyReplicas', 0),
            }

            entity.update(self.entity_labels(obj, 'labels', 'annotations'))

            yield entity

    def get_cluster_statefulsets(self):
        namespace = self.namespace if self.namespace else 'default'
        statefulsets = self.get_all(self.kube_client.get_statefulsets, namespace)

        for statefulset in statefulsets:
            obj = statefulset.obj

            # Stale replic set?!
            if obj['spec']['replicas'] == 0:
                continue

            containers = obj['spec'].get('template', {}).get('spec', {}).get('containers', [])

            entity = {
                'id': 'statefulset-{}-{}[{}]'.format(statefulset.name, statefulset.namespace, self.cluster_id),
                'type': STATEFULSET_TYPE,
                'kube_cluster': self.cluster_id,
                'alias': self.alias,
                'environment': self.environment,
                'created_by': self.agent_type,
                'infrastructure_account': self.infrastructure_account,
                'region': self.region,

                'statefulset_name': statefulset.name,
                'statefulset_namespace': obj['metadata']['namespace'],
                'statefulset_service_name': obj['spec']['serviceName'],

                'volume_claims': {
                    v['metadata']['name']: v['status'].get('phase', 'UNKNOWN')
                    for v in obj['spec'].get('volumeClaimTemplates', [])
                },
                'containers': {c['name']: c.get('image', '') for c in containers if 'name' in c},

                'replicas': obj['spec'].get('replicas'),
                'replicas_status': obj['status'].get('replicas'),
                'actual_replicas': obj['status'].get('readyReplicas'),
                'version': obj['metadata']['labels']['version']
            }

            entity.update(self.entity_labels(obj, 'labels', 'annotations'))

            yield entity

    def get_cluster_daemonsets(self):
        namespace = self.namespace if self.namespace else 'default'
        daemonsets = self.get_all(self.kube_client.get_daemonsets, namespace)

        for daemonset in daemonsets:
            obj = daemonset.obj

            containers = obj['spec']['template']['spec']['containers']

            entity = {
                'id': 'daemonset-{}-{}[{}]'.format(daemonset.name, daemonset.namespace, self.cluster_id),
                'type': DAEMONSET_TYPE,
                'kube_cluster': self.cluster_id,
                'alias': self.alias,
                'environment': self.environment,
                'created_by': self.agent_type,
                'infrastructure_account': self.infrastructure_account,
                'region': self.region,

                'daemonset_name': daemonset.name,
                'daemonset_namespace': obj['metadata']['namespace'],

                'containers': {c['name']: c.get('image', '') for c in containers if 'name' in c},

                'desired_count': obj['status'].get('desiredNumberScheduled', 0),
                'current_count': obj['status'].get('currentNumberScheduled', 0),
            }

            entity.update(self.entity_labels(obj, 'labels', 'annotations'))

            yield entity

    def get_cluster_ingresses(self):
        namespace = self.namespace if self.namespace else 'default'
        ingresses = self.get_all(self.kube_client.get_ingresses, namespace)

        for ingress in ingresses:
            obj = ingress.obj

            entity = {
                'id': 'ingress-{}-{}[{}]'.format(ingress.name, ingress.namespace, self.cluster_id),
                'type': INGRESS_TYPE,
                'kube_cluster': self.cluster_id,
                'alias': self.alias,
                'environment': self.environment,
                'created_by': self.agent_type,
                'infrastructure_account': self.infrastructure_account,
                'region': self.region,

                'ingress_name': ingress.name,
                'ingress_namespace': ingress.namespace,

                'ingress_rules': obj['spec'].get('rules', [])
            }

            entity.update(self.entity_labels(obj, 'labels'))

            yield entity
