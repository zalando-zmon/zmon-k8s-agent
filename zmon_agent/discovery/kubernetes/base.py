"""BaseDiscovery class used by agent core"""

import os
import sys
import logging

from . import kube


AGENT_TYPE = 'zmon-kubernetes-agent'

INSTANCE_TYPE_LABEL = 'beta.kubernetes.io/instance-type'

PROTECTED_FIELDS = set(('id', 'type', 'infrastructure_account', 'created_by', 'region'))

# FIXME - unused ...
SERVICE_ACCOUNT_PATH = '/var/run/secrets/kubernetes.io/serviceaccount'

SKIPPED_ANNOTATIONS = set(('kubernetes.io/created-by'))


class BaseDiscovery:

    def __init__(self, region, infrastructure_account):
        logger = logging.getLogger(AGENT_TYPE)
        logger.addHandler(logging.StreamHandler(stream=sys.stdout))
        logger.setLevel(logging.INFO)
        self.logger = logger
        self.namespace = os.environ.get('ZMON_AGENT_KUBERNETES_NAMESPACE')
        self.cluster_id = os.environ.get('ZMON_AGENT_KUBERNETES_CLUSTER_ID')
        self.alias = os.environ.get('ZMON_AGENT_KUBERNETES_CLUSTER_ALIAS', '')
        self.environment = os.environ.get('ZMON_AGENT_KUBERNETES_CLUSTER_ENVIRONMENT', '')

        config_path = os.environ.get('ZMON_AGENT_KUBERNETES_CONFIG_PATH')
        self.kube_client = kube.Client(config_file_path=config_path)

        self.region = region
        self.infrastructure_account = infrastructure_account
        self.agent_type = AGENT_TYPE

    def init(self):
        pass

    def requires(self):
        raise RuntimeError('Base class not overwritten')

    def provides(self):
        raise RuntimeError('Base class not overwritten')

    def filter_queries(self):
        raise RuntimeError('Base class not overwritten')

    def account_entity(self):
        entity = {
            'type': 'local',
            'infrastructure_account': self.infrastructure_account,
            'region': self.region,
            'kube_cluster': self.cluster_id,
            'alias': self.alias,
            'environment': self.environment,
            'id': 'kube-cluster[{}:{}]'.format(self.infrastructure_account, self.region),
            'created_by': AGENT_TYPE,
        }

        return entity

    def entities(self, dependencies):
        raise RuntimeError('Base class not overwritten')

    def get_all(self, kube_func, namespace):
        items = []

        namespaces = [namespace] if namespace else [ns.name for ns in self.kube_client.get_namespaces()]

        for ns in namespaces:
            items += list(kube_func(namespace=ns))

        return items
