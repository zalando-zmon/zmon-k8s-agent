===============
ZMON AGENT CORE
===============

.. image:: https://travis-ci.org/zalando-zmon/zmon-agent-core.svg?branch=master
    :target: https://travis-ci.org/zalando-zmon/zmon-agent-core


**WIP**

ZMON agent core for infrastructure discovery.

The agent loads the python modules for discovery dynamically. The module to use
must be given with the ``-m`` / ``--modules`` command line flag or the ``ZMON_AGENT_MODULES``
environment variable.

To be able to use a module for discovery, the module **must** provide a function named
``agent_class`` which returns the class to be instantiated. The constructor
receives two arguments: the region and infrastructure account.

The discovery object itself must provide:
* account_entity() - the ``type=local`` entity for this discovery. This entity must at least
  contain the follwing keys
  * ``type`` with the value ``local``
  * ``infrastructure_account`` with the value passed to the constructor
  * ``region`` with the value passed to the constructor
  * ``id`` a unique identifier for this local entity
  * ``created_by`` should be unique and describe the module, e.g. for a kubernetes discovery it could be
      ``zmon-kubernetes-agent``
* filter_queries() - the zmon filter to get the current list of entities supported by this
  module
* provides() - a list of entity types which the module discovers
* requires() - a list of entity types discovered by other module(s) which are required to return
  complete entity information
* entities() - the actual discovery. It must return all currently available entities it discovered. It will
  receive a dict of ``id: entity`` with all entities of the types it set in requires().

Discovery agents shipped with this core agent:

* ``zmon_agent.discovery.kubernetes`` - kubernetes cluster discovery, discovers entities of types
  * ``kube_pod``
  * ``kube_pod_container``
  * ``kube_service``
  * ``kube_node``
  * ``kube_replicaset`` - to be determined if supported in the future
  * ``kube_statefulset``
  * ``kube_daemonset``
  * ``kube_ingress``
* ``zmon_agent.discovery.postgres`` - postgresql db discovery in k8s. Requires ``kube_statefulset``,
  discovers entities of types
  * ``postgresql_cluster``
  * ``postgresql_cluster_member``
  * ``postgresql_database``
  * ``postgresql_database_replica``

Supports:

- Kubernetes discovery
- Kubernetes Spilo Postgresql clusters/databases
