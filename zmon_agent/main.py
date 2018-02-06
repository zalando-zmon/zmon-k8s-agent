import os
import sys
import time
import argparse
import logging
import json

import requests
import tokens

from zmon_cli.client import Zmon, compare_entities

AGENT_TYPE = 'zmon-agent'

logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler(stream=sys.stdout))
logger.setLevel(logging.INFO)


def load_module(name):
    mod = __import__(name)
    parts = name.split('.')
    for comp in parts[1:]:
        mod = getattr(mod, comp)
    return mod


def get_clients(zmon_url, verify=True) -> Zmon:
    """Return Zmon client instance"""

    # Get token if set as ENV variable. This is useful in development.
    zmon_token = os.getenv('ZMON_AGENT_TOKEN')

    if not zmon_token:
        zmon_token = tokens.get('uid')

    return Zmon(zmon_url, token=zmon_token, verify=verify)


def get_existing_ids(existing_entities):
    return [entity['id'] for entity in existing_entities]


def remove_missing_entities(existing_ids, current_ids, zmon_client, dry_run=False):
    to_be_removed_ids = list(set(existing_ids) - set(current_ids))

    error_count = 0

    if not dry_run:
        logger.info('Removing {} entities from ZMon'.format(len(to_be_removed_ids)))
        for entity_id in to_be_removed_ids:
            logger.info('Removing entity with id: {}'.format(entity_id))
            deleted = zmon_client.delete_entity(entity_id)
            if not deleted:
                logger.info('Failed to delete entity!')
                error_count += 1

    return to_be_removed_ids, error_count


def new_or_updated_entity(entity, existing_entities_dict):
    # check if new entity
    if entity['id'] not in existing_entities_dict:
        return True

    existing_entities_dict[entity['id']].pop('last_modified', None)

    return not compare_entities(entity, existing_entities_dict[entity['id']])


def add_new_entities(all_current_entities, existing_entities, zmon_client, dry_run=False):
    existing_entities_dict = {e['id']: e for e in existing_entities}
    new_entities = [e for e in all_current_entities if new_or_updated_entity(e, existing_entities_dict)]

    error_count = 0

    if not dry_run:
        try:
            logger.info('Found {} new entities to be added in ZMon'.format(len(new_entities)))
            for entity in new_entities:
                logger.info(
                    'Adding new {} entity with ID: {}'.format(entity['type'], entity['id'])
                )

                resp = zmon_client.add_entity(entity)

                resp.raise_for_status()
        except Exception:
            logger.exception('Failed to add entity!')
            error_count += 1

    return new_entities, error_count


def sync(plugins, infrastructure_account, region, entity_service, verify, dry_run, interval):

    run_list = [p for p in plugins if not p['depends']]
    providing = []
    remaining = []
    for p in run_list:
        providing.extend(p['provides'])

    for p in plugins:
        if not p['depends']:
            continue
        if all_in(p['depends'], providing):
            run_list.append(p)
            providing.append(p['provides'])
        else:
            remaining.append(p)

    lost = []
    for p in remaining:
        if all_in(p['depends'], providing):
            run_list.append(p)
            providing.append(p['provides'])
        else:
            lost.append(p)

    if lost:
        # Do we want to load from ZMON with
        # {
        #  'type': dependency,
        #  'region': self.region,
        #  'infrastructure_account': self.infrastructure_account,
        # }
        # as fallback?
        raise Exception('dependencies not provided for plugins: {}'.format(
                        [p['plugin'] for p in lost]))

    while True:
        entities = {}
        for plugin in run_list:
            try:
                dependencies = {}
                for name in plugin['depends']:
                    typed_entities = {k: v for k, v in entities if v['type'] == name}
                    dependencies.update(typed_entities)
                discovered = run_sync(plugin, dependencies, infrastructure_account, region,
                                      entity_service, verify, dry_run, interval)
                for entity in discovered:
                    entities[entity] = discovered[entity]
            except KeyboardInterrupt:
                logger.error("FIXME: KeyboardInterrupt")
                raise
            except Exception:
                logger.exception('failed to run plugin {}'.format(plugin['plugin']))

        if not dry_run:
            s = interval if interval else 60
            time.sleep(s)


def all_in(wanted, given):
    for w in wanted:
        if w not in given:
            return False
    return True


def run_sync(plugin, dependencies, infrastructure_account, region, entity_service, verify, dry_run, interval):
    zmon_client = get_clients(entity_service, verify=verify)

    discovery = plugin['discovery']
    account_entity = discovery.account_entity()

    all_current_entities = discovery.entities(dependencies) + [account_entity]

    # ZMON entities
    existing_entities = []
    for query in discovery.filter_queries():
        existing_entities.extend(zmon_client.get_entities(query=query))

    # Remove non-existing entities
    existing_ids = get_existing_ids(existing_entities)

    current_ids = [entity['id'] for entity in all_current_entities]

    to_be_removed_ids, delete_err = remove_missing_entities(
        existing_ids, current_ids, zmon_client, dry_run=dry_run)

    # Add new entities
    new_entities, add_err = add_new_entities(
        all_current_entities, existing_entities, zmon_client, dry_run=dry_run)

    logger.info('{}: Found {} new entities from {} entities ({} failed)'.format(
        plugin['plugin'], len(new_entities), len(all_current_entities), add_err))

    # Add account entity - always!
    if not dry_run:
        try:
            account_entity['errors'] = {'delete_count': delete_err, 'add_count': add_err}
            zmon_client.add_entity(account_entity)
        except Exception:
            logger.exception('Failed to add account entity!')

    logger.info(
        'ZMON agent plugin {} completed sync with {} addition errors and {} deletion errors'.format(
            plugin['plugin'], add_err, delete_err))

    if dry_run:
        output = {
            'to_be_removed_ids': to_be_removed_ids,
            'new_entities': new_entities
        }

        print(json.dumps(output, indent=4))

    return all_current_entities


def main():
    argp = argparse.ArgumentParser(description='ZMON Agent')

    argp.add_argument('-i', '--infrastructure-account', dest='infrastructure_account', default=None,
                      help='Infrastructure account which identifies this agent. Can be set via  '
                           'ZMON_AGENT_INFRASTRUCTURE_ACCOUNT env variable.')
    argp.add_argument('-r', '--region', dest='region',
                      help='Infrastructure Account Region. Can be set via ZMON_AGENT_REGION env variable.')

    argp.add_argument('-e', '--entity-service', dest='entity_service',
                      help='ZMON backend URL. Can be set via ZMON_AGENT_ENTITY_SERVICE_URL env variable.')

    argp.add_argument('--interval', dest='interval',
                      help='Interval for agent sync. If not set then agent will run once. Can be set via '
                      'ZMON_AGENT_INTERVAL env variable.')

    argp.add_argument('-j', '--json', dest='json', action='store_true', help='Print JSON output only.', default=False)
    argp.add_argument('--skip-ssl', dest='skip_ssl', action='store_true', default=False)
    argp.add_argument('-v', '--verbose', dest='verbose', action='store_true', default=False, help='Verbose output.')

    argp.add_argument('-m', '--modules', dest='mods', help='Python module(s) to load for discovery. Can be set via '
                      'ZMON_AGENT_MODULES env variable. Space separated list.')

    args = argp.parse_args()

    # Hard requirements
    infrastructure_account = (args.infrastructure_account if args.infrastructure_account else
                              os.environ.get('ZMON_AGENT_INFRASTRUCTURE_ACCOUNT'))
    if not infrastructure_account:
        raise RuntimeError('Cannot determine infrastructure account. Please use --infrastructure-account option or '
                           'set env variable ZMON_AGENT_INFRASTRUCTURE_ACCOUNT.')

    region = os.environ.get('ZMON_AGENT_REGION', args.region)
    entity_service = os.environ.get('ZMON_AGENT_ENTITY_SERVICE_URL', args.entity_service)
    interval = os.environ.get('ZMON_AGENT_INTERVAL', args.interval)

    if interval:
        interval = int(interval)

    # OAUTH2 tokens
    tokens.configure()
    tokens.manage('uid', ['uid'])

    verbose = args.verbose if args.verbose else os.environ.get('ZMON_AGENT_DEBUG', False)
    if verbose:
        logger.setLevel(logging.DEBUG)

    verify = True
    if args.skip_ssl:
        logger.warning('ZMON agent will skip SSL verification!')
        verify = False

    if not region:
        # Assuming running on AWS
        logger.info('Trying to figure out region ...')
        try:
            response = requests.get('http://169.254.169.254/latest/meta-data/placement/availability-zone', timeout=2)

            response.raise_for_status()

            region = response.text[:-1]
        except Exception:
            logger.error('AWS region was not specified and can not be fetched from instance meta-data!')
            raise

    mods = (args.mods if args.mods else os.environ.get('ZMON_AGENT_MODULES'))
    if not mods:
        raise RuntimeError('no module given for discovery')

    plugins = {}
    for mod in mods.split():
        try:
            module = load_module(mod)
            cls = module.agent_class()
            discovery = cls(region, infrastructure_account)
            plugins[mod] = {
                'plugin': module,
                'discovery': discovery,
                'provides': discovery.provides(),
                'depends': discovery.requires(),
            }
        except Exception:
            logger.error('Failed to load module {}'.format(args.mod))
            raise
    sync(plugins, infrastructure_account, region, entity_service, verify, args.json, interval)


if __name__ == '__main__':
    main()
