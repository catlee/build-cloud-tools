#!/usr/bin/env python
import socket

import boto.vpc
import yaml

import logging
log = logging.getLogger(__name__)


def get_connection(region, aws_access_key_id=None, aws_secret_access_key=None):
    return boto.vpc.connect_to_region(
        region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )


def load_config(filename):
    return yaml.load(open(filename))


_dns_cache = {}
def resolve_host(hostname):
    if hostname in _dns_cache:
        return _dns_cache[hostname]
    log.info("resolving host %s", hostname)
    ip = socket.gethostbyname(hostname)
    _dns_cache[hostname] = ip
    return ip


def sync_tables(conn, my_tables, remote_tables):
    # Check that remote tables don't have overlapping names
    seen_names = set()
    for t in remote_tables[:]:
        name = t.tags.get('Name')
        if not name:
            log.warn("table %s has no name", t.id)
            remote_tables.remove(t)
            continue
        if name in seen_names:
            log.warn("table %s has a duplicate name %s; skipping", t.id, name)
            remote_tables.remove(t)
        seen_names.add(name)

    # TODO: Create tables that exist in my_tables but not in remote_tables

    # Sync remote tables
    for t in remote_tables:
        name = t.tags['Name']
        if name not in my_tables:
            if raw_input("table %s doesn't exist in local config; delete? (y/N)" % t.id) == 'y':
                log.warn("DELETING %s", t.id)
                # TODO
            continue

        my_t = my_tables[name]

        # Now look at routes
        remote_routes = set()
        for r in t.routes:
            remote_routes.add((r.destination_cidr_block, r.gateway_id, r.instance_id))

        my_routes = set()
        IGW = None
        VGW = None
        for cidr, dest in my_t['routes'].iteritems():
            if "/" not in cidr:
                cidr = resolve_host(cidr) + "/32"
            instance_id = None
            gateway_id = None
            if dest == "IGW":
                # Use our VPC's IGW
                if IGW is None:
                    IGW = conn.get_all_internet_gateways()[0]
                gateway_id = IGW.id
            elif dest == "VGW":
                # Use our VPC's IGW
                if VGW is None:
                    VGW = conn.get_all_vpn_gateways()[0]
                gateway_id = VGW.id
            elif dest == 'local':
                gateway_id = 'local'
            elif dest and dest.startswith("i-"):
                instance_id = dest
            my_routes.add((cidr, gateway_id, instance_id))

        # Delete extra routes first, in case we need to change the gateway of
        # some route
        extra_routes = remote_routes - my_routes
        for cidr, gateway_id, instance_id in extra_routes:
            log.info("%s - deleting route to %s via %s %s", t.id, cidr, gateway_id, instance_id)
            if raw_input("delete? (y/N) ") == 'y':
                conn.delete_route(t.id, cidr)

        # Add missing routes
        missing_routes = my_routes - remote_routes
        for cidr, gateway_id, instance_id in missing_routes:
            log.info("%s - adding route to %s via %s %s", t.id, cidr, gateway_id, instance_id)
            conn.create_route(t.id, cidr, gateway_id=gateway_id, instance_id=instance_id)

        # TODO: Set default, manage subnets


def main():
    log.debug("Parsing file")
    rt_defs = load_config('rt.txt')

    regions = set(rt_defs.keys())

    log.info("Working in regions %s", regions)

    for region in regions:
        log.info("Working in %s", region)
        conn = get_connection(region)
        remote_tables = conn.get_all_route_tables()

        # Compare vs. our configs
        my_tables = rt_defs[region]

        sync_tables(conn, my_tables, remote_tables)

if __name__ == '__main__':
    logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
    main()
