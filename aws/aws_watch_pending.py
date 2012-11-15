#!/usr/bin/env python
"""
Watches pending jobs and starts or creates EC2 instances if required
"""
import re
import time

try:
    import simplejson as json
    assert json
except ImportError:
    import json

import boto.ec2
import sqlalchemy as sa

import logging
log = logging.getLogger()


def find_pending(db):
    engine = sa.create_engine(db)
    result = engine.execute(
        sa.text("""
        SELECT buildername, count(*) FROM
               buildrequests WHERE
               complete=0 AND
               claimed_at=0 AND
               submitted_at > :yesterday AND
               submitted_at < :toonew

               GROUP BY buildername"""),
        yesterday=time.time() - 86400,
        toonew=time.time() - 60,
    )
    retval = result.fetchall()
    return retval


# Used by aws_connect_to_region to cache connection objects per region
_aws_cached_connections = {}


def aws_connect_to_region(region, secrets):
    """Connect to an EC2 region. Caches connection objects"""
    if region in _aws_cached_connections:
        return _aws_cached_connections[region]
    conn = boto.ec2.connect_to_region(region, **secrets)
    _aws_cached_connections[region] = conn
    return conn


def aws_get_all_instances(regions, secrets):
    """
    Returns a list of all instances in the given regions
    """
    log.debug("fetching all instances for %s", regions)
    retval = []
    for region in regions:
        conn = aws_connect_to_region(region, secrets)
        reservations = conn.get_all_instances()
        for r in reservations:
            retval.extend(r.instances)
    return retval


def aws_filter_instances(all_instances, state=None, tags=None):
    retval = []
    for i in all_instances:
        matched = True
        if state and i.state != state:
            matched = False
            continue
        if tags:
            for k, v in tags.items():
                if i.tags.get(k) != v:
                    matched = False
                    continue
        if matched:
            retval.append(i)
    return retval


def aws_get_reservations(regions, secrets):
    """
    Return a mapping of (availability zone, ec2 instance type) -> count
    """
    log.debug("getting reservations for %s", regions)
    retval = {}
    for region in regions:
        conn = aws_connect_to_region(region, secrets)
        reservations = conn.get_all_reserved_instances(filters={
            'state': 'active',
        })
        for r in reservations:
            az = r.availability_zone
            ec2_instance_type = r.instance_type
            if (az, ec2_instance_type) not in retval:
                retval[az, ec2_instance_type] = 0
            retval[az, ec2_instance_type] += r.instance_count
    return retval


def aws_resume_instances(moz_instance_type, start_count, regions, secrets, dryrun):
    "resume up to `start_count` stopped instances of the given type in the given regions"
    instance_config = json.load(open("configs/%s" % moz_instance_type))
    max_running = instance_config.get('max_running')
    # Do a single request to get all instances here
    # We end up needing to look at most of them to figure out which instances
    # we can start
    all_instances = aws_get_all_instances(regions, secrets)
    # We'll filter by these tags
    tags = {'moz-state': 'ready', 'moz-type': moz_instance_type}
    if max_running is not None:
        running = len(aws_filter_instances(all_instances, state='running', tags=tags))
        if running + start_count > max_running:
            start_count = max_running - running
            if start_count <= 0:
                log.info("max_running limit hit (%s - %i)", moz_instance_type, max_running)
                return 0

    # Get our list of stopped instances, sorted by launch_time
    stopped_instances = aws_filter_instances(all_instances, state='stopped', tags=tags)
    stopped_instances.sort(key=lambda i: i.launch_time)
    stopped_instances.reverse()
    log.debug("stopped_instances: %s", stopped_instances)

    # Get our current reservations
    reservations = aws_get_reservations(regions, secrets)
    # XXX: For debugging - add a fake reservation we can't use to make sure it
    # gets filtered out later
    reservations['us-west-1b', 'm1.large'] = 2
    reservations['us-west-1a', 'c1.xlarge'] = 500
    log.debug("current reservations: %s", reservations)

    # Get our currently running instances
    running = aws_filter_instances(all_instances, state='running')
    # Subtract running instances from our reservations
    for i in running:
        az = i.placement
        ec2_instance_type = i.instance_type
        if (az, ec2_instance_type) in reservations:
            reservations[az, ec2_instance_type] -= 1

    log.debug("available reservations: %s", reservations)
    # Remove reservations that are used up
    for k, count in reservations.items():
        if count <= 0:
            log.debug("all reservations for %s are used; removing", k)
            del reservations[k]

    # Remove reservations that we can't use right now (no stopped instances in
    # the right zones or of the right type)
    stopped_kinds = set((i.placement, i.instance_type) for i in stopped_instances)
    reserved_kinds = set(reservations.keys())
    impossible_kinds = reserved_kinds - stopped_kinds
    log.debug("impossible to use reservations for %s; removing", impossible_kinds)
    for k in impossible_kinds:
        del reservations[k]

    log.debug("filtered reservations: %s", reservations)

    # List of (instance,is_reserved) tuples
    to_start = []

    # While we still have reservations, start instances that can use those
    # reservations first
    for i in stopped_instances[:]:
        k = (i.placement, i.instance_type)
        if k not in reservations:
            continue
        stopped_instances.remove(i)
        to_start.append((i, True))
        reservations[k] -= 1
        if reservations[k] <= 0:
            del reservations[k]

    # Add the rest
    to_start.extend((i, False) for i in stopped_instances)

    # Limit ourselves to start only start_count instances
    log.debug("starting up to %i instances", start_count)
    to_start = to_start[:start_count]

    log.debug("to_start: %s", to_start)

    started = 0
    for i, is_reserved in to_start:
        r = "reserved instance" if is_reserved else "instance"
        if not dryrun:
            log.info("%s - %s - starting %s", i.placement, i.tags['Name'], r)
            i.start()
        else:
            log.info("%s - %s - would start %s", i.placement, i.tags['Name'], r)
        started += 1

    assert started == len(to_start)
    return started


def aws_watch_pending(db, regions, secrets, key_name, builder_map, dryrun):
    # First find pending jobs in the db
    pending = find_pending(db)

    # Mapping of instance types to # of instances we want to creates
    to_create = {}
    # Then match them to the builder_map
    for pending_buildername, count in pending:
        for buildername_exp, instance_type in builder_map.items():
            if re.match(buildername_exp, pending_buildername):
                log.debug("%s has %i pending jobs, checking instances of type %s", pending_buildername, count, instance_type)
                to_create[instance_type] = to_create.get(instance_type, 0) + count

                break
        else:
            log.debug("%s has %i pending jobs, but no instance types defined", pending_buildername, count)

    for instance_type, count in to_create.items():
        log.debug("need %i %s", count, instance_type)

        # Check for stopped instances in the given regions and start them if there are any
        started = aws_resume_instances(instance_type, count, regions, secrets, dryrun)
        count -= started
        log.info("%s - started %i instances; need %i", instance_type, started, count)

if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser()
    parser.set_defaults(
        regions=[],
        secrets=None,
        loglevel=logging.INFO,
        key_name=None,
        config=None,
        dryrun=False,
    )

    parser.add_option("-r", "--region", action="append", dest="regions")
    parser.add_option("-k", "--secrets", dest="secrets")
    parser.add_option("-s", "--key-name", dest="key_name")
    parser.add_option("-v", "--verbose", action="store_const", dest="loglevel", const=logging.DEBUG)
    parser.add_option("-c", "--config", dest="config")
    parser.add_option("-n", "--dryrun", dest="dryrun", action="store_true", help="don't actually do anything")

    options, args = parser.parse_args()

    logging.basicConfig(level=options.loglevel, format="%(asctime)s - %(message)s")
    logging.getLogger("boto").setLevel(logging.INFO)

    if not options.regions:
        parser.error("at least one region is required")

    if not options.secrets:
        parser.error("secrets are required")

    if not options.config:
        parser.error("you must specify a config file to use")

    config = json.load(open(options.config))
    secrets = json.load(open(options.secrets))

    print aws_resume_instances('bld-linux64', 5, options.regions, secrets, dryrun=True)

    raise SystemExit()
    aws_watch_pending(
        config['db'],
        options.regions,
        secrets,
        options.key_name,
        config['buildermap'],
        options.dryrun,
    )
