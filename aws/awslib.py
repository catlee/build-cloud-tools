import boto.ec2

import logging
log = logging.getLogger()

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
