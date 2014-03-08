import os
import logging
import time
from boto.ec2 import connect_to_region
from boto.vpc import VPCConnection
from repoze.lru import lru_cache

log = logging.getLogger(__name__)
AMI_CONFIGS_DIR = os.path.join(os.path.dirname(__file__), "../../ami_configs")
INSTANCE_CONFIGS_DIR = os.path.join(os.path.dirname(__file__), "../../configs")


@lru_cache(10)
def get_aws_connection(region):
    """Connect to an EC2 region. Caches connection objects"""
    return connect_to_region(region)


@lru_cache(10)
def get_vpc(region):
    conn = get_aws_connection(region)
    return VPCConnection(region=conn.region)


def wait_for_status(obj, attr_name, attr_value, update_method):
    log.debug("waiting for %s availability", obj)
    while True:
        try:
            getattr(obj, update_method)()
            if getattr(obj, attr_name) == attr_value:
                break
            else:
                time.sleep(1)
        except:
            log.exception('hit error waiting')
            time.sleep(10)


def name_available(conn, name):
    res = conn.get_all_instances()
    instances = reduce(lambda a, b: a + b, [r.instances for r in res])
    names = [i.tags.get("Name") for i in instances if i.state != "terminated"]
    if name in names:
        return False
    else:
        return True
