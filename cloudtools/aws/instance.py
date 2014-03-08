import logging
import time
import random

from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType
from boto.ec2.networkinterface import NetworkInterfaceSpecification, \
    NetworkInterfaceCollection

from fabric.api import run, env, sudo
from . import wait_for_status
from cloudtools.aws.vpc import get_subnets_by_name

log = logging.getLogger(__name__)


def run_instance(connection, instance_name, config, key_name, user='root',
                 subnet_id=None):
    bdm = None
    if 'device_map' in config:
        bdm = BlockDeviceMapping()
        for device, device_info in config['device_map'].items():
            bd = BlockDeviceType()
            if device_info.get('size'):
                bd.size = device_info['size']
            # Overwrite root device size for HVM instances, since they cannot
            # be resized online
            if device_info.get("delete_on_termination") is not False:
                bd.delete_on_termination = True
            if device_info.get("ephemeral_name"):
                bd.ephemeral_name = device_info["ephemeral_name"]

            bdm[device] = bd

    if not subnet_id and config.get('subnet_name'):
        log.info("looking for subnets called %s in %s", config['subnet_name'], connection.region.name)
        subnet_id = random.choice([s.id for s in get_subnets_by_name(connection.region.name, config['subnet_name'])])

    interface = NetworkInterfaceSpecification(
        subnet_id=subnet_id,
        private_ip_address=config.get('ip_address'),
        delete_on_termination=True,
        groups=config.get('security_group_ids', []),
        associate_public_ip_address=config.get("use_public_ip", True)
    )
    interfaces = NetworkInterfaceCollection(interface)
    reservation = connection.run_instances(
        image_id=config['ami'],
        key_name=key_name,
        instance_type=config['instance_type'],
        block_device_map=bdm,
        #client_token=str(uuid.uuid4())[:16],
        #subnet_id=subnet_id,
        network_interfaces=interfaces,
        instance_profile_name=config.get("instance_profile_name"),
        disable_api_termination=bool(config.get('disable_api_termination')),
    )

    instance = reservation.instances[0]
    log.info("instance %s created, waiting to come up", instance)
    instance.add_tag('Name', instance_name)
    # Wait for the instance to come up
    wait_for_status(instance, "state", "running", "update")
    if subnet_id:
        env.host_string = instance.private_ip_address
    else:
        env.host_string = instance.public_dns_name
    env.user = user
    env.abort_on_prompts = True
    env.disable_known_hosts = True

    # wait until the instance is responsive
    while True:
        try:
            if run('date').succeeded:
                break
        except:
            log.debug('hit error waiting for instance to come up')
        time.sleep(10)

    # Overwrite root's limited authorized_keys so we can login as root and not
    # have to do sudo for everything
    if user != 'root':
        sudo("cp -f ~%s/.ssh/authorized_keys "
             "/root/.ssh/authorized_keys" % user)
        sudo("sed -i -e '/PermitRootLogin/d' "
             "-e '$ a PermitRootLogin without-password' /etc/ssh/sshd_config")
        sudo("service sshd restart || service ssh restart")
        sudo("sleep 20")
    return instance


def get_instance(connection, instance_id):
    return connection.get_only_instances(instance_ids=[instance_id])[0]
