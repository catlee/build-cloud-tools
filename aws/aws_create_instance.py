#!/usr/bin/env python
import json
import uuid
import time
import boto
import StringIO

from random import choice
from fabric.api import run, put, env, sudo, settings, local
from fabric.context_managers import cd
from boto.ec2 import connect_to_region
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType

import logging
log = logging.getLogger()


def assimilate(ip_addr, config, instance_data, create_ami):
    """Assimilate hostname into our collective

    What this means is that hostname will be set up with some basic things like
    a script to grab AWS user data, and get it talking to puppet (which is
    specified in said config).
    """
    env.host_string = ip_addr
    env.user = 'root'
    env.abort_on_prompts = True
    env.disable_known_hosts = True

    # Sanity check
    run("date")

    distro = config.get('distro')
    # Set our hostname
    hostname = "{hostname}".format(**instance_data)
    run("hostname %s" % hostname)
    if distro in ('ubuntu', 'debian'):
        run("echo %s > /etc/hostname" % hostname)

    # Resize the file systems
    # We do this because the AMI image usually has a smaller filesystem than
    # the instance has.
    if 'device_map' in config:
        for mapping in config['device_map'].values():
            run('resize2fs {dev}'.format(dev=mapping['instance_dev']))

    # Set up /etc/hosts to talk to 'puppet'
    hosts = ['127.0.0.1 localhost.localdomain localhost %s' % hostname,
             '::1 localhost6.localdomain6 localhost6'] + \
            ["%s %s" % (ip, host) for host, ip in
             instance_data['hosts'].iteritems()]
    hosts = StringIO.StringIO("\n".join(hosts) + "\n")
    put(hosts, '/etc/hosts')

    if distro in ('ubuntu', 'debian'):
        put('releng.list', '/etc/apt/sources.list')
        run("apt-get update")
        run("apt-get install -y --allow-unauthenticated puppet")
        run("apt-get clean")
    else:
        # Set up yum repos
        run('rm -f /etc/yum.repos.d/*')
        put('releng-public.repo', '/etc/yum.repos.d/releng-public.repo')
        run('yum clean all')

        # Get puppet installed
        run('yum install -q -y puppet')

    # /var/lib/puppet skel
    run("test -d /var/lib/puppet/ssl || mkdir -m 771 /var/lib/puppet/ssl")
    run("test -d /var/lib/puppet/ssl/ca || mkdir -m 755 /var/lib/puppet/ssl/ca")
    run("test -d /var/lib/puppet/ssl/certs || mkdir -m 755 /var/lib/puppet/ssl/certs")
    run("test -d /var/lib/puppet/ssl/public_keys || mkdir -m 755 /var/lib/puppet/ssl/public_keys")
    run("test -d /var/lib/puppet/ssl/private_keys || mkdir -m 750 /var/lib/puppet/ssl/private_keys")
    run("chown puppet:root /var/lib/puppet/ssl /var/lib/puppet/ssl/ca "
        "/var/lib/puppet/ssl/certs /var/lib/puppet/ssl/public_keys "
        "/var/lib/puppet/ssl/private_keys")

    # generate certs
    local("test -d certs.{h} || (mkdir certs.{h} && "
          "./ca-scripts/generate-cert.sh {h} certs.{h})".format(h=hostname))

    # cleanup
    run('find /var/lib/puppet/ssl -type f -delete')

    # put files to puppet dirs
    put("certs.%s/ca_crt.pem" % hostname, "/var/lib/puppet/ssl/certs/ca.pem",
        mode=0644)
    put("certs.{h}/{h}.crt".format(h=hostname),
        "/var/lib/puppet/ssl/certs/%s.pem" % hostname, mode=0644)
    put("certs.{h}/{h}.key".format(h=hostname),
        "/var/lib/puppet/ssl/private_keys/%s.pem" % hostname, mode=0600)

    # Run puppet
    # We need --detailed-exitcodes here otherwise puppet will return 0
    # sometimes when it fails to install dependencies
    with settings(warn_only=True):
        result = run("puppetd --onetime --no-daemonize --verbose "
                     "--detailed-exitcodes --waitforcert 10 "
                     "--server {puppet}".format(
                     puppet=instance_data['default_puppet_server']))
        assert result.return_code in (0, 2)

    if create_ami:
        run('find /var/lib/puppet/ssl -type f -delete')
        return

    if 'home_tarball' in instance_data:
        put(instance_data['home_tarball'], '/tmp/home.tar.gz')
        with cd('~cltbld'):
            sudo('tar xzf /tmp/home.tar.gz', user="cltbld")
            sudo('chmod 700 .ssh', user="cltbld")
            sudo('chmod 600 .ssh/*', user="cltbld")
        run('rm -f /tmp/home.tar.gz')

    # Set up a stub buildbot.tac
    sudo("/tools/buildbot/bin/buildslave create-slave /builds/slave {buildbot_master} {name} {buildslave_password}".format(**instance_data), user="cltbld")

    hg = "/tools/python27-mercurial/bin/hg"
    for share, bundle in instance_data['hg_shares'].iteritems():
        target_dir = '/builds/hg-shared/%s' % share
        sudo('rm -rf {d} && mkdir -p {d}'.format(d=target_dir), user="cltbld")
        sudo('{hg} init {d}'.format(hg=hg, d=target_dir), user="cltbld")
        hgrc = "[path]\n"
        hgrc += "default = http://hg.mozilla.org/%s\n" % share
        put(StringIO.StringIO(hgrc), '%s/.hg/hgrc' % target_dir)
        run("chown cltbld: %s/.hg/hgrc" % target_dir)
        sudo('{hg} -R {d} unbundle {b}'.format(hg=hg, d=target_dir, b=bundle),
             user="cltbld")

    run("reboot")


def create_instance(name, config, region, secrets, key_name, instance_data,
                    create_ami=False):
    """Creates an AMI instance with the given name and config. The config must
    specify things like ami id."""
    conn = connect_to_region(
        region,
        aws_access_key_id=secrets['aws_access_key_id'],
        aws_secret_access_key=secrets['aws_secret_access_key'],
    )

    # Make sure we don't request the same things twice
    token = str(uuid.uuid4())[:16]

    instance_data = instance_data.copy()
    instance_data['name'] = name
    instance_data['hostname'] = '{name}.build.aws-{region}.mozilla.com'.format(
        name=name, region=region)

    bdm = None
    if 'device_map' in config:
        bdm = BlockDeviceMapping()
        for device, device_info in config['device_map'].items():
            bdm[device] = BlockDeviceType(size=device_info['size'],
                                          delete_on_termination=True)

    subnet_id = config.get('subnet_id')
    if subnet_id:
        if isinstance(subnet_id, (list, tuple)):
            subnet_id = choice(subnet_id)

    reservation = conn.run_instances(
        image_id=config['ami'],
        key_name=key_name,
        instance_type=config['instance_type'],
        block_device_map=bdm,
        client_token=token,
        subnet_id=subnet_id,
    )

    instance = reservation.instances[0]
    log.info("instance %s created, waiting to come up", instance)
    # Wait for the instance to come up
    while True:
        try:
            instance.update()
            if instance.state == 'running':
                break
        except:
            log.exception("hit error waiting for instance to come up")
        time.sleep(10)

    instance.add_tag('Name', name)
    instance.add_tag('moz-type', config['type'])

    log.info("assimilating %s", instance)
    instance.add_tag('moz-state', 'pending')
    while True:
        try:
            if instance.subnet_id:
                assimilate(instance.private_ip_address, config, instance_data,
                           create_ami)
            else:
                assimilate(instance.public_dns_name, config, instance_data,
                           create_ami)
            break
        except:
            log.exception("problem assimilating %s", instance)
            time.sleep(10)
    if not create_ami:
        instance.add_tag('moz-state', 'ready')
    else:
        ami_from_instance(instance)


def ami_from_instance(instance):
    base_ami = instance.connection.get_image(instance.image_id)
    target_name = '%s-puppetized' % base_ami.name
    v = instance.connection.get_all_volumes(
        filters={'attachment.instance-id': instance.id})[0]
    instance.stop()
    log.info('Stopping instance')
    while True:
        try:
            instance.update()
            if instance.state == 'stopped':
                break
        except:
            log.info('Waiting for instance stop')
            time.sleep(10)
    log.info('Creating snapshot')
    snapshot = v.create_snapshot('EBS-backed %s' % target_name)
    while True:
        try:
            snapshot.update()
            if snapshot.status == 'completed':
                break
        except:
            log.exception('hit error waiting for snapshot to be taken')
            time.sleep(10)
    snapshot.add_tag('Name', target_name)

    log.info('Creating AMI')
    block_map = BlockDeviceMapping()
    block_map[base_ami.root_device_name] = BlockDeviceType(
        snapshot_id=snapshot.id)
    ami_id = instance.connection.register_image(
        target_name,
        '%s EBS AMI' % target_name,
        architecture=base_ami.architecture,
        kernel_id=base_ami.kernel_id,
        ramdisk_id=base_ami.ramdisk_id,
        root_device_name=base_ami.root_device_name,
        block_device_map=block_map,
    )
    while True:
        try:
            ami = instance.connection.get_image(ami_id)
            ami.add_tag('Name', target_name)
            log.info('AMI created')
            log.info('ID: {id}, name: {name}'.format(id=ami.id, name=ami.name))
            break
        except boto.exception.EC2ResponseError:
            log.info('Wating for AMI')
            time.sleep(10)
    instance.terminate()

import multiprocessing
import sys


class LoggingProcess(multiprocessing.Process):
    def __init__(self, log, *args, **kwargs):
        self.log = log
        super(LoggingProcess, self).__init__(*args, **kwargs)

    def run(self):
        output = open(self.log, 'wb', 0)
        logging.basicConfig(stream=output)
        sys.stdout = output
        sys.stderr = output
        return super(LoggingProcess, self).run()


def make_instances(names, config, region, secrets, key_name, instance_data,
                   create_ami):
    """Create instances for each name of names for the given configuration"""
    procs = []
    for name in names:
        p = LoggingProcess(log="{name}.log".format(name=name),
                           target=create_instance,
                           args=(name, config, region, secrets, key_name,
                                 instance_data, create_ami),
                           )
        p.start()
        procs.append(p)

    log.info("waiting for workers")
    for p in procs:
        p.join()


if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser()
    parser.set_defaults(
        config=None,
        region="us-west-1",
        secrets=None,
        key_name=None,
        action="create",
        create_ami=False,
        instance_id=None,
        instance_data=None,
    )
    parser.add_option("-c", "--config", dest="config", help="instance configuration to use")
    parser.add_option("-r", "--region", dest="region", help="region to use")
    parser.add_option("-k", "--secrets", dest="secrets", help="file where secrets can be found")
    parser.add_option("-s", "--key-name", dest="key_name", help="SSH key name")
    parser.add_option("-l", "--list", dest="action", action="store_const", const="list", help="list available configs")
    parser.add_option("--instance_id", dest="instance_id", help="assimilate existing instance")
    parser.add_option("-i", "--instance-data", dest="instance_data", help="instance specific data")
    parser.add_option("--create-ami", dest="create_ami", action="store_true",
                      help="create AMI from instance")

    options, args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    if not args:
        parser.error("at least one instance name is required")

    if not options.config:
        parser.error("config name is required")

    if not options.secrets:
        parser.error("secrets are required")

    if not options.instance_data:
        parser.error("instance data is required")

    if not options.key_name:
        parser.error("SSH key name name is required")

    try:
        config = json.load(open(options.config))[options.region]
    except KeyError:
        parser.error("unknown configuration")

    secrets = json.load(open(options.secrets))

    instance_data = json.load(open(options.instance_data))
    if options.instance_id:
        conn = connect_to_region(
            options.region,
            aws_access_key_id=secrets['aws_access_key_id'],
            aws_secret_access_key=secrets['aws_secret_access_key'],
        )
        instance = conn.get_all_instances([options.instance_id])[0].instances[0]
        instance_data['name'] = args[0]
        instance_data['hostname'] = '{name}.build.aws-{region}.mozilla.com'.format(
            name=args[0], region=options.region)
        assimilate(instance.private_ip_address, config, instance_data, False)
    else:
        make_instances(args, config, options.region, secrets, options.key_name,
                       instance_data, options.create_ami)
