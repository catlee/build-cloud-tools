#!/usr/bin/env python
import json
import uuid
import time
import boto
import StringIO
from socket import gethostbyname, gaierror

from random import choice
from fabric.api import run, put, env, sudo, settings, local
from fabric.context_managers import cd
from boto.ec2 import connect_to_region
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType
from boto.vpc import VPCConnection
from IPy import IP

import logging
log = logging.getLogger()


def get_ip(hostname):
    try:
        return gethostbyname(hostname)
    except gaierror:
        return None


def get_subnet_id(vpc, ip):
    subnets = vpc.get_all_subnets()
    for s in subnets:
        if IP(ip) in IP(s.cidr_block):
            return s.id
    return None


def ip_available(conn, ip):
    res = conn.get_all_instances()
    instances = reduce(lambda a, b: a + b, [r.instances for r in res])
    ips = [i.private_ip_address for i in instances]
    if ip in ips:
        return False
    else:
        return True


def assimilate_windows(ip_addr, config, instance_data):
    # Get puppet and ssh working
    pass


def assimilate(ip_addr, config, instance_data):
    """Assimilate hostname into our collective

    What this means is that hostname will be set up with some basic things like
    a script to grab AWS user data, and get it talking to puppet (which is
    specified in said config).
    """
    distro = config.get('distro')
    if distro.startswith('win'):
        return assimilate_windows(ip_addr, config, instance_data)

    env.host_string = ip_addr
    env.user = 'root'
    env.abort_on_prompts = True
    env.disable_known_hosts = True

    # Sanity check
    run("date")

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
        run('yum install -q -y puppet-2.7.17-1.el6')

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
        result = run("puppet agent --onetime --no-daemonize --verbose "
                     "--detailed-exitcodes --waitforcert 10 "
                     "--server {puppet}".format(
                     puppet=instance_data['default_puppet_server']))
        assert result.return_code in (0, 2)

    if 'home_tarball' in instance_data:
        put(instance_data['home_tarball'], '/tmp/home.tar.gz')
        with cd('~cltbld'):
            sudo('tar xzf /tmp/home.tar.gz', user="cltbld")
            sudo('chmod 700 .ssh', user="cltbld")
            sudo('chmod 600 .ssh/*', user="cltbld")
        run('rm -f /tmp/home.tar.gz')

    if "buildslave_password" in instance_data:
        # Set up a stub buildbot.tac
        sudo("/tools/buildbot/bin/buildslave create-slave /builds/slave {buildbot_master} {name} {buildslave_password}".format(**instance_data), user="cltbld")

    if "hg_shares" in instance_data:
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


def create_instance(name, config, region, secrets, key_name, instance_data):
    """Creates an AMI instance with the given name and config. The config must
    specify things like ami id."""
    conn = connect_to_region(
        region,
        aws_access_key_id=secrets['aws_access_key_id'],
        aws_secret_access_key=secrets['aws_secret_access_key']
    )
    vpc = VPCConnection(
        region=conn.region,
        aws_access_key_id=secrets['aws_access_key_id'],
        aws_secret_access_key=secrets['aws_secret_access_key'])

    # Make sure we don't request the same things twice
    token = str(uuid.uuid4())[:16]

    instance_data = instance_data.copy()
    instance_data['name'] = name
    instance_data['domain'] = config['domain']
    instance_data['hostname'] = '{name}.{domain}'.format(
        name=name, domain=config['domain'])

    bdm = None
    if 'device_map' in config:
        bdm = BlockDeviceMapping()
        for device, device_info in config['device_map'].items():
            bdm[device] = BlockDeviceType(size=device_info['size'],
                                          delete_on_termination=True)

    ip_address = get_ip(instance_data['hostname'])
    subnet_id = None

    if ip_address:
        s_id = get_subnet_id(vpc, ip_address)
        if s_id in config['subnet_ids']:
            if ip_available(conn, ip_address):
                subnet_id = s_id
            else:
                log.warning("%s already assigned" % ip_address)

    if not ip_address or not subnet_id:
        ip_address = None
        subnet_id = choice(config.get('subnet_ids'))

    while True:
        try:
            user_data = """<powershell>
Start-Transcript -Path 'c:\userdata-transcript.txt' -Force
Set-StrictMode -Version Latest
Set-ExecutionPolicy Unrestricted
 
Import-Module AWSPowerShell

$log = 'c:\userdata-log.txt'
Function Log ($str) {{
    $d = Get-Date
    Add-Content $log -value "$d - $str"
}}
 
Log "Userdata started"

# Set the administrator password; note that the password
# has to meet some minimum requirements (length, numbers, and
# I think a special symbol)
Log "Setting password"

$password = "password123!"
net user Administrator $password

# We need this helper, because PowerShell has a separate
# notion of directory for all child commands, and directory
# for the script. Running commands directly use the
# location set by cd, while things like DownloadFile
# will use the script directory (set by SetCurrentDirectory)
#
# This function makes things a little bit easier to follow
Function SetDirectory ($dir) {{
    Set-Location $dir
    [System.IO.Directory]::SetCurrentDirectory($dir)
}}

# silent MSI install helper
Function InstallMSI ($msi) {{
    Start-Process -Wait -FilePath "msiexec.exe" -ArgumentList "/qb /i $msi"
}}

# HTTP download helper
Function GetFromHTTP ($url, $path) {{
    Log "Downloading $url to $path"
    $client = new-object System.Net.WebClient
    $client.DownloadFile($url, $path)
}}

# For setting the hostname
Function SetHostname ($hostname, $domain) {{
        # http://msdn.microsoft.com/en-us/library/ms724224(v=vs.85).aspx
        $ComputerNamePhysicalDnsHostname = 5
        $ComputerNamePhysicalDnsDomain = 6

        Add-Type -TypeDefinition @"
        using System;
        using System.Runtime.InteropServices;

        namespace ComputerSystem {{
            public class Identification {{
                [DllImport("kernel32.dll", CharSet = CharSet.Auto)]
                static extern bool SetComputerNameEx(int NameType, string lpBuffer);

                public static bool SetPrimaryDnsSuffix(string suffix) {{
                    try {{
                        return SetComputerNameEx($ComputerNamePhysicalDnsDomain, suffix);
                    }}
                    catch (Exception) {{
                        return false;
                    }}
                }}
            }}
        }}
"@
        [ComputerSystem.Identification]::SetPrimaryDnsSuffix($domain)
        $computerName = Get-WmiObject Win32_ComputerSystem 
        $computerName.Rename($hostname)
}}

SetHostname {hostname} {domain}

SetDirectory $Env:USERPROFILE


### Install python
GetFromHTTP http://cruncher.srv.releng.scl3.mozilla.com/~catlee/python-2.7.5.msi python-2.7.5.msi
Log "Installing python"
Start-Process -Wait -FilePath "python-2.7.5.msi" -ArgumentList "/qn"
Log "Done"


### Install MozillaBuild
GetFromHTTP http://cruncher.srv.releng.scl3.mozilla.com/~catlee/MozillaBuildSetup-Latest.exe MozillaBuildSetup-Latest.exe
Log "Install MozillaBuild"
Start-Process -Wait -FilePath "MozillaBuildSetup-Latest.exe" -ArgumentList "/S"
Log "Done"


### Install puppet
GetFromHTTP http://cruncher.srv.releng.scl3.mozilla.com/~catlee/puppet-3.2.1.msi puppet-3.2.1.msi
Log "Installing puppet"
Start-Process -Wait -FilePath "msiexec.exe" -ArgumentList "/qb /i puppet-3.2.1.msi PUPPET_MASTER_SERVER={puppet_server} PUPPET_AGENT_CERTNAME={fqdn}"
Log "Done"


#Log "Running puppet"
#Start-Process -Wait -FilePath "C:\Program Files (x86)\Puppet Labs\Puppet\bin\puppet.bat" -ArgumentList "agent --test --server {puppet_server}"

Log "Done. Rebooting now!"
shutdown /r /t 0
</powershell>""".format(
        puppet_server=instance_data['default_puppet_server'],
        fqdn=instance_data['hostname'],
        hostname=instance_data['name'],
        domain=instance_data['domain'],
)

            reservation = conn.run_instances(
                image_id=config['ami'],
                key_name=key_name,
                instance_type=config['instance_type'],
                block_device_map=bdm,
                client_token=token,
                subnet_id=subnet_id,
                private_ip_address=ip_address,
                disable_api_termination=bool(config.get('disable_api_termination')),
                security_group_ids=config.get('security_group_ids', []),
                #user_data=config.get('user_data').encode('base64'),
                user_data=user_data,
            )
            break
        except boto.exception.BotoServerError:
            log.exception("Cannot start an instance")
        time.sleep(10)

    instance = reservation.instances[0]
    log.info("instance %s created, waiting to come up", instance)
    # Wait for the instance to come up
    while True:
        try:
            instance.update()
            if instance.state == 'running':
                break
        except Exception:
            log.exception("hit error waiting for instance to come up")
        time.sleep(10)

    instance.add_tag('Name', name)
    instance.add_tag('FQDN', instance_data['hostname'])
    instance.add_tag('created', time.strftime("%Y-%m-%d %H:%M:%S %Z",
                                              time.gmtime()))
    instance.add_tag('moz-type', config['type'])

    log.info("assimilating %s", instance)
    instance.add_tag('moz-state', 'pending')
    while True:
        try:
            assimilate(instance.private_ip_address, config, instance_data)
            break
        except:
            log.exception("problem assimilating %s", instance)
            time.sleep(10)
    instance.add_tag('moz-state', 'ready')


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
        except Exception:
            log.info('Waiting for instance stop')
            time.sleep(10)
    log.info('Creating snapshot')
    snapshot = v.create_snapshot('EBS-backed %s' % target_name)
    while True:
        try:
            snapshot.update()
            if snapshot.status == 'completed':
                break
        except Exception:
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


def make_instances(names, config, region, secrets, key_name, instance_data):
    """Create instances for each name of names for the given configuration"""
    procs = []
    for name in names:
        p = LoggingProcess(log="{name}.log".format(name=name),
                           target=create_instance,
                           args=(name, config, region, secrets, key_name,
                                 instance_data),
                           )
        p.start()
        procs.append(p)

    log.info("waiting for workers")
    for p in procs:
        p.join()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", required=True,
                        type=argparse.FileType('r'),
                        help="instance configuration to use")
    parser.add_argument("-r", "--region", help="region to use",
                        default="us-east-1")
    parser.add_argument("-k", "--secrets", type=argparse.FileType('r'),
                        required=True, help="file where secrets can be found")
    parser.add_argument("-s", "--key-name", help="SSH key name", required=True)
    parser.add_argument("-i", "--instance-data", help="instance specific data",
                        type=argparse.FileType('r'), required=True)
    parser.add_argument("--instance_id", help="assimilate existing instance")
    parser.add_argument("hosts", metavar="host", nargs="+",
                        help="hosts to be processed")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    try:
        config = json.load(args.config)[args.region]
    except KeyError:
        parser.error("unknown configuration")

    secrets = json.load(args.secrets)

    instance_data = json.load(args.instance_data)
    if args.instance_id:
        conn = connect_to_region(
            args.region,
            aws_access_key_id=secrets['aws_access_key_id'],
            aws_secret_access_key=secrets['aws_secret_access_key'],
        )
        instance = conn.get_all_instances([args.instance_id])[0].instances[0]
        instance_data['name'] = args.hosts[0]
        instance_data['domain'] = config['domain']
        instance_data['hostname'] = '{name}.{domain}'.format(
            name=args.hosts[0], domain=config['domain'])
        assimilate(instance.private_ip_address, config, instance_data)
    else:
        make_instances(args.hosts, config, args.region, secrets, args.key_name,
                       instance_data)
