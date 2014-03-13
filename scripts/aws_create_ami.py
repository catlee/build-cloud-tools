#!/usr/bin/env python
from boto.ec2.blockdevicemapping import BlockDeviceMapping, BlockDeviceType
from fabric.api import run, put, env, lcd
import json
import time
import logging
import os
import site
import re

site.addsitedir(os.path.join(os.path.dirname(__file__), ".."))
from cloudtools.aws import get_aws_connection, AMI_CONFIGS_DIR, wait_for_status
from cloudtools.aws.instance import run_instance, get_instance

log = logging.getLogger()


def manage_service(service, target, state, distro="centos"):
    assert state in ("on", "off")
    if distro in ("debian", "ubuntu"):
        pass
    else:
        run('chroot %s chkconfig --level 2345 %s %s' % (target, service,
                                                        state))


def attach_volume(volume, instance, aws_dev_name, int_dev_name):
    while True:
        if volume.attach_data and volume.attach_data.instance_id == instance.id:
            log.debug("attached at %s", volume.attach_data.device)
            break
        try:
            log.debug("trying to attach at %s", aws_dev_name)
            volume.attach(instance.id, aws_dev_name)
            break
        except:
            log.debug('hit error waiting for volume to be attached')
            time.sleep(10)
    wait_for_status(volume, "status", "in-use", "update")
    while True:
        try:
            log.debug("waiting for %s", int_dev_name)
            if run('ls %s' % int_dev_name, quiet=True).succeeded:
                break
        except:
            log.debug('hit error waiting for volume to be attached')
            time.sleep(10)
    return


def get_volume(instance, size, aws_dev_name, int_dev_name, volume_name, instance_tag_name=None):
    """Creates a volume `size` GB large, and attaches it to instance"""
    volume_id = None
    if instance_tag_name:
        volume_id = instance.tags.get(instance_tag_name)
    if volume_id:
        v = instance.connection.get_all_volumes(volume_ids=[volume_id])[0]
    else:
        log.info("creating volume %s GB in %s", size, instance.placement)
        v = instance.connection.create_volume(size=size, zone=instance.placement)
        log.info("created %s", v)
        v.add_tag('Name', volume_name)
        instance.add_tag(instance_tag_name, v.id)
    attach_volume(v, instance, aws_dev_name, int_dev_name)
    return v


def is_mounted(mount_point):
    # Check if mount_dev is mounted
    log.info("Checking if %s is mounted", mount_point)
    mtab = run("cat /etc/mtab")
    if re.search("^\S+ %s" % mount_point, mtab, re.M):
        log.info("%s is mounted!", mount_point)
        return True


def format_device(mount_dev, fs, label, inode_size=None):
    if not inode_size:
        run('/sbin/mkfs.{fs_type} {dev}'.format(fs_type=fs, dev=mount_dev))
    else:
        run('/sbin/mkfs.{fs_type} -I {inode_size} {dev}'.format(fs_type=fs, dev=mount_dev, inode_size=inode_size))
    run('/sbin/e2label {dev} {label}'.format(dev=mount_dev, label=label))


def mount_device(mount_dev, mount_point):
    run('mkdir -p %s' % mount_point)
    run('mount {dev} {mount_point}'.format(dev=mount_dev,
                                           mount_point=mount_point))


def setup_chroot(config):
    mount_point = config['target']['mount_point']
    run('mkdir -p {0}/dev {0}/proc {0}/etc'.format(mount_point))
    if not is_mounted("%s/proc" % mount_point):
        run('mount -t proc proc %s/proc' % mount_point)


def install_debian(config_dir, config):
    mount_point = config['target']['mount_point']
    run('apt-get update')
    run('which debootstrap >/dev/null || apt-get install -y debootstrap')
    run('debootstrap precise %s http://puppetagain.pub.build.mozilla.org/data/repos/apt/ubuntu/' % mount_point)
    run('chroot %s mount -t proc none /proc' % mount_point)
    run('mount -o bind /dev %s/dev' % mount_point)
    put('%s/releng-public.list' % AMI_CONFIGS_DIR, '%s/etc/apt/sources.list' % mount_point)
    with lcd(config_dir):
        put('usr/sbin/policy-rc.d', '%s/usr/sbin/' % mount_point, mirror_local_mode=True)
    run('chroot %s apt-get update' % mount_point)
    run('DEBIAN_FRONTEND=text chroot {mnt} apt-get install -y '
        'ubuntu-desktop openssh-server makedev curl grub {kernel}'.format(
            mnt=mount_point, kernel=config['kernel_package']))
    run('rm -f %s/usr/sbin/policy-rc.d' % mount_point)
    run('umount %s/dev' % mount_point)
    run('chroot %s ln -s /sbin/MAKEDEV /dev/' % mount_point)
    for dev in ('zero', 'null', 'console', 'generic'):
        run('chroot %s sh -c "cd /dev && ./MAKEDEV %s"' % (mount_point, dev))
    run('chroot %s apt-get clean' % mount_point)


def install_centos(config_dir, config):
    mount_point = config['target']['mount_point']
    with lcd(config_dir):
        put('etc/yum-local.cfg', '%s/etc/yum-local.cfg' % mount_point)
        put('groupinstall', '/tmp/groupinstall')
        put('additional_packages', '/tmp/additional_packages')
    #run('which MAKEDEV >/dev/null || yum install -y MAKEDEV')
    run('for i in console null zero ; '
        'do cp -a /dev/$i {mount_point}/dev/$i ; done'.format(mount_point=mount_point))
    yum = 'yum -c {0}/etc/yum-local.cfg -y -q --installroot={0} '.format(
        mount_point)
    run('%s groupinstall "`cat /tmp/groupinstall`"' % yum)
    run('%s install `cat /tmp/additional_packages`' % yum)
    run('%s clean packages' % yum)
    # Rebuild RPM DB for cases when versions mismatch
    run('chroot %s rpmdb --rebuilddb || :' % mount_point)


def setup_grub(mount_point, int_dev_name):
    run("echo '(hd0) {int_dev_name}' > {mount_point}/boot/grub/device.map".format(mount_point=mount_point, int_dev_name=int_dev_name))
    run("grub-install --root-directory=%s --no-floppy %s" %
        (mount_point, int_dev_name))


def configify(config_dir, config):
    mount_point = config['target']['mount_point']
    run('chroot %s mkdir -p /boot/grub' % mount_point)
    with lcd(config_dir):
        for f in ('etc/rc.local', 'etc/fstab', 'etc/hosts',
                  'etc/sysconfig/network',
                  'etc/sysconfig/network-scripts/ifcfg-eth0',
                  'etc/init.d/rc.local', 'boot/grub/device.map',
                  'etc/network/interfaces', 'boot/grub/menu.lst',
                  'boot/grub/grub.conf'):
            if os.path.exists(os.path.join(config_dir, f)):
                put(f, '%s/%s' % (mount_point, f), mirror_local_mode=True)
            else:
                log.warn("Skipping %s", f)

    run('sed -i -e s,@ROOT_DEV_LABEL@,{label},g -e s,@FS_TYPE@,{fs},g '
        '{mnt}/etc/fstab'.format(label=config['target']['fs_label'],
                                 fs=config['target']['fs_type'],
                                 mnt=mount_point))
    if config.get('distro') in ('debian', 'ubuntu'):
        # sanity check
        run('ls -l %s/boot/vmlinuz-%s' % (mount_point, config['kernel_version']))
        run('sed -i s/@VERSION@/%s/g %s/boot/grub/menu.lst' %
            (config['kernel_version'], mount_point))
    else:
        run('ln -sf grub.conf %s/boot/grub/menu.lst' % mount_point)
        run('ln -sf ../boot/grub/grub.conf %s/etc/grub.conf' % mount_point)
        if config.get('kernel_package') == 'kernel-PAE':
            run('sed -i s/@VERSION@/`chroot %s rpm -q '
                '--queryformat "%%{version}-%%{release}.%%{arch}.PAE" '
                '%s | tail -n1`/g %s/boot/grub/grub.conf' %
                (mount_point, config.get('kernel_package', 'kernel'), mount_point))
        else:
            run('sed -i s/@VERSION@/`chroot %s rpm -q '
                '--queryformat "%%{version}-%%{release}.%%{arch}" '
                '%s | tail -n1`/g %s/boot/grub/grub.conf' %
                (mount_point, config.get('kernel_package', 'kernel'), mount_point))

    run("sed -i -e '/PermitRootLogin/d' -e '/UseDNS/d' "
        "-e '$ a PermitRootLogin without-password' "
        "-e '$ a UseDNS no' "
        "%s/etc/ssh/sshd_config" % mount_point)

    if config.get('distro') in ('debian', 'ubuntu'):
        pass
    else:
        manage_service("network", mount_point, "on")
        manage_service("rc.local", mount_point, "on")


def unmount(mount_point):
    run('umount %s/proc || :' % mount_point)
    run('umount %s' % mount_point)


def create_snapshot(v, name):
    v.detach()
    wait_for_status(v, "status", "available", "update")

    log.info('Creating a snapshot')
    snapshot = v.create_snapshot('EBS-backed %s' % name)
    wait_for_status(snapshot, "status", "completed", "update")
    snapshot.add_tag('Name', name)
    return snapshot


def register_ami(conn, name, config, virtualization_type, boot_snapshot, root_snapshot):
    vt_config = config['target_virtualization_types'][virtualization_type]
    root_dev_name = vt_config['root_dev_name']
    boot_dev_name = vt_config['boot_dev_name']
    block_map = BlockDeviceMapping()
    block_map[root_dev_name] = BlockDeviceType(snapshot_id=root_snapshot.id)
    block_map[boot_dev_name] = BlockDeviceType(snapshot_id=boot_snapshot.id)

    kernel_id = vt_config.get('kernel_id')

    ami_id = conn.register_image(
        name,
        '%s EBS AMI' % name,
        architecture=config['host_config']['arch'],
        kernel_id=kernel_id,
        block_device_map=block_map,
        virtualization_type=virtualization_type,
        # Tell Amazon to use the boot volume as the root device
        root_device_name=boot_dev_name,
    )
    while True:
        try:
            ami = conn.get_image(ami_id)
            ami.add_tag('Name', name)
            if config["target"].get("tags"):
                for tag, value in config["target"]["tags"].items():
                    log.info("Tagging %s: %s", tag, value)
                    ami.add_tag(tag, value)
            log.info('AMI created')
            log.info('ID: {id}, name: {name}'.format(id=ami.id, name=ami.name))
            return ami
        except:
            log.info('Wating for AMI')
            time.sleep(10)


def patch_grub(config_dir):
    # See https://bugs.archlinux.org/task/30241 for the details,
    # grub-nstall doesn't handle /dev/xvd* devices properly
    log.info("patching grub-install")
    grub_install_patch = os.path.join(config_dir, "grub-install.diff")
    run("which patch || yum install -y -q patch")
    put(grub_install_patch, "/tmp/grub-install.diff")
    run('patch -p0 -i /tmp/grub-install.diff /sbin/grub-install')


def setup_root_volume(instance, config, config_dir):
    int_dev_name = config['target']['int_dev_name']
    aws_dev_name = config['target']['aws_dev_name']
    mount_point = config['target']['mount_point']
    v = get_volume(instance, config['target']['size'], aws_dev_name, int_dev_name, "root", "root_volume_id")

    # Step 1: prepare target FS
    if not v.tags.get('formatted'):
        format_device(int_dev_name, config['target']['fs_type'], config['target']['fs_label'])
        v.add_tag('formatted', time.time())

    if not is_mounted(mount_point):
        mount_device(int_dev_name, mount_point)

    # Step 2: install base system
    if not v.tags.get('installed_os'):
        if config.get('distro') in ('centos',):
            setup_chroot(config)

        if config.get('distro') in ('debian', 'ubuntu'):
            install_debian(config_dir, config)
        elif config.get('distro') in ('centos',):
            install_centos(config_dir, config)

        v.add_tag('installed_os', time.time())

    # Step 3: upload custom configuration files
    if not v.tags.get('added_configs'):
        configify(config_dir, config)
        patch_grub(config_dir)
        v.add_tag('added_configs', time.time())

    return v


def get_boot_snapshot(instance, config, vt, snapshot_name):
    vt_config = config['target_virtualization_types'][vt]
    mount_point = config['target']['mount_point']
    bv = get_volume(instance, 1, vt_config['aws_dev_name'], vt_config['int_dev_name'], "boot-%s" % vt)
    run("mkdir -p /mnt/boot-%s" % vt)
    if is_mounted("/mnt/boot-%s" % vt):
        unmount("/mnt/boot-%s" % vt)

    if vt == "hvm":
        run("parted -s {int_dev_name} mklabel msdos".format(int_dev_name=vt_config['int_dev_name']))
        run("parted -s {int_dev_name} mkpart primary ext2 1 '100%'".format(int_dev_name=vt_config['int_dev_name']))
        run("parted -s {int_dev_name} set 1 boot on".format(int_dev_name=vt_config['int_dev_name']))
        format_device(vt_config['int_dev_name'] + "1", "ext2", "boot", 128)
        mount_device(vt_config['int_dev_name'] + "1", "/mnt/boot-%s" % vt)
        run("rsync -a --delete {mount_point}/boot/ /mnt/boot-{vt}/boot/".format(mount_point=mount_point, vt=vt))
        setup_grub("/mnt/boot-%s" % vt, vt_config['int_dev_name'])
    else:
        format_device(vt_config['int_dev_name'], "ext2", "boot", 128)
        mount_device(vt_config['int_dev_name'], "/mnt/boot-%s" % vt)
        run("rsync -a --delete {mount_point}/boot/ /mnt/boot-{vt}/boot/".format(mount_point=mount_point, vt=vt))

    unmount("/mnt/boot-%s" % vt)
    s = create_snapshot(bv, snapshot_name)
    log.info("deleting boot volume %s", bv)
    bv.delete()
    return s


def create_amis(target_name, host_instance, config, keep_host_instance=False, keep_volume=False):
    connection = host_instance.connection
    env.host_string = host_instance.private_ip_address
    env.user = 'root'
    env.abort_on_prompts = True
    env.disable_known_hosts = True

    config_dir = "%s/%s" % (AMI_CONFIGS_DIR, target_name)
    dated_target_name = "%s-%s" % (
        target_name, time.strftime("%Y-%m-%d-%H-%M", time.gmtime()))
    mount_point = config['target']['mount_point']

    run("date", quiet=True)

    # AMIs: HVM AMI, PV AMI
    # X AMI: root snapshot, X snapshot
    # root snapshot: root installed
    # root installed: root volume, etc...
    # X snapshot: root installed
    log.info("Setting up root volume")
    root_volume = setup_root_volume(host_instance, config, config_dir)

    # TODO: Check if we have a root snapshot id already?

    # Create boot volumes and snapshots
    boot_snapshots = {}
    for vt in config['target_virtualization_types']:
        log.info("Creating snapshot for %s boot volume", vt)
        boot_snapshots[vt] = get_boot_snapshot(host_instance, config, vt, "boot %s-%s" % (vt, dated_target_name))
        host_instance.add_tag('%s-snapshot_id' % vt, boot_snapshots[vt].id)

    # Step 5: Create a snapshot of /
    if not host_instance.tags.get('root_snapshot_id'):
        log.info("Creating snapshot of root volume %s", root_volume)
        unmount(mount_point)
        root_snapshot = create_snapshot(root_volume, dated_target_name)
        log.info("snapshot: %s", root_snapshot)
        host_instance.add_tag('root_snapshot_id', root_snapshot.id)
    else:
        log.info("Using previous snapshot of root volume %s", host_instance.tags['root_snapshot_id'])
        root_snapshot = connection.get_all_snapshots(snapshot_ids=[host_instance.tags['root_snapshot_id']])[0]

    # Step 6: Create an AMI
    log.info('Creating AMIs')
    amis = {}
    for vt in config['target_virtualization_types']:
        boot_snapshot = boot_snapshots[vt]
        ami = register_ami(connection, "%s-%s" % (vt, dated_target_name),
                           config, vt, boot_snapshot, root_snapshot)
        amis[vt] = ami

    # Step 7: Cleanup
    if not keep_volume:
        log.info('Deleting volume')
        root_volume.delete()

    if not keep_host_instance:
        log.info('Terminating host instance')
        host_instance.terminate()

    return amis


def main():
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.set_defaults(
        config=None,
        region="us-east-1",
        key_name=None,
        action="create",
        keep_volume=False,
        keep_host_instance=False,
        loglevel=logging.INFO,
    )
    parser.add_argument("-c", "--config", dest="config",
                        help="instance configuration to use", required=True,
                        )
    parser.add_argument("-r", "--region", dest="region", help="region to use")
    parser.add_argument('--keep-volume', dest='keep_volume', action='store_true',
                        help="Don't delete target volume")
    parser.add_argument('--keep-host-instance', dest='keep_host_instance',
                        action='store_true', help="Don't delete host instance")
    parser.add_argument("-i", "--instance-id", dest="instance_id", help="instance id to use instead of creating a new one")
    parser.add_argument('-v', '--verbose', dest='loglevel', action='store_const', const=logging.DEBUG)
    parser.add_argument('-q', '--quiet', dest='loglevel', action='store_const', const=logging.WARN)

    args = parser.parse_args()

    logging.basicConfig(format="%(asctime)s - %(message)s", level=args.loglevel)
    logging.getLogger("boto").setLevel(logging.INFO)
    logging.getLogger("paramiko").setLevel(logging.INFO)

    try:
        target_name = args.config
        config_file = os.path.join(AMI_CONFIGS_DIR, "%s.json" % target_name)
        config = json.load(open(config_file))[args.region]
    except KeyError:
        parser.error("unknown configuration")

    connection = get_aws_connection(args.region)
    host_config = config['host_config']

    if not args.instance_id:
        host_instance = run_instance(
            connection,
            instance_name=host_config['instance_name'],
            config=host_config,
            key_name=host_config['ssh_keyname'],
            user=host_config['user'],
        )
    else:
        host_instance = get_instance(connection, args.instance_id)
        if host_instance.state != 'running':
            log.info("Starting %s", host_instance)
            host_instance.start()
            wait_for_status(host_instance, 'state', 'running', 'update')

    create_amis(target_name, host_instance, config,
                keep_volume=args.keep_volume,
                keep_host_instance=args.keep_host_instance)

if __name__ == '__main__':
    main()
