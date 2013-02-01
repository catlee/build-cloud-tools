#!/usr/bin/python
import os
import time

import subprocess
import logging
log = logging.getLogger()

import requests

from awslib import aws_connect_to_region

from statelogger import StateLogger


def aws_get_my_placement():
    return requests.get("http://169.254.169.254/latest/meta-data/placement/availability-zone").text

_my_instance_id = None


def aws_get_my_instanceid():
    global _my_instance_id
    if not _my_instance_id:
        _my_instance_id = requests.get("http://169.254.169.254/latest/meta-data/instance-id").text
    return _my_instance_id


def aws_detach_volume(v):
    v.update()
    if v.attach_data.instance_id is None:
        # Nothing to do, carry on!
        return

    log.info("Detaching volume %s from %s", v.id, v.attach_data.instance_id)
    if not v.detach():
        log.warn("Didn't detach cleanly")

    while v.status != "available":
        log.info("Waiting for %s to detach", v.id)
        time.sleep(10)
        v.update()


def aws_safe_attach_volume(v, allowed_instance_ids, device):
    """
    Attaches volume `v` to the current instance safely.
    It will detach v from any instances in allowed_instance_ids
    It will be attached here as `device`

    Return True once attached
    """
    log.info("Attaching volume %s", v.id)
    my_instance_id = aws_get_my_instanceid()
    v.update()
    if v.attach_data.instance_id == my_instance_id:
        log.info("Already attached to me!")
    elif v.attach_data.instance_id is None:
        # Attach the new volume here
        log.info("Currently detached; attach it here")
        v.attach(my_instance_id, device)
    elif v.attach_data.instance_id in allowed_instance_ids:
        log.info("Volume is already attached to someone else")
        aws_detach_volume(v)
        log.info("Attach it here")
        v.attach(my_instance_id, device)
    else:
        log.error("Volume is attached to %s - I won't touch it!", v.attach_data.instance_id)
        return False

    while v.attach_data.instance_id != my_instance_id:
        log.info("Waiting for %s to attach", v.id)
        time.sleep(2)
        v.update()

    while not os.path.exists(device):
        log.info("Waiting for %s to attach", v.id)
        time.sleep(2)
    return True


def aws_resize_disk(i, device, size):
    slog = StateLogger("logs/%s" % i.id)
    my_instance_id = aws_get_my_instanceid()

    i.update()
    name = i.tags.get("Name")
    conn = i.connection
    # Make sure it's off
    if i.state != "stopped":
        log.error("%s is not stopped", name)
        return False

    if slog.get('device') is None:
        if device not in i.block_device_mapping:
            log.error("%s doesn't exist on %s", device, name)
            return False
        slog.log("old device", device=device)
        old_block_device = i.block_device_mapping[device]
        old_volume = conn.get_all_volumes([old_block_device.volume_id])[0]
    elif slog.get('device') != device:
        log.error("old device was %s, but we're trying to mount %s", slog.get('device'), device)
        return False
    else:
        old_volume = conn.get_all_volumes([slog.get('old_volume')])[0]

    log.info("Old volume id was %s", old_volume.id)
    if slog.get("old_volume") != old_volume.id:
        slog.log("old volume id", old_volume=old_volume.id)

    if old_volume.size == size:
        log.info("old volume is the same size as the new one; skipping")
        return True

    # Detach volumes from myself
    my_instance = conn.get_all_instances([my_instance_id])[0].instances[0]
    for d in "/dev/sdb1", "/dev/sdc1":
        if d in my_instance.block_device_mapping:
            # TODO: Make sure it's unmounted
            my_vol_id = my_instance.block_device_mapping[d].volume_id
            my_vol = conn.get_all_volumes([my_vol_id])[0]
            log.info("Detaching %s from myself", my_vol_id)
            aws_detach_volume(my_vol)

    # Mark instance as not ready
    old_state = slog.get('old_state')
    if not old_state:
        old_state = i.tags.get('moz-state')
        if old_state not in ('ready', 'resizing'):
            log.error("%s in unsupported state: %s", name, old_state)
            return False

    if old_state != 'resizing':
        slog.log('old state', old_state=old_state)
        log.info("Setting state to 'resizing' (was '%s')", old_state)
        i.add_tag('moz-state', 'resizing')

    # Create a new volume
    new_volume_id = slog.get('new_volume')
    if not new_volume_id:
        new_volume = conn.create_volume(size, i.placement)
        slog.log("new volume id", new_volume=new_volume.id)
        slog.log("needs formatting", formatted=False)
        log.info("Creating volume %s", new_volume.id)
        time.sleep(1)
        while new_volume.status != "available":
            log.info("Waiting for volume..")
            time.sleep(10)
            new_volume.update()
    else:
        log.info("Re-using already created volume %s", new_volume_id)
        new_volume = conn.get_all_volumes([new_volume_id])[0]
        assert new_volume.id == new_volume_id

    # Attach the new volume
    if not aws_safe_attach_volume(new_volume, [i.id], "/dev/sdc1"):
        log.error("Couldn't attach new volume")
        return

    # Attach the old volume
    if not aws_safe_attach_volume(old_volume, [i.id], "/dev/sdb1"):
        log.error("Couldn't attach new volume")
        return

    if not slog.get('formatted'):
        log.info("Formatting new volume %s", new_volume.id)
        subprocess.check_call(['sudo', 'mkfs.ext4', '/dev/sdc1'])
        # TODO: Copy the actual label
        subprocess.check_call(['sudo', 'tune2fs', '-L', 'root_dev', '/dev/sdc1'])
        slog.log("formatted new volume", formatted=True)
    else:
        log.info("Skipping formatting - we did it already!")

    # mount them locally
    log.info("mounting")
    subprocess.check_call(["sudo", "mkdir", "-p", "/mnt/sdb1", "/mnt/sdc1"])
    # TODO: Check if they're already mounted
    subprocess.check_call(["sudo", "mount", "-o", "ro", "/dev/sdb1", "/mnt/sdb1"])
    subprocess.check_call(["sudo", "mount", "-o", "rw", "/dev/sdc1", "/mnt/sdc1"])
    log.info("copying...(this can take a while...like several hours)")
    subprocess.check_call(["sudo", "rsync", "-a", "--delete", "--stats", "/mnt/sdb1/", "/mnt/sdc1/"])

    # Unmount
    log.info("unmounting")
    subprocess.check_call(["sudo", "umount", "/mnt/sdb1"])
    subprocess.check_call(["sudo", "umount", "/mnt/sdc1"])

    log.info("fsck")
    subprocess.check_call(["sudo", "fsck", "-a", "/dev/sdc1"])

    log.info("Detaching volumes")
    for v in old_volume, new_volume:
        aws_detach_volume(v)

    # Attach new volume to the instance
    log.info("Attaching new volume on instance")
    new_volume.attach(i.id, device)

    log.info("Restoring instance state")
    # Mark instance as ready
    i.add_tag('moz-state', old_state)
    slog.log("reset state", old_state=None)
    slog.log("old volume", old_volume=None)
    slog.log("device", device=None)

    log.info("Deleting old volume")
    # Delete old volume
    #conn.delete_volume(old_block_device.volume_id)
    return True


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("hosts", nargs="*")
    parser.add_argument("-d", "--device", dest="device", required=True)
    parser.add_argument("-s", "--size", dest="size", type=int, required=True)
    parser.add_argument("-t", "--type", dest="type", required=True)

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

    instances = []
    instance_ids = set()

    my_placement = aws_get_my_placement()
    my_region = my_placement[:-1]
    log.info("My region is %s", my_placement)

    # Load all instances for this region, then filter by our AZ and our type
    conn = aws_connect_to_region(my_region, {})
    res = conn.get_all_instances([], {
        'availability-zone': my_placement,
        'tag:moz-type': args.type,
    })
    # Collapse all the reservations
    instances = reduce(lambda x, y: x + y, [r.instances for r in res])
    if args.hosts:
        # Filter out the hosts we care about
        instances = [i for i in instances if i.tags.get('Name') in args.hosts]

    # Remove instances that already have the right disk size
    for i in instances[:]:
        name = i.tags.get('Name')
        if args.device in i.block_device_mapping:
            block_device = i.block_device_mapping[args.device]
            vol = conn.get_all_volumes([block_device.volume_id])[0]
            if vol.size == args.size:
                log.info("%s(%s) already has the correct size; skipping", name, i)
                instances.remove(i)

    # Go resize ALL the disks
    while instances:
        for i in instances[:]:
            i.update()
            if i.state != "stopped":
                log.info("Skipping %s; not stopped", i.tags.get('Name'))
                continue
            log.info("Resizing disk on %s (%s)", i.tags.get('Name'), i.id)
            if aws_resize_disk(i, args.device, args.size):
                instances.remove(i)
            else:
                exit()
        else:
            if instances:
                log.info("still some instances to re-size; try again later")
                time.sleep(300)
