#!/usr/bin/python
import os
import time
import boto.ec2
import requests
import logging
import subprocess
log = logging.getLogger()

from awslib import aws_get_all_instances

def aws_get_my_placement():
    return requests.get("http://169.254.169.254/latest/meta-data/placement/availability-zone").text

def aws_get_my_instanceid():
    return requests.get("http://169.254.169.254/latest/meta-data/instance-id").text

def aws_resize_disk(i, device, size):
    # TODO: write logs out so we can resume
    my_instance_id = aws_get_my_instanceid()
    i.update()
    name = i.tags.get("Name")
    conn = i.connection
    # Make sure it's off
    if i.state != "stopped":
	log.error("%s is not stopped", name)
	return False

    # Mark instance as not ready
    old_state = i.tags.get('moz-state')
    log.info("Setting state to 'resizing' (was '%s')", old_state)
    i.add_tag('moz-state', 'resizing')

    if device not in i.block_device_mapping:
	log.error("%s doesn't exist on %s", device, name)
	return False

    old_block_device = i.block_device_mapping[device]
    old_volume = conn.get_all_volumes([old_block_device.volume_id])[0]
    log.info("Old volume id was %s", old_volume.id)
    if old_volume.size == size:
	log.info("old volume is the same size as the new one; skipping")
	return True

    # Create a new volume
    new_volume = conn.create_volume(size, i.placement)
    log.info("Creating volume %s", new_volume.id)
    time.sleep(1)
    while new_volume.status != "available":
	log.info("Waiting for volume..")
	time.sleep(10)
	new_volume.update()

    # Attach the new volume here
    log.info("Attaching new volume %s", new_volume.id)
    conn.attach_volume(new_volume.id, my_instance_id, "/dev/sdc1")
    while not os.path.exists("/dev/xvdc1"):
	log.info("Waiting for %s to attach", new_volume.id)
	time.sleep(2)

    # Detach the old volume
    log.info("Detaching old volume %s", old_volume.id)
    conn.detach_volume(old_volume.id)
    while old_volume.status != "available":
	log.info("Waiting for %s to detach", old_volume.id)
	time.sleep(10)
	old_volume.update()

    # Attach the old volume here
    log.info("Attaching old volume %s", old_volume.id)
    conn.attach_volume(old_volume.id, my_instance_id, "/dev/sdb1")
    while not os.path.exists("/dev/xvdb1"):
	log.info("Waiting for %s to attach", old_volume.id)
	time.sleep(2)

    log.info("Formatting new volume %s", new_volume.id)
    subprocess.check_call(['sudo', 'mkfs.ext4', '/dev/xvdc1'])
    # TODO: Copy the actual label
    subprocess.check_call(['sudo', 'tune2fs', '-L', 'root_dev', '/dev/xvdc1'])

    # mount them locally
    log.info("mounting")
    subprocess.check_call(["sudo", "mkdir", "-p", "/mnt/sdb1", "/mnt/sdc1"])
    subprocess.check_call(["sudo", "mount", "-o", "ro", "/dev/xvdb1", "/mnt/sdb1"])
    subprocess.check_call(["sudo", "mount", "-o", "rw", "/dev/xvdc1", "/mnt/sdc1"])
    log.info("copying...(this can take a while...like several hours)")
    subprocess.check_call(["sudo", "rsync", "-a", "--stats", "/mnt/sdb1/", "/mnt/sdc1/"])

    # Unmount
    log.info("unmounting")
    subprocess.check_call(["sudo", "umount", "/mnt/sdb1"])
    subprocess.check_call(["sudo", "umount", "/mnt/sdc1"])

    log.info("fsck")
    subprocess.check_call(["sudo", "fsck", "-a", "/dev/xvdc1"])

    log.info("Detaching volumes")
    for v in old_volume, new_volume:
	conn.detach_volume(v.id)
	v.update()
	while v.status != "available":
	    log.info("Waiting for %s to detach", v.id)
	    time.sleep(10)
	    v.update()

    # Attach new volume to the instance
    log.info("Attaching new volume on instance")
    conn.attach_volume(new_volume.id, i.id, device)

    log.info("Restoring instance state")
    # Mark instance as ready
    i.add_tag('moz-state', old_state)

    log.info("Deleting old volume")
    # Delete old volume
    #conn.delete_volume(old_block_device.volume_id)
    return True


if __name__ == "__main__":
    import argparse
    import cPickle as pickle

    parser = argparse.ArgumentParser()
    parser.add_argument("hosts", nargs="+")
    parser.add_argument("-d", "--device", dest="device", required=True)
    parser.add_argument("-s", "--size", dest="size", type=int, required=True)
    parser.add_argument("-t", "--type", dest="type", required=True)

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

    instances = []
    instance_ids = set()

    my_placement = aws_get_my_placement()
    my_region = my_placement[:-1]
    log.info("My region is %s", my_region)

    # Load all instances for this region, then filter by our AZ and our type
    conn = aws_connect_to_region(my_region)
    res = conn.get_all_instances([], {
	'availability-zone': my_placement,
	'tag:moz-type': args.type,
    })
    # Collapse all the reservations
    all_instances = reduce(lambda x, y: x + y, [r.instances for r in res])

    # Go resize ALL the disks
    while instances:
	for i in instances[:]:
	    if aws_resize_disk(i, args.device, args.size):
		instances.remove(i)
