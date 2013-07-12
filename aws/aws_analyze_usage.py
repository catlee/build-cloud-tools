#!/usr/bin/env python
import csv
import datetime

from aws_ec2_pricing import get_pricing
import boto.ec2

# Used by aws_connect_to_region to cache connection objects per region
_aws_cached_connections = {}


def aws_connect_to_region(region, secrets):
    """Connect to an EC2 region. Caches connection objects"""
    if region in _aws_cached_connections:
        return _aws_cached_connections[region]
    conn = boto.ec2.connect_to_region(region, **secrets)
    _aws_cached_connections[region] = conn
    return conn


def get_region_and_type(usage_type):
    """Returns the region for the given usage_type"""
    if "USW2" in usage_type:
        region = "us-west-2"
    elif "USW1" in usage_type:
        region = "us-west-1"
    else:
        assert usage_type.startswith("BoxUsage")
        region = "us-east-1"

    if ":" in usage_type:
        type_ = usage_type.split(":")[1]
    else:
        type_ = "m1.small"
    return region, type_


def calc_hourly_cost(usage, reservations):
    cost = 0.0
    for date, region, type_, count in usage:
        # apply reservations
        for r_type in 'HU', 'MU', 'LU':
            if (region, type_, r_type) in reservations:
                r_count = reservations[region, type_, r_type]
                # We can only have up to count of these
                r_count = min(count, r_count)

                # Decrement number of instances, down to 0 at a minimum
                count = max(0, count - r_count)

                # Add the cost for this hour
                try:
                    cost += (get_pricing(region, type_, r_type)[1] * r_count)
                except KeyError:
                    raise KeyError("region: %s; type: %s; rtype: %s" % (region, type_, r_type))

        assert count >= 0
        try:
            cost += (get_pricing(region, type_, 'ondemand')[1] * count)
        except KeyError:
            raise KeyError("region: %s; type: %s; rtype: ondemand" % (region, type_))

    return cost


def calc_monthly_cost(usage, reservations):
    cost = calc_hourly_cost(usage, reservations)

    # Amortize the cost over a month
    first_date = usage[0][0]
    last_date = usage[-1][0]

    elapsed = td2s(last_date - first_date)
    if not elapsed:
        monthly_cost = 0.0
    else:
        monthly_cost = cost * (86400 * 365.25 / 12.0) / elapsed

    # Add the upfront cost of the reservations, amortized by month
    for (region, type_, r_type), count in reservations.items():
        fixed_cost = get_pricing(region, type_, r_type)[0] * count
        monthly_cost += fixed_cost

    return monthly_cost


def td2s(d):
    """Returns total number of seconds for the given timedelta object"""
    return d.seconds + d.days * 86400


def get_usage_types(usage):
    retval = set()
    for d, region, type_, c in usage:
        retval.add((region, type_))
    return sorted(list(retval))


def filter_usage(usage, region, type_):
    retval = []
    for d, r, t, c in usage:
        if (r, t) == (region, type_):
            retval.append((d, r, t, c))
    return retval


def calc_best_reservations(usage, region, type_):
    filtered_usage = filter_usage(usage, region, type_)
    best_cost = calc_monthly_cost(filtered_usage, {})

    best_reservations = {}
    i = 0
    while True:
        # Try adding one to each of the types and see which saves the most, if
        # any
        i += 1
        costs = []
        for r in 'HU', 'MU', 'LU':
            reservations = best_reservations.copy()
            reservations[region, type_, r] = reservations.get((region, type_, r), 0) + 1
            cost = calc_monthly_cost(filtered_usage, reservations)
            costs.append((cost, reservations, r))

        best = min(costs)
        if best[0] < best_cost:
            best_reservations = best[1]
            best_cost = best[0]
        else:
            break

    return best_reservations


def parse_report(report):
    r = csv.reader(report)

    # Eat the header
    r.next()

    usage = []

    for row in r:
        if not row:
            continue
        if "BoxUsage" not in row[2]:
            continue

        region, type_ = get_region_and_type(row[2])
        count = int(row[-1])
        date = datetime.datetime.strptime(row[4], "%m/%d/%y %H:%M:%S")
        u = (date, region, type_, count)
        usage.append(u)

    return usage


def aws_get_current_reservations(regions):
    retval = {}
    for g in regions:
        conn = aws_connect_to_region(g, {})
        reservations = conn.get_all_reserved_instances()
        for r in reservations:
            if r.state == 'retired':
                continue
            usage_type = {'Medium Utilization': 'MU', 'Light Utilization': 'LU', 'Heavy Utilization': 'HU'}[r.offering_type]
            k = (r.availability_zone, r.instance_type, usage_type)
            retval[k] = retval.get(k, 0) + r.instance_count
    return retval


def aws_get_current_instance_counts(regions):
    retval = {}
    for g in regions:
        conn = aws_connect_to_region(g, {})
        reservations = conn.get_all_instances()
        for r in reservations:
            for i in r.instances:
                if i.state == 'terminated':
                    continue
                k = (i.placement, i.instance_type)
                retval[k] = retval.get(k, 0) + 1
    return retval


def calc_az_weights(instances, region, type_):
    """
    Returns a dict mapping AZ to a (count, weight) tuple
    """
    # Find the list of instances in this region of the specified type
    our_instances = []
    s = 0
    for (az, t), count in instances.items():
        if az.startswith(region) and t == type_:
            our_instances.append((az, count))
            s += count

    retval = {}
    for az, count in our_instances:
        retval[az] = (count, count / float(s))

    return retval


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument("-r", "--report", dest="report", required=True, type=file)
    parser.add_argument("-s", dest="suggest", default=False, action="store_true")

    args = parser.parse_args()

    usage = parse_report(args.report)

    current_cost = calc_monthly_cost(usage, {})
    print "usage cost   ${:,.2f}".format(calc_hourly_cost(usage, reservations={}))
    print "monthly cost ${:,.2f}".format(current_cost)

    # What kinds of machines are we using?
    usage_types = get_usage_types(usage)

    def sort_by_region(i):
        (region, instance_type, usage_type), count = i
        # sort by region, instance type as normal
        # but sort light, medium, heavy
        u = {'LU': 0, 'MU': 1, 'HU': 2}[usage_type]
        return region, instance_type, u

    best_reservations = {}
    for region, type_ in usage_types:
        b = calc_best_reservations(usage, region, type_)
        best_reservations.update(b)
    new_cost = calc_monthly_cost(usage, best_reservations)
    print "new cost     ${:,.2f}".format(new_cost)
    print "            -${:,.2f}".format(current_cost - new_cost)

    if not args.suggest:
        for (region, type_, r_type), count in sorted(best_reservations.items(), key=sort_by_region):
            print "{:8s} {:12s} {:2s} {:d}".format(region, type_, r_type, count)
        exit()

    # compare with current reservations
    regions = set(r for (r, t) in usage_types)
    current_az_reservations = aws_get_current_reservations(regions)
    current_reservations = {}
    for (r, t, u), c in current_az_reservations.items():
        # Strip off AZ suffix
        r = r[:-1]
        if (r, t, u) not in current_reservations:
            current_reservations[r, t, u] = 0
        current_reservations[r, t, u] += c

    # figure out how much we're off by
    delta = best_reservations.copy()
    for (r, t, u), c in current_reservations.items():
        delta[r, t, u] = delta.get((r, t, u), 0) - c

    #print
    #print "{:9s} {:12s} {:4s} {:>4s} {:>4s} {:>4s}".format("region", "type", "load", "cur", "best", "delta")
    #for (region, type_, r_type), d_count in sorted(delta.items(), key=sort_by_region):
        #cur_count = current_reservations.get((region, type_, r_type), 0)
        #best_count = best_reservations.get((region, type_, r_type), 0)
        #print "{:9s} {:12s} {:4s} {:4d} {:4d} {:+4d}".format(region, type_, r_type, cur_count, best_count, d_count)

    print
    print "Recommendations"
    # figure out where to buy them (specific AZs)
    """
    Ideally figure out which reservations give the most bang-for-the buck
    Sort list of deltas in increasing order
    Divide by some factor
    Get list of instances in that AZ
    Get number of reservations in that AZ
    Balance new reservations into AZs
    """
    # Sort by count
    current_instances = aws_get_current_instance_counts(regions)
    for (region, type_, r_type), d_count in sorted(delta.items(), key=lambda i: (i[0][2], -i[1])):
        cur_count = current_reservations.get((region, type_, r_type), 0)
        best_count = best_reservations.get((region, type_, r_type), 0)
        print "{:9s} {:12s} {:4s} {:4d} {:4d} {:+4d}".format(region, type_, r_type, cur_count, best_count, d_count)
        az_weights = calc_az_weights(current_instances, region, type_)
        #print az_weights

        # figure out delta per az
        delta_by_az = dict((az, 0) for az in az_weights)
        for az in delta_by_az:
            c = current_az_reservations.get((az, type_, r_type), 0)
            r = az[:-1]
            if (best_reservations.get((region, type_, r_type), 0) * az_weights.get(az, (0, 0))[1]) <= current_instances.get((az, t), 0):
                delta_by_az[az] = (best_reservations.get((region, type_, r_type), 0) * az_weights.get(az, (0, 0))[1]) - c

        factor = 4.0  # TODO: remove hardcode
        for az, count in sorted(delta_by_az.items(), key=lambda x: -x[1]):
            delta_by_az[az] = int(round(count / factor))
            print "    {az} {count}".format(az=az, count=delta_by_az[az])
