from IPy import IP

from cloudtools.aws import get_vpc


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
    interfaces = conn.get_all_network_interfaces()
    ips.extend(i.private_ip_address for i in interfaces)
    if ip in ips:
        return False
    else:
        return True


def get_subnets_by_name(region, name):
    conn = get_vpc(region)
    all_subnets = conn.get_all_subnets()
    return [s for s in all_subnets if s.tags.get('Name') == name]
