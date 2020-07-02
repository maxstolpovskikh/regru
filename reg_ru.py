import requests
import ast
import ipaddress
from datetime import datetime
import os
from peewee import *
import bz2


bz2_name = 'oix-full-snapshot-latest.dat.bz2'
fname = 'oix-full-snapshot-latest.dat'
db_name = 'database.db'
db = SqliteDatabase(db_name)

cleaning_list = [bz2_name, fname, db_name]


class BaseModel(Model):
    class Meta:
        database = db


class Network(BaseModel):
    network = CharField(unique=True)


def get_full_view():
    print('Скачиваю ', bz2_name)
    f = open(bz2_name, 'wb')
    ufr = requests.get('http://archive.routeviews.org/oix-route-views/oix-full-snapshot-latest.dat.bz2')
    f.write(ufr.content)
    f.close()
    file = open(fname, 'wb')
    print('Распаковываю ', fname)
    for line in bz2.BZ2File(bz2_name, 'rb', 10000000):
        file.write(line)
    file.close()


def ripe_date_common():
    r = requests.get(
        'https://rest.db.ripe.net/search.json?query-string=REGRU-MNT&inverse-attribute=mnt-by&type-filter=inetnum&flags'
        '=no-referenced&flags=no-irt&flags=no-filtering&source=RIPE')
    return ast.literal_eval(r.text)


def networks_common(answer):
    networks = []
    for net in answer['objects']['object']:
        attribute = net['attributes']['attribute']
        a = attribute[0]
        a = a['value'].split()
        startip = ipaddress.IPv4Address(a[0])
        endip = ipaddress.IPv4Address(a[2])
        net = [ipaddr for ipaddr in ipaddress.summarize_address_range(startip, endip)]
        for n in net:
            networks.append(n)
    return networks


def clear_network(networks):
    for a in networks:
      for b in networks:
          if a.supernet_of(b):
              networks.remove(b)
    return networks


def prefix_common(networks):
    prefix_list = []
    for n in networks:
        _str = str(n)
        _str = _str.split('.')
        prefix = _str[0] + '.' + _str[1]
        if prefix not in prefix_list:
            prefix_list.append(prefix)
    return prefix_list


def common_network_from_bgp(ripe_big_net, fname, prefix_list):
    unique_net = []
    bgp_networks = []
    with open(fname) as f:
        for line in f:
            string = line.split()
            try:
                net = string[1]
                rez = True
            except:
                rez = False
            if rez:
                prefix = net.split('.')
                if len(prefix) > 1:
                    prefix = prefix[0] + '.' + prefix[1]
                    if prefix in prefix_list:
                        if net not in unique_net:
                            unique_net.append(net)
                            ipv4 = ipaddress.ip_network(net)
                            if ipv4.is_global:
                                for a in ripe_big_net:
                                    if ipv4.subnet_of(a):
                                        bgp_networks.append(ipv4)
    return bgp_networks


def cidr_to_range(network):
    network = [str(ip) for ip in ipaddress.IPv4Network(network)]
    return [network[0], network[-1]]


def aggregate_range(networks_range):
    new_net_range = []
    for i in networks_range:
        rez = True
        while rez:
            ip = ipaddress.ip_address(i[1])
            ip = ip + 1
            rez = False
            for nr in networks_range:
                if ipaddress.ip_address(nr[0]) == ip:
                    i[1] = nr[1]
                    ip = ipaddress.ip_address(i[1])
                    ip = ip + 1
                    networks_range.remove(nr)
                    rez = True
        new_net_range.append(i)
    return new_net_range


def cleaning_temp(_list):
    for _l in _list:
        try:
            path = os.path.join(os.path.abspath(os.path.dirname(__file__)), _l)
            os.remove(path)
        except:
            pass


if __name__ == '__main__':
    date1 = datetime.now()
    cleaning_temp(cleaning_list)
    get_full_view()
    print('Ищу информацию о сетях в RIPE')
    answer = ripe_date_common()
    networks = networks_common(answer)
    ripe_big_net = clear_network(networks)
    prefix_list = prefix_common(ripe_big_net)
    print('Ищу информацию о сетях в', fname)
    net_list = common_network_from_bgp(ripe_big_net, fname, prefix_list)
    net_list.sort()
    networks_range = []
    _net_list = []
    for en in net_list:
        if en not in _net_list:
            _net_list.append(en)
    for n in _net_list:
        networks_range.append(cidr_to_range(n))
    _answer = aggregate_range(networks_range)
    answer = []
    for a in _answer:
        startip = ipaddress.IPv4Address(a[0])
        endip = ipaddress.IPv4Address(a[1])
        an = [ipaddr for ipaddr in ipaddress.summarize_address_range(startip, endip)]
        for _a in an:
            answer.append(_a)
    entry = []
    for a in answer:
        if a not in entry:
            entry.append(a)
    print('Создаю SqlLite DB')
    db.connect()
    db.create_tables([Network])
    print('Записываю результаты в', db_name)
    for en in entry:
        _entry = Network(network=en)
        _entry.save()
    date2 = datetime.now()
    print(f'Готово! Спасибо за терпение!\n'
          f'Запущен: {date1}\n'
          f'Завершил работу: {date2}\n'
          f'Выполнен за {date2 - date1}\n')