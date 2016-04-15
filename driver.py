import time, csv, re, subprocess


def run_command(command):
    p = subprocess.Popen(command,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    return iter(p.stdout.readline, b'')


databasescommand = ("hadoop fs -ls /user/hive/warehouse/*.db").split()
date = time.strftime("%d/%m/%Y")

outputs = []

def getBlockDistribution(database):
    command = ("hadoop fsck " + database + " -files -blocks -locations").split()

    ips = []


    for line in run_command(command):
        ip = re.findall('([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\:[0-9]{2,5})', line)
        if (ip):
            ips.append(ip)

    ipcounts = {}

    ipsfinal = []

    for ip in ips:
        for i in ip:
            ipsfinal.append(i)

    for ip in ipsfinal:
        if (ipcounts.has_key(str(ip))):
            ipcounts[str(ip)] += 1
        else:
            ipcounts[str(ip)] = 1

    for key, value in ipcounts.iteritems():
        return (key, value)

for db in run_command(databasescommand):

    temparr = re.findall('(\/user\/hive\/warehouse\/.*)$', db)
    if (temparr):
        for dbitem in temparr:
            tple = getBlockDistribution(dbitem)
            outputs.append((date, temparr, tple[0], tple[1]))

csvwriter = csv.writer(open('blockdistibution.csv', 'wb'), delimiter=',')
for output in outputs:
    csvwriter.writerow(output)