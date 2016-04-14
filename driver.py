import subprocess
import re
import sys


def run_command(command):
    p = subprocess.Popen(command,
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    return iter(p.stdout.readline, b'')

searcharea = sys.argv[1]

command = ("hadoop fsck " + searcharea + " -files -blocks -locations").split()

ips = []

for line in run_command(command):
	ip = re.findall('([0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\:[0-9]{2,5})', line)
	if(ip):
		ips.append(ip)

ipcounts = {}

for ip in ips:
	if(ipcounts.has_key(str(ip))):
		ipcounts[str(ip)] += 1
	else:
		ipcounts[str(ip)] = 1

for key, value in ipcounts.iteritems():
	print "Datanode %s has %d blocks" % (str(key), value)
