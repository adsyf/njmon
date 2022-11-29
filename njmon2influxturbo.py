#!/usr/bin/python3
# Version 53
# njmon2influxturbo takes njmon for AIX or Linux v50+ JSON data
#  and loads it into InfluxDB via njmond
# njmond is FAST due to being multi-threaded and caching records in a queue.
#
# NOTE DO NOT CHANGE THIS FILE
# The njmond file njmond.conf is used to set the parameters 

import sys
from sys import argv
import json
import socket

def hints():
    print('njmon2influxturbo.py HELP Information - Version 50')
    print('')
    print('Purpose: takes njmon v50+ data and loads it into InfluxDB via njmond')
    print('Syntax: njmon2influxturbo.py njmond.conf')
    print('')
    print('Syntax for alternative uses:')
    print('File on InfluxDB server')
    print('    1 ./njmon2influxturbo.py njmond.conf <my_njmon_file.json ')
    print('File on source server and ssh to the InfluxDB server')
    print('    2 cat my_njmon_file.json | ssh nigel@influxdb_box /home/nag/njmon/njmon2influxturbo.py njmond.conf')
    print('Skip the file on source server and ssh to the InfluxDB server')
    print('    3 njmon -s30 -c 2880 | ssh nigel@influxdb_box /home/nag/njmon/njmon2influxturbo.py njmond.conf')
    print('')
    print('Example config file - njmond.conf (these are the lines used by njmon2influxturbo.py):')
    print('{')
    print('"njmon_port": "8181",')
    print('"njmond_host": localhost,')
    print('...')
    print('}')
    print('Recommend njmond.py is running with at least 10 workers (see "workers": 20 in its njmond.conf)')
    print('')
    sys.exit(6)

# --- main ---
try:
    configfile = argv[1]
except:
    print("njmon2influxturbo: Missing config file argument: like njmond.conf")
    hints()

if configfile == "-h" or configfile == "-?" or configfile == "help":
    hints()

# read the conf file removing line starting with a "#"
try:
    configs = ""
    config = {}
    with open(configfile,"r") as f:
        for line in f:
            if line[0] == '#':
                pass
            else:
                configs = configs + line
    config = json.loads(configs) # convert string to Python dictionary
except:
    print("njmon2influxturbo: Failed to read or parse JSON in the njmond config file:"+str(configfile))
    #debug print(configs)
    #debug print(config)
    sys.exit(20)

try:
    njmond_host  = config["njmond_host"]
    njmon_port = config["njmon_port"]
except:
    print("njmon2influxturbo: Config missing njmond_host and/or njmon_port entries")
    hints()

count = 0
bytes = 0

for line in sys.stdin:
    count = count + 1
    length = len(line)
    bytes = bytes + length

    # debug print("push(%s,%s,%d)"%(host,port,length))
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((njmond_host, njmon_port))

    totalsent = 0
    while totalsent < length: # the socket will send the data in chunks
        sent = sock.send(line.encode())
        if sent == 0:
            raise RuntimeError("socket connection broken")
        totalsent = totalsent + sent

    sock.close()

print("njmon2influxturbo: injected=%d records from file size=%d"%(count,bytes))
