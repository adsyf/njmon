#!/usr/bin/python3
'''njmond.py opens a socket from remote njmon stats agents and injects the data in to influxDB 2.1'''
import socket
import queue
import sys
from sys import argv
import string
import json
import time
import multiprocessing
import argparse
from datetime import datetime

from influxdb_client import InfluxDBClient, WriteOptions
from influxdb_client.client.write_api import ASYNCHRONOUS

# import thread module
from _thread import *
#import threading

worker_id = 0
queue = multiprocessing.Queue()
queue_size = 0
queue_warning = 1024
queue_max = 4096

def logger(string1, string2):
    '''Append log information and error to the njmond log file'''
    global config

    if config["logging"] == False:
        return
    
    if string1 == "DEBUG" and config["debug"] is False:
        return

    log_file = config["directory"] + "njmond.log"

    with open(log_file, 'a') as logfile:
        timestr = datetime.now()
        logfile.write(str(timestr)+': '+string1 + ":" + string2 + "\n")
    return

def thread_stats():
    '''Output stats regardng threads'''
    global queue
    global queue_size
    global queue_warning

    queue_warning = 20
    logger('INFO', 'Statistics thread started')
    while True:
        time.sleep(10)
        queue_size = queue.qsize() 
        if queue_size > queue_warning:
            logger('WARNING', ' Queue length='+str(queue.qsize())+ " Warning size=" + str(queue_warning))

        logger('DEBUG', 'Queue length='+str(queue.qsize()))

def parse_worker(queue):
    '''This is one of the logger threads'''
    dbhost = "localhost"
    global config
    global worker_id
    global queue_size
    global queue_max

    # influxdb 2.1
    try:
        client = InfluxDBClient(url=config["influx2_url"], token=config["influx2_token"])
    except:
        print("InfluxDBClient() open failed")
        sys.ext(345)

    try:
        api = client.write_api(write_options=WriteOptions(ASYNCHRONOUS))
    except:
        print("write_api(set async) failed")
        sys.ext(346)

    try:
        bucket=config["influx2_bucket"]
        org=config["influx2_org"]
    except:
        print("set bucker & org failed")
        sys.ext(347)

    logger('DEBUG', 'Worker id='+str(worker_id)+'started')

    entry = []
    taglist = {}
    serial_no = 'unknown'
    mtm = 'unknown'
    os_name = 'unknown'
    arch = 'unknown'

    while True:
        time.sleep(0.02)
        while not queue.empty():
            string = queue.get()
            logger("DEBUG", "Unqueue packet" + "length=%d"%(len(string)))
            try:
                sample = json.loads(string)
            except:
                logger("INFO", "JSON parse failure" + "length=%d"%(len(string)))
                # log the first 2 kb as the data includes the source vm
                logger("INFO", string[0:2048])
                continue

            timestamp = sample["timestamp"]["UTC"]

            try:
                os_name = sample["config"]["OSname"]
                arch = sample["config"]["processorFamily"]
                mtm = sample["server"]["machine_type"]
                serial_no = sample["server"]["serial_no"]
                aix = True
            except: 
                # Not aix imples linux
                os_name = sample["os_release"]["name"]
                if os_name == "Red Hat Enterprise Linux":
                    os_name = "RHEL"
                if os_name == "Red Hat Enterprise Linux Server":
                    os_name = "RHEL"
                if os_name == "SUSE Linux Enterprise Server":
                    os_name = "SLES"
                arch = "unknown"
                aix = False

            if aix is False:
                try:
                    # linux under PowerVM
                    serial_no = sample['ppc64_lparcfg']['serial_number']
                    mtm = sample['ppc64_lparcfg']['system_type']
                except:
                    serial_no = "unknown"
                    mtm = "unknown"
            # logger("INFO", "Linux 100")
            if arch == "unknown":
                try:
                    arch = sample['lscpu']['architecture']
                except:
                    arch = "unknown"
            # logger("INFO", "Linux 200")
            if serial_no == "unknown":
                try:
                    serial_no = sample['identity']['serial-number']
                except:
                    serial_no = "unknown"
            # logger("INFO", "Linux 300")
            if mtm == "unknown":
                try:
                    mtm = sample['identity']['model']
                except:
                    mtm = "unknown"
            # logger("INFO", "Linux 400")

            try:
                # logger("INFO", "trying mtm")
                mtm = mtm.replace('IBM,', '')
            except:
                logger("INFO", "failed on mtm")
                continue

            try:
                # logger("INFO", "trying serial_no")
                serial_no = serial_no.replace('IBM,', '')
            except:
                logger("INFO", "failed on serial_no")
                continue

            try:
                # logger("INFO", "trying hostname")
                host = sample['identity']['hostname']
            except:
                logger("INFO", "failed on hostname")
                continue

            try:
                # logger("INFO", "trying loop")
                snap = sample["timestamp"]["snapshot_loop"]
            except:
                logger("INFO", "failed on loop")
                continue

            try:
                # logger("INFO", "trying njmon version")
                njmonv = sample['identity']['njmon_version']
            except:
                logger("INFO", "failed on njmon vesion")
                continue

            logger("snapshot", "%d,%s,%s,%s,%s,%s,%d,%s"%(config["njmon_port"], host, os_name, arch, mtm, serial_no, snap, njmonv))

            for section in sample.keys():
                logger ("DEBUG", "section" + section)
                for sub in sample[section].keys():
                    logger("DEBUG", "members are type" + str(type(sample[section][sub])))
                    if type(sample[section][sub]) is dict:
                        fieldlist = sample[section][sub]
                        measurename = str(section)
                        # Rename so all the cpu stats start "cpu..."
                        name = measurename
                        if name[-1] == "s": # has a training "s" like disks or networks
                            name = name[0:-1] 
                        name = name + "_name"
                        taglist = {'host': host, 'os': os_name, 'architecture': arch, 'serial_no': serial_no, 'mtm': mtm, name: sub}
                        if name == "vios_virtual_fcadapter_name":
                          taglist.update({'client_part_name': sample['vios_virtual_fcadapter'][sub]['client_part_name']})
                        if name == "network_adapter_name":
                          taglist.update({'network_adapter_type': sample['network_adapters'][sub]['adapter_type']})

                        measure = {'measurement': measurename, 'tags': taglist, 'time': timestamp, 'fields': fieldlist}
                        entry.append(measure)
                    else:
                        fieldlist = sample[section]
                        measurename = str(section)
                        # Rename so all the cpu stats start "cpu..."
                        taglist = {'host': host, 'os': os_name, 'architecture': arch, 'serial_no': serial_no, 'mtm': mtm}
                        measure = {'measurement': measurename, 'tags': taglist, 'time': timestamp, 'fields': fieldlist}
                        entry.append(measure)
                        break
            while True:
                try:
                    if api.write(bucket=bucket, org=org, record=entry) is False:
                        logger("DEBUG","WID="+str(worker_id)+", write.points() to Influxdb2 failed length="+str(len(entry)))
                    else:
                        logger("DEBUG", "WID="+str(worker_id)+",Injected snapshot for " + host)
                    entry.clear()
                    break
                except Exception as e:
                    logger('ERROR ', 'Exception WID='+str(worker_id)+'Error in write INFLUXDB: '+str(e))
                    logger('ERROR', 'TotalSize: ' + str(len(entry)))
                # if too many in the queue, dump the entry and carry on
                # if only a few in the queue but hit exception, sleep and then try again until InfluxDB is works
                logger('ERROR', 'Large Queue queue_size: ' + str() + ' queue_max:' + str(queue_max))
                if queue_size >= queue_max:
                    entry.clear()
                    logger('ERROR', 'Large Queue')
                    break
                logger('ERROR', 'Small Queue sleeping')
                time.sleep(15)

    clientdb.close()

def clean_hostname(hostname):
    PERMITTED = "" + string.digits + string.ascii_letters + '_-.'
    safe = "".join(c for c in hostname if c in PERMITTED)
    return safe.replace('..','')

def threaded(conn):
    '''Get the message from the queue'''
    global config

    buffer = ""

    while True:
        try:
            data = conn.recv(655360)
            if not data:
                # zero length packets are silently ignored
                # logger('ERROR ','Read but no data in socket. Closing socket.')
                break
        except:
            logger('ERROR ', 'Error reading from socket.')
            break
        buffer = buffer + data.decode('utf-8')
        if buffer[-1:] != "\n":
            continue # not a complete JSON record so recv some more

        # Process buffer read from njmon client, parse json and insert into DB if packet is complete.
        if config["data_inject"]:
            logger('INFO', "QUEUE buffer size=" + str(len(buffer)))
            queue.put(buffer)

        if config["data_json"]:
            try:
                sample = json.loads(buffer)
            except:
                logger('INFO', "Saving .json but record does not parse")
                logger('INFO', buffer)
                continue

            host = sample['identity']['hostname'] 
            #clean hostname
            json_file = config["directory"] + clean_hostname(host) + ".json"
            
            logger('DEBUG', "Opening file "+json_file)
            try:
                jsonfd = open(json_file, "a")
            except Exception as e:
                logger('ERROR', "Opening file "+json_file+". Error: "+str(e))

            try:
                jsonfd.write(buffer)
            except:
                logger('ERROR', 'Error writing to file: '+json_file)

            try:
                jsonfd.close()
            except:
                logger('ERROR', 'Error closing json file: '+json_file)
        buffer = ""

    logger('DEBUG', 'Exiting Thread')
    # socket connection closed
    conn.close()

def hints():
    print('njmond.py HELP Information - Version 81')
    print('Syntax:')
    print('    njmond.py configfile')
    print('')
    print('Sample config file - njmond.conf:')
    print('{')
    print('# Lines with 1st char are ignored')
    print('  "njmon_port": 8181,')
    print('  "data_inject": true,')
    print('  "data_json": false,')
    print('  "directory": "/home/nigel/njmon/data",')
    print('  "influx2_url": "http://localhost:8086",')
    print('  "influx2_bucket": "njmon",')
    print('  "influx2_org": "default",')
    print('  "influx2_token": "MANDATORY",')
    print('  "workers": 5,')
    print('  "logging": true,')
    print('  "debug": false')
    print('}')
    print('')
    print('Note: workers=2 tested for 600++ endpoints at once a minute (max workers=32)')
    print('Note: if "inject": false then influx_ details are ignored"')
    print('Note: if "json": true then .json files placed in "diretory"')
    print('      Warning: json files are large = risk of filling the file system')
    print('      Recommend daily mv the files to archive dir and compress')
    print('Note: njmond.log created in "directory"')
    print('Note: debug just addes more details to the log')
    sys.exit(6)

 
def Main():
    '''main code starts here'''
    global config
    global worker_id

    # config file parsing
    try:
        configfile = argv[1]
    except:
        print("Missing configfile argument: njmond.py njmod.conf")
        hints()

    if configfile == "-h" or configfile == "-?" or configfile == "help":
        hints()
    # read the conf file removing lise starting with #
    try:
        configs = ""
        with open(configfile,"r") as f:
            for line in f:
                if line[0] == '#':
                    pass
                else:
                    configs = configs + line
    except:
        print("Failed to read njmond config file:"+str(configfile))
        return 20

    try:
        config = json.loads(configs)
    except:
        print("Failed to parse JSON within njmond config file")
        return 21

    print(config)

    try:
        number = config["njmon_port"]
    except:
        config["njmon_port"] = 8181
        #print("using default njmon_port 8181")

    try:
        mybool  = config["data_inject"]
    except:
        config["data_inject"] = True
        #print("using default data_inject true")

    try:
        mybool  = config["data_json"]
    except:
        config["data_json"] = False
        #print("using default data_json False")

    try:
        string  = config["directory"]
    except:
        config["directory"] = "."
        #print("using default directory .")

    try:
        mybool = config["logging"]
    except:
        config["logging"] = True

    if config["data_inject"] == True:
        try:
            number = config["influx_port"]
        except:
            config["influx_port"] = 8086
            print("using default influx_port 8086")

        try:
            string  = config["influx2_url"]
        except:
            config["influx2_url"] = "http://localhost:8086"
            print("using default http://localhost:8086")

        try:
            string  = config["influx2_bucket"]
        except:
            config["influx2_bucket"] = "njmon"
            print("using default influx2_bucket njmon")

        try:
            string  = config["influx2_org"]
        except:
            config["influx2_org"] = "default"
            print("using default influx2_org default")

        try:
            string  = config["influx2_token"]
        except:
            print("Missing influx2_token in the conf file")
            sys.exit(40)

        try:
            number = config["workers"]
        except:
            config["workers"] = 2
            print("using default workers 2")

    try:
        mybool = config["debug"]
    except:
        config["debug"] = False
        print("using default debug False")

    # Sanity Check
    dir = config["directory"]
    if dir == '//' or \
       dir.startswith('/bin') or \
       dir.startswith('/tmp') or \
       dir.startswith('/dev') or \
       dir.startswith('/etc') or \
       dir.startswith('/lib') or \
       dir.startswith('/usr') or \
       dir.startswith('/sbin'):
        print('Not a good top directory %s (suggest /home/njmon or /var/njmon)'%(dir))
        return 32

    if not config["directory"].endswith("/"):
        config["directory"] = config["directory"] + "/"
    # now logger function will work

    if config["njmon_port"] < 0 or config["njmon_port"] > 60000:
        msg = 'Invalid port number '+port+'(try 1->60000)'
        print(msg)
        logger('ERROR:', msg)
        return 34

    if config["workers"] < 2 and config["workers"] > 32:
        msg = 'Injector workers can\'t less than 2 or exceed 32'
        print(msg)
        logger('ERROR:', msg)
        return 35

    logger('INFO', 'njmond v81')
    parse_workers_list = []
    nproc = 0

    while (nproc < config["workers"]):
        thisWorker = multiprocessing.Process(target=parse_worker, args=[queue])
        parse_workers_list.append(thisWorker)
        thisWorker.start()
        nproc = nproc + 1
        worker_id = worker_id + 1
    try:
       # set up the socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("0.0.0.0", config["njmon_port"]))
        # put the socket into listening mode
        sock.listen(250)
    except:
        logger('ERROR', "socket create, bind or listen failed")

    start_new_thread(thread_stats, ())

    # a forever loop until client wants to exit
    while True:

        # establish connection with njmon client
        conn, addr = sock.accept()

        # Start a new thread and return its identifier
        start_new_thread(threaded, (conn,))

if __name__ == '__main__':
    Main()
