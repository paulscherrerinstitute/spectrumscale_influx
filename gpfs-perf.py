#!/usr/bin/python

import sys
import time
from socket import *
import re
import json
import argparse

##########################################################################################################################

Debug=False

##########################################################################################################################

metrics_ops = {
    'nsdfs': ['gpfs_nsdfs_bytes_read', 'gpfs_nsdfs_bytes_written','gpfs_nsdfs_read_ops','gpfs_nsdfs_write_ops'],
    'nsdds': ['gpfs_nsdds_max_disk_wait_rd', 'gpfs_nsdds_max_disk_wait_wr', 'gpfs_nsdds_max_queue_wait_rd', 'gpfs_nsdds_max_queue_wait_wr', 'gpfs_nsdds_tot_disk_wait_rd', 'gpfs_nsdds_tot_disk_wait_wr', 'gpfs_nsdds_tot_queue_wait_rd', 'gpfs_nsdds_tot_queue_wait_wr'],
    'gpfsfs': ['gpfs_fs_bytes_read', 'gpfs_fs_bytes_written', 'gpfs_fs_max_disk_wait_rd', 'gpfs_fs_max_disk_wait_wr', 'gpfs_fs_max_queue_wait_rd', 'gpfs_fs_max_queue_wait_wr', 'gpfs_fs_read_ops', 'gpfs_fs_write_ops'],
    'cpu': ['cpu_user', 'cpu_system', 'cpu_iowait', 'cpu_context', 'cpu_idle', 'cpu_interrupts', 'cpu_nice'],
    'mem': ['mem_active', 'mem_buffers', 'mem_cached', 'mem_dirty', 'mem_memfree', 'mem_memtotal'],
    'net': ['netdev_bytes_r', 'netdev_bytes_s', 'netdev_errors_r', 'netdev_errors_s', 'netdev_packets_r', 'netdev_packets_s', 'netdev_drop_r', 'netdev_drop_s'],
    'netstat': ['ns_closewait', 'ns_established', 'ns_listen', 'ns_local_bytes_r', 'ns_local_bytes_s', 'ns_localconn', 'ns_remote_bytes_r', 'ns_remote_bytes_s', 'ns_remoteconn', 'ns_timewait'],
    'load': ['load1', 'load5', 'load15', 'jobs'],
    'df': ['df_free', 'df_total', 'df_used' ],
    'afm': ['gpfs_afm_bytes_pending', 'gpfs_afm_num_queued_msgs', 'gpfs_afm_longest_time', 'gpfs_afm_avg_time', 'gpfs_afm_used_q_memory','gpfs_afm_bytes_read','gpfs_afm_bytes_written', 'gpfs_afm_shortest_time', 'gpfs_afm_longest_time', 'gpfs_afm_avg_time', 'gpfs_afm_conn_esta', 'gpfs_afm_conn_broken', 'gpfs_afm_fset_expired', 'gpfs_afm_tot_read_time', 'gpfs_afm_tot_write_time'],
    'nfs': ['nfs_read', 'nfs_write', 'nfs_read_ops', 'nfs_write_ops', 'nfs_read_lat', 'nfs_write_lat'],
    'smb': ['op_outbytes', 'op_inbytes', 'request_count', 'current_connections'],
    'waiters': ['gpfs_wt_count_all','gpfs_wt_count_local_io','gpfs_wt_count_network_io','gpfs_wt_count_thcond','gpfs_wt_count_thmutex','gpfs_wt_count_delay','gpfs_wt_count_syscall']
}

metrics_filters = {
    'nsdfs': ['gpfs_fs_name'],
    'nsdds': [],
    'gpfsfs': ['gpfs_fs_name', 'cluster_name'],
    'cpu': [],
    'mem': [],
    'net': [],
    'load': [],
    'df': [],
    'afm': ['node'],
    'netstat': [],
    'nfs': [],
    'smb': [],
    'waiters': [],
}

rowInfoFunctions = {
    'nsdfs': 'nsdfs',
    'nsdds': 'nsdds',
    'gpfsfs': 'gpfsfs',
    'cpu': 'cpu',
    'mem': 'mem',
    'net': 'net',
    'netstat': 'netstat',
    'load': 'load',
    'df': 'df',
    'afm': 'afm',
    'smb': 'smb',
    'nfs': 'nfs',
    'waiters': 'waiters',
}

tagqueries = {
    'nsdfs': 'nsdfs_query',
    'gpfsfs': 'gpfsfs_query',
    'nsdds': 'nsdds_query',
    'cpu': 'cpu_query',
    'mem': 'mem_query',
    'net': 'net_query',
    'netstat': 'netstat_query',
    'load': 'load_query',
    'df': 'df_query',
    'afm': 'afm_query',
    'nfs': 'nfs_query',
    'smb': 'smb_query',
    'waiters': 'waiters_query',
}

##########################################################################################################################

def RepresentsInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False

def RepresentsFloat(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

##########################################################################################################################

def queryInfo(client, query):
    client.sendQuery( query )
    answer = client.receiveAnswer()
    parsed_json = json.loads(answer)
    
    legend = parsed_json['legend']
    keys = []
    for leg in legend:
        keys.append(leg['keys'][0])

    return (keys, parsed_json['rows'])

##########################################################################################################################

class rowInfo_gpfsfs():
    def __init__(self, host, fs, metric, cluster):
        self.host = host
        self.filesystem = fs
        self.metric = metric
        self.cluster=cluster
 
class rowInfo_nsdfs():
    def __init__(self, host, fs, metric):
        self.host = host
        self.filesystem = fs
        self.metric = metric
 
class rowInfo_nsdds():
    def __init__(self, host, nsd, metric):
        self.nsd = nsd
        self.host = host
        self.metric = metric
               
class rowInfo_net():
    def __init__(self, host, nic, metric):
        self.host = host
        self.nic = nic
        self.metric = metric
class rowInfo_waiters():
    def __init__(self, host, threshold, waiter):
        self.host = host
        self.threshold_time = threshold
        self.waiter = waiter

class rowInfo():
    def __init__(self, host, metric):
        self.host = host
        self.metric = metric
   
##########################################################################################################################

class Funcs():
    def gpfsfs(self, array):
        return rowInfo_gpfsfs(array[0], array[3], array[4], array[2])

    def nsdfs(self, array):
        return rowInfo_nsdfs(array[0], array[2], array[3])

    def nsdds(self, array):
        #print array
        return rowInfo_nsdds(array[0], array[2], array[3])

    def cpu(self, array):
        return rowInfo(array[0], array[2])

    def load(self, array):
        return rowInfo(array[0], array[2])
    
    def df(self, array):
        return rowInfo_nsdfs(array[0], array[2], array[3])

    def net(self, array):
        return rowInfo_net(array[0], array[2], array[3])
    
    def netstat(self, array):
        return rowInfo(array[0], array[2])
    
    def afm(self, array):
        return rowInfo(array[0], array[2])

    def mem(self, array):
        return rowInfo(array[0], array[2])
    
    def nfs(self, array):
        return rowInfo(array[0], array[2])

    def smb(self, array):
        return rowInfo(array[0], array[2])

    def waiters(self, array):
        return rowInfo_waiters(array[0], array[2], array[3])

    def nsdfs_query(self, rowinfo):
        return "filesystem=%s,host=%s,operation=%s" % (rowinfo.filesystem, rowinfo.host, rowinfo.metric)
    
    def nsdds_query(self, rowinfo):
        #print(array)
        return "host=%s,nsd=%s,operation=%s" % (rowinfo.host, rowinfo.nsd, rowinfo.metric)
    
    def gpfsfs_query(self, rowinfo):
        return "filesystem=%s,host=%s,operation=%s,cluster=%s" % (rowinfo.filesystem, rowinfo.host, rowinfo.metric, rowinfo.cluster)

    def cpu_query(self, rowinfo):
        return "type=%s,host=%s" % (rowinfo.metric, rowinfo.host)

    def mem_query(self, rowinfo):
        return "type=%s,host=%s" % (rowinfo.metric, rowinfo.host)

    def load_query(self, rowinfo):
        return "type=%s,host=%s" % (rowinfo.metric, rowinfo.host)

    def net_query(self, rowinfo):
        return "interface=%s,host=%s,operation=%s" % (rowinfo.nic, rowinfo.host, rowinfo.metric)

    def df_query(self, rowinfo):
        return "mountpoint=%s,host=%s,info=%s" % (rowinfo.filesystem, rowinfo.host, rowinfo.metric)

    def afm_query(self, rowinfo):
        return "host=%s,metrics=%s" % (rowinfo.host, rowinfo.metric)

    def netstat_query(self, rowinfo):
        return "host=%s,operation=%s" % (rowinfo.host, rowinfo.metric)
    
    def nfs_query(self, rowinfo):
        return "host=%s,operation=%s" % (rowinfo.host, rowinfo.metric)

    def smb_query(self, rowinfo):
        return "host=%s,operation=%s" % (rowinfo.host, rowinfo.metric)

    def waiters_query(self, rowinfo):
        return "host=%s,threshold_time=%s,waiter=%s" % (rowinfo.host, rowinfo.threshold_time, rowinfo.waiter)

##########################################################################################################################

class CollectorClient():
    def __init__(self, server, port=9084, maxChunkSize=16384):
        self.server = server
        self.port   = port
        self.chunk  = maxChunkSize
        self.s = socket(AF_INET, SOCK_STREAM)
        self.answer = ""
        
    def connect(self):
        try:
            self.s.connect((self.server, self.port))
            self.s.settimeout( 1 )
        except error as err:
            print("Error: " + str(err))
            sys.exit(1)
            
    def disconnect(self):
        self.s.close()
                
    def sendQuery(self, query):
        try:
            self.s.send(query)
        except error as err:
            print "Error: " + str(err)
            sys.exit(1)
    
    def receiveAnswerOLD(self):
        self.answer = ""
        while True:
            try:
                out = self.s.recv(self.chunk)
            except:
                break
            if len(out) <= 0:
                self.answer = re.sub(r'\n\.',r'',self.answer)
                return self.answer
            self.answer += out
        self.answer = re.sub(r'\n\.',r'',self.answer)
        return self.answer
    
    def receiveAnswer(self):
        self.answer = ""
        while True:
            out = ""
            try:
                out = self.s.recv(self.chunk)
                if len(out) == 0:
                  if self.answer.endswith("}\n.\n") == True or self.answer.endswith("}\n.") == True:
                    self.answer = re.sub(r'\n\.\n',r'',self.answer)
                    self.answer = re.sub(r'\n\.',r'',self.answer)
                    self.s.close()
                    return self.answer
            except:
                self.answer += out
                if self.answer.endswith("}\n.\n") == True or self.answer.endswith("}\n.") == True:
                  break
            self.answer += out
        self.answer = re.sub(r'\n\.\n',r'',self.answer)
        self.s.close()
        return self.answer
  
##########################################################################################################################

def zimon_metrics(metric_group, table, query_filter, numbuckets, period, cli, Debug):
    global metrics_ops
    global metrics_filters
    query = "get -j metrics %s last %d bucket_size %d %s\n" % (','.join(metrics_ops[metric_group]), numbuckets, period, query_filter)
    if Debug:
        print "Query to Collector                 : %s" %query

    try:
        (keys, rows) = queryInfo(cli, query)        
    except ValueError as e:
        print e
        return

    f = Funcs()
    parsefunc = getattr( f, rowInfoFunctions[metric_group])
    gettagsquery = getattr( f, tagqueries[metric_group])
    
    InfoRows = {}
    index=0
    for key in keys:
        pieces = key.split('|')
        InfoRows[index] = parsefunc( pieces )
        index += 1
    timestamp = 0
    filename = ""
    queries = []
    for x in range(0,numbuckets):
        row = rows[x]
        if sum(row['nsamples']) != 0:
            timestamp = row['tstamp']
            index = 0
            for val in row['values']:
                if RepresentsInt(str(val)) or RepresentsFloat(str(val)):
                    _val = float(str(val))
                    influxQuery = table + "," + gettagsquery(InfoRows[index]) + " value=" + str(_val) + " " + str( timestamp * 1000000000 )
                    queries.append(influxQuery)
                index += 1
    
    for p in queries:
        print p

############################################################################################

def main():
    
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--metric', type=str, required=True,
                        help='name of the metric. Possible metrics: %s'%", ".join(metrics_ops.keys()))
    
    parser.add_argument('--collector', type=str, required=False, default='localhost',
                        help='hostname or ip of the host running the pmcollector process')
    
    parser.add_argument('--collport', type=int, required=False, default=9084,
                        help='TCP port number of the pmcollector process')
    
    parser.add_argument('metric_arguments', nargs='*',
                        help='additional arguments required by specific metrics in the form of key=value')

    parser.add_argument('--delay', type=int, required=False, default=5,
                        help='This is how often a caller invokes this script. This value is internally used to calculate the size of buckets to query to PMCollector')

    parser.add_argument('--period', type=int, required=False, default=1,
                        help='This value must match the ZiMon period for the selected metric (according to output of the command \'mmperfmon config show\'')

    parser.add_argument('--table', type=str, required=False, 
                        help='This name of the table (\'measurement\' in the Influx terminology) where insert data into. Default is the metrics\'s name itself')

    parser.add_argument('--debug', action='store_true', required=False, default=False,
                        help='print out debug information')
    
    args = parser.parse_args()
    metric_arguments={}
    for arg in args.metric_arguments:
        try:
            (key,value)=arg.split('=')
            metric_arguments[key]=value
        except ValueError:
            print("Invalid format for %s. Should be key=value"%arg)
            sys.exit(1)

    metric=args.metric
    if metric not in metrics_ops.keys():
        print("Metric %s not available"%metric)
        sys.exit(1)

    missing_params=set(metrics_filters[metric])-set(metric_arguments)
    if len(missing_params)>0:
        for p in missing_params:
            print "Parameter %s missing"%p
        sys.exit(1)

    missing_params=set(metric_arguments)-set(metrics_filters[metric])
    if len(missing_params)>0:
        for p in missing_params:
            print "Parameter %s not supported"%p
        sys.exit(1)

    Debug = args.debug
        
    collectorhost = args.collector or 'localhost'
    if Debug:
        print "Connecting to collector at endpoint: %s:%d" % (collectorhost, args.collport)
    cli = CollectorClient(collectorhost, args.collport)
    cli.connect()

    delay = args.delay
    period = args.period
    numbuckets = int( delay / period )
    if numbuckets < 1:
        numbuckets = 1
    
    received_params = metric_arguments
    query_filter=""
    array_filter=[]
    for p in received_params.keys():
        array_filter.append("%s=%s" % (p, received_params[p]) )                            
    if len(array_filter)>0:
        query_filter = "from " + ",".join( array_filter )

    table = metric
    if args.table:
        table = args.table
    if Debug:
        print "Will insert data into measurement  : %s" %table
        print "Size of bucket                     : %d" %period
        print "Num of buckets to collect          : %d" %numbuckets
        print "Metric to query                    : %s" %metric
        print "Filters                            : %s" %(query_filter or 'None')

    zimon_metrics(metric, table, query_filter, numbuckets, period, cli, Debug)
 
##########################################################################################################################
   
if __name__ == "__main__":
    main()
        
