import csv
import requests
import sys
import time
import uuid
import json

def fmt(values):
    val = []
    for value in values:
        val.append([value[0], float(value[1])])
    return val

def ifmt(values):
    val = []
    for value in values:
        val.append([value[0], int(value[1])])
    return val

def FilterCPUListByMode(list, mode):

    val = []
    for item in list:
        if item['metric']['mode'] == mode:
            val.append('cpu'+item['metric']['cpu'])
            val.append(fmt(item['values']))
    return val

def FilterDiskList(list):

    val = []
    for item in list:
        val.append(item['metric']['device'])
        val.append(fmt(item['values']))
    return val

# write cpu collector header
# query/ write ts/min, id fields

response = requests.get('{0}/api/v1/query'.format(sys.argv[1]),
                        params={'query': 'node_uname_info'})
instances = response.json()['data']['result']

with open("resource_cpu_details.tsv", "w") as record_file:

    header = 'ts\tinstance\tdevice\tidle\tiowait\tirq\tnice\tsoftirq\tsteal\tsystem\tuser'
    print(header)
    record_file.write(header+'\n')

    for result in instances:
        instance = result['metric']['instance']

        # query/ write cpu collector
        collector = 'CPU'
        response = requests.get('{0}/api/v1/query'.format(sys.argv[1]),
                                params={'query': 'node_cpu_seconds_total{instance="'+instance+'"}[1d]'})
        results = response.json()['data']['result']
        val1 = FilterCPUListByMode(results, 'idle')
        val2 = FilterCPUListByMode(results, 'iowait')
        val3 = FilterCPUListByMode(results, 'irq')
        val4 = FilterCPUListByMode(results, 'nice')
        val5 = FilterCPUListByMode(results, 'softirq')
        val6 = FilterCPUListByMode(results, 'steal')
        val7 = FilterCPUListByMode(results, 'system')
        val8 = FilterCPUListByMode(results, 'user')

        val = val1[0]
        for i in range (1, len(val1)):
            if isinstance(val1[i],str):
                val = val1[i]
            else:
                mv = max(len(val1[i]), len(val2[i]), len(val3[i]), len(val4[i]), len(val5[i]), len(val6[i]), len(val7[i]), len(val8[i]))
                pv1, pv2, pv3, pv4, pv5, pv6, pv7, pv8 = 0, 0, 0, 0, 0, 0, 0, 0
                for j in range(mv):
                    ts = str(val1[i][j][0])
                    #record = ts+'\t'+instance+'\t'+collector+'\t'+val+'\t'+str(val1[i][j][1])+'\t'+str(val2[i][j][1])+'\t'+str(val3[i][j][1])+'\t'+str(val4[i][j][1])+'\t'+str(val5[i][j][1])+'\t'+str(val6[i][j][1])+'\t'+str(val7[i][j][1])+'\t'+str(val8[i][j][1])
                    record = ts+'\t'+instance+'\t'+val+'\t'+str(round(val1[i][j][1]-pv1,3))+'\t'+str(round(val2[i][j][1]-pv2,3))+'\t'+str(round(val3[i][j][1]-pv3,3))+'\t'+ \
                        str(round(val4[i][j][1]-pv4,3))+'\t'+str(round(val5[i][j][1]-pv5,3))+'\t'+str(round(val6[i][j][1]-pv6,3))+'\t'+str(round(val7[i][j][1]-pv7,3))+'\t'+str(round(val8[i][j][1]-pv8,3))
                    pv1, pv2, pv3, pv4, pv5, pv6, pv7, pv8 = val1[i][j][1], val2[i][j][1], val3[i][j][1], val4[i][j][1], val5[i][j][1], val6[i][j][1], val7[i][j][1], val8[i][j][1]
                    if j > 0:
                        print(record)
                        record_file.write(record+'\n')
    
with open("resource_disk_details.tsv", "w") as record_file:

    # write disk collector header
    header = 'ts\tinstance\tdevice\tio_now\tio_time\tread_bytes\tread_time\treads_complete\twritten_bytes\twrite_time\twrites_completed'
    print(header)
    record_file.write(header+'\n')

    # query/ write disk collector
    collector = 'DISK'

    for result in instances:
        instance = result['metric']['instance']

        response = requests.get('{0}/api/v1/query'.format(sys.argv[1]),
                                params={'query': 'node_disk_io_now{instance="'+instance+'"}[1d]'})
        results = response.json()['data']['result']
        val1 = FilterDiskList(results)
        response = requests.get('{0}/api/v1/query'.format(sys.argv[1]),
                                params={'query': 'node_disk_io_time_seconds_total{instance="'+instance+'"}[1d]'})
        results = response.json()['data']['result']
        val2 = FilterDiskList(results)
        response = requests.get('{0}/api/v1/query'.format(sys.argv[1]),
                                params={'query': 'node_disk_read_bytes_total{instance="'+instance+'"}[1d]'})
        results = response.json()['data']['result']
        val3 = FilterDiskList(results)
        response = requests.get('{0}/api/v1/query'.format(sys.argv[1]),
                                params={'query': 'node_disk_read_time_seconds_total{instance="'+instance+'"}[1d]'})
        results = response.json()['data']['result']
        val4 = FilterDiskList(results)
        response = requests.get('{0}/api/v1/query'.format(sys.argv[1]),
                                params={'query': 'node_disk_reads_completed_total{instance="'+instance+'"}[1d]'})
        results = response.json()['data']['result']
        val5 = FilterDiskList(results)
        response = requests.get('{0}/api/v1/query'.format(sys.argv[1]),
                                params={'query': 'node_disk_written_bytes_total{instance="'+instance+'"}[1d]'})
        results = response.json()['data']['result']
        val6 = FilterDiskList(results)
        response = requests.get('{0}/api/v1/query'.format(sys.argv[1]),
                                params={'query': 'node_disk_write_time_seconds_total{instance="'+instance+'"}[1d]'})
        results = response.json()['data']['result']
        val7 = FilterDiskList(results)
        response = requests.get('{0}/api/v1/query'.format(sys.argv[1]),
                                params={'query': 'node_disk_writes_completed_total{instance="'+instance+'"}[1d]'})
        results = response.json()['data']['result']
        val8 = FilterDiskList(results)

        val = val1[0]  
        for i in range (1, len(val1)):
            if isinstance(val1[i],str):
                val = val1[i]
            else:
                mv = max(len(val1[i]), len(val2[i]), len(val3[i]), len(val4[i]), len(val5[i]), len(val6[i]), len(val7[i]), len(val8[i]))
                pv1, pv2, pv3, pv4, pv5, pv6, pv7, pv8 = 0, 0, 0, 0, 0, 0, 0, 0
                for j in range (mv):
                    ts = str(val1[i][j][0])
                    #record = ts+'\t'+instance+'\t'+collector+'\t'+val+'\t'+str(val1[i][j][1])+'\t'+str(val2[i][j][1])+'\t'+str(val3[i][j][1])+'\t'+str(val4[i][j][1])+'\t'+str(val5[i][j][1])+'\t'+str(val6[i][j][1])+'\t'+str(val7[i][j][1])+'\t'+str(val8[i][j][1])
                    record = ts+'\t'+instance+'\t'+val+'\t'+str(round(val1[i][j][1]-pv1,3))+'\t'+str(round(val2[i][j][1]-pv2,3))+'\t'+str(round(val3[i][j][1]-pv3,3))+'\t'+ \
                        str(round(val4[i][j][1]-pv4,3))+'\t'+str(round(val5[i][j][1]-pv5,3))+'\t'+str(round(val6[i][j][1]-pv6,3))+'\t'+str(round(val7[i][j][1]-pv7,3))+'\t'+str(round(val8[i][j][1]-pv8,3))
                    pv1, pv2, pv3, pv4, pv5, pv6, pv7, pv8 = val1[i][j][1], val2[i][j][1], val3[i][j][1], val4[i][j][1], val5[i][j][1], val6[i][j][1], val7[i][j][1], val8[i][j][1]
                    if j > 0:
                        print(record)
                        record_file.write(record+'\n')

with open("resource_mem_details.tsv", "w") as record_file:

    # write memory collector header
    header = 'ts\tinstance\tmemfree_bytes\tbuffer_bytes\tmem_available'
    print(header)
    record_file.write(header+'\n')

    # query/ write memory collector
    collector = 'MEM'

    for result in instances:
        instance = result['metric']['instance']

        response = requests.get('{0}/api/v1/query'.format(sys.argv[1]),
                                params={'query': 'node_memory_MemFree_bytes{instance="'+instance+'"}[1d]'})
        results = response.json()['data']['result']
        val1 = ifmt(results[0]['values'])
        response = requests.get('{0}/api/v1/query'.format(sys.argv[1]),
                                params={'query': 'node_memory_Active_bytes{instance="'+instance+'"}[1d]'})
        results = response.json()['data']['result']
        val2 = ifmt(results[0]['values'])
        response = requests.get('{0}/api/v1/query'.format(sys.argv[1]),
                                params={'query': 'node_memory_MemTotal_bytes{instance="'+instance+'"}[1d]'})
        results = response.json()['data']['result']
        val3 = ifmt(results[0]['values'])
        mv = max(len(val1), len(val2), len(val3))
        for i in range (mv):
            ts = str(val1[i][0])
            record = ts+'\t'+instance+'\t'+str(val1[i][1])+'\t'+str(val2[i][1])+'\t'+str(val3[i][1])
            print(record)
            record_file.write(record+'\n')

with open("resource_load_details.tsv", "w") as record_file:

    # write load collector header
    header = 'ts\tinstance\tmemfree_bytes\tbuffer_bytes\tmem_available'
    print(header)
    record_file.write(header+'\n')

    # query/ write load collector
    collector = 'LOAD'

    for result in instances:
        instance = result['metric']['instance']

        response = requests.get('{0}/api/v1/query'.format(sys.argv[1]),
                                params={'query': 'node_load1{instance="'+instance+'"}[1d]'})
        results = response.json()['data']['result']
        val1 = fmt(results[0]['values'])
        response = requests.get('{0}/api/v1/query'.format(sys.argv[1]),
                                params={'query': 'node_load5{instance="'+instance+'"}[1d]'})
        results = response.json()['data']['result']
        val2 = fmt(results[0]['values'])
        response = requests.get('{0}/api/v1/query'.format(sys.argv[1]),
                                params={'query': 'node_load15{instance="'+instance+'"}[1d]'})
        results = response.json()['data']['result']
        val3 = fmt(results[0]['values'])

        mv = max(len(val1), len(val2), len(val3))
        for i in range (mv):
            ts = str(val1[i][0])
            record = ts+'\t'+instance+'\t'+str(val1[i][1])+'\t'+str(val2[i][1])+'\t'+str(val3[i][1])
            print(record)
            record_file.write(record+'\n')
