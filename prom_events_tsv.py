import csv
import requests
import sys
import time
import json
import itertools
import math

def filter(elements, metric):
    v = 0
    for element in elements:
        n = element[1]
        if n != v and n != '+Inf':
            # unpack begin_ts, end_ts
            fn = float(n)
            begin_ts = int(fn) 
            if fn - begin_ts > 0:
                e_tm = round(1 / (fn - begin_ts))
            else:
                e_tm = 0
            end_ts = begin_ts + e_tm
            rc = metric['return_code']
            instance_name = metric['instance']
            instance_ip = metric['ip']
            evt = metric['event']
            svc = metric['service']
            cluster = metric['cluster']
            app = metric['app']
            usr = metric['user']
            if 'cid' in metric:
                cid = metric['cid']
            else:
                cid = ''
            record = str(begin_ts)+'\t'+str(end_ts)+'\t'+rc+'\t'+instance_name+'\t'+instance_ip+'\t'+event+'\t'+svc+'\t'+cluster+'\t'+app+'\t'+usr+'\t'+cid
            print(record)
            record_file.write(record+'\n')
            v = n

def ProcessEventMetrics(event):

    # query/ write event details
    response = requests.get('{0}/api/v1/query'.format(sys.argv[1]),
                            params={'query': 'perf_'+event+'_ts[1d]'})
    if response.json()['data']['result']:
        results = response.json()['data']['result']
        for result in results:
            metric = result['metric']
            filter(result['values'], metric)

#print(sys.argv[1])
with open("event_details.tsv", "w") as record_file:

    # write memory collector header
    header = 'begin_ts\tend_ts\treturn_code\tinstance_name\tinstance_ip\tevent\tservice\tcluster\tapp\tuser\tcid'
    print(header)
    record_file.write(header+'\n')

    for event in ['mediator_descriptor_add',
                  'mediator_descriptor_instantiate',
                  'mediator_descriptor_push',
                  'mediator_descriptor_get',
                  'mediator_metaroot_rcv',
                  'mediator_descriptor_rcv',
                  'mediator_descriptor_drp',
                  'mediator_descriptor_rsn',
                  'mediator_properties_upd',
                  'mediator_relation_add',
                  'mediator_relation_get',
                  'mediator_relation_upd',
                  'mediator_relation_drp',
                  'mediator_associate_add',
                  'mediator_associate_get',
                  'mediator_associate_upd',
                  'associate_add',
                  'associate_get',
                  'associate_get_existing',
                  'associate_upd',
                  'associate_drp',
                  'descriptor_get_root',
                  'descriptor_get',
                  'descriptor_add_root',
                  'descriptor_add',
                  'descriptor_instantiate',
                  'descriptor_push',
                  'descriptor_upd',
                  'descriptor_drp',
                  'properties_upd',
                  'relation_add',
                  'relation_get',
                  'relation_upd',
                  'relation_drp',
                  'store_associate_add',
                  'store_associate_get',
                  'store_associate_get_existing',
                  'store_associate_upd',
                  'store_associate_drp',
                  'store_descriptor_get_root',
                  'store_descriptor_get',
                  'store_descriptor_get_history',
                  'store_descriptor_add_root',
                  'store_descriptor_add',
                  'store_descriptor_instantiate',
                  'store_descriptor_push',
                  'store_descriptor_rcv',
                  'store_descriptor_upd',
                  'store_descriptor_drp',
                  'store_properties_upd',
                  'store_relation_add',
                  'store_relation_get',
                  'store_relation_upd',
                  'store_relation_drp',
                  'reason_reason',
                  'reason_rule_add',
                  'reason_rule_upd',
                  'reason_rule_drp',
                  'reason_descriptor_add',
                  'reason_descriptor_upd',
                  'reason_descriptor_drp',
                  'reason_associate_add',
                  'reason_associate_upd',
                  'reason_associate_drp',
                  'reason_relation_add',
                  'reason_relation_upd',
                  'reason_relation_drp',
                  'context_descriptor_rsn',
                  'web_metaroot_rcv',
                  'web_descriptor_add',
                  'web_descriptor_instantiate',
                  'web_descriptor_push',
                  'web_descriptor_get',
                  'web_descriptor_rcv',
                  'web_descriptor_drp',
                  'web_descriptor_rsn',
                  'web_properties_upd',
                  'web_associate_add',
                  'web_associate_drp',
                  'web_relation_add',
                  'web_relation_get',
                  'web_relation_upd',
                  'web_relation_drp']:
        ProcessEventMetrics(event)
