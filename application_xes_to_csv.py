#!/usr/bin/env python
import xmltodict
import pandas as pd
from collections import OrderedDict
import sys
import argparse
import os


def parse_arg():
    parser = argparse.ArgumentParser(description='Script to convert the BPI Challenge 2017.xes file to applications and events CSV files')
    parser.add_argument('-f', '--file', required=True, help='Path to the xes file')
    parser.add_argument('-o', '--out-dir', help="Directory to output to, default is the current directory", default=".")

    return parser.parse_args()


def parse_event(event):
    ret = {}
    for k, v in event.iteritems():
        if not isinstance(v, OrderedDict):
            # This is a list
            for att in v:
                # att is an OrderedDict
                ret[att['@key']] = att['@value']
        else:
            ret[v['@key']] = v['@value']
    return ret


def parse_trace(trace):
    trace_info = {}
    for k, v in trace.iteritems():
        if k != 'event':
            if not isinstance(v, OrderedDict):
                for att in v:
                    trace_info[att['@key']] = att['@value']
            else:
                trace_info[v['@key']] = v['@value']

    events = []
    for event in trace['event']:
        ret = parse_event(event)
        ret['ApplicationID'] = trace_info['concept:name']
        events.append(ret)

    return trace_info, events


def parse_log(log):
    traces = log['trace']
    trace_infos = []
    events = []
    for trace in traces:
        trace_info, trace_events = parse_trace(trace)
        trace_infos.append(trace_info)
        events.extend(trace_events)

    trace_columns = ['ApplicationType', 'LoanGoal', 'RequestedAmount', 'ApplicationID']
    trace_df = pd.DataFrame(trace_infos)
    trace_df.columns = trace_columns

    event_columns = ['Accepted', 'Action', 'ApplicationID', 'CreditScore', 'EventID', 'EventOrigin', 'FirstWithdrawalAmount', 'MonthlyCost',
                    'NumberOfTerms', 'OfferID', 'OfferedAmount', 'Selected', 'EventName', 'Lifecycle', 'Resource', 'Timestamp']
    event_df = pd.DataFrame(events)
    event_df.columns = event_columns

    return trace_df, event_df

if __name__ == "__main__":
    ns = parse_arg()

    print ns.out_dir, ns.file

    print 'Parsing log...'
    with open(ns.file) as fd:
        doc = xmltodict.parse(fd.read())

    log = doc['log']

    trace_df, event_df = parse_log(log)

    print 'Write to CSV files...'

    trace_df.to_csv(os.path.join(ns.out_dir, 'applications.csv'), index=False)

    event_df.to_csv(os.path.join(ns.out_dir, 'events.csv'), index=False)