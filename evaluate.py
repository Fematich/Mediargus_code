#!/usr/bin/env python
"""
@author:    Matthias Feys (matthiasfeys@gmail.com), IBCN (Ghent University)
@date:      Wed Dec 11 12:42:05 2013
"""
from mongostore.mongostore import MongoStore
import logging, subprocess, sys
from config import fgold, fevent_index, faevents
from utils import *

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger=logging.getLogger("TODO")

def wccount(filename):
    out = subprocess.Popen(['wc', '-l', filename],
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT
                         ).communicate()[0]
    return int(out.partition(b' ')[0])

def load_gold_events(goldpath=fgold):
    event_sets=[]
    old_id=-1
    with open(goldpath,'r') as goldfile:
        for line in goldfile:
            e_id,f_id=[int(nmbr) for nmbr in line.strip('\n').split(',')]
            if e_id!=old_id:
                event_sets.append(set([f_id]))
                old_id=e_id
            else:
                event_sets[-1].add(f_id)
    return event_sets

def load_event_sets(evfile=faevents):
    n_events=wccount(fevent_index)
    event_sets=[set([]) for tel in range(n_events)]
    with open(evfile,'r') as eventfile:
        for line in eventfile:
            blocks=line.strip('\n').split()
            f_id=int(blocks[0].split('=')[1])
            e_id=int(blocks[1].split('=')[1].split('/')[0])
            event_sets[e_id-1].add(f_id)
    return event_sets

@MongoStore
def compare_event(name,info,g_count,r_count,big):
    if big:
        gold_events=sig_set
    else:
        gold_events=mod_set
    #filter the articles that aren't in the daterange of the event out...
#    dates_gold=set([dates[doc] for doc in gold_events[g_count]])
    gold=set([])
    dates_gold=set([])
    non_corpus_docs=0
    for doc in gold_events[g_count]:
        try:
            dates_gold.add(dates[doc])
            gold.add(doc)
        except KeyError:
            non_corpus_docs+=1
    retrieved=set([doc for doc in retrieved_events[r_count] if min(dates_gold)<=dates[doc]<=max(dates_gold)])
    ret={}
    ret['n_g_docs']=len(gold)
    ret['n_r_docs']=len(retrieved)
    ret['n_matching_docs']=len(gold)-len(gold-retrieved)    
    ret['cos_sim']=cosine_similarity(gold,retrieved)
    ret['precision']=precision(gold,retrieved)
    ret['recall']=recall(gold,retrieved)
    ret['F1']=F1(ret['precision'],ret['recall'])
    ret['non_corpus_docs']=non_corpus_docs
    return ret
    
@MongoStore
def compare_events(name,info,big):
    if big:
        gold_events=sig_set
    else:
        gold_events=mod_set
    ret={'cos_sim':0,'precision':0,'recall':0,'F1':0,}
    g_count=-1
    non_match=[]
    for g_event in gold_events:
        g_count+=1
        r_count=-1
        max_match=0
        match_event=-1
        for r_event in retrieved_events:
            r_count+=1
            if match(r_event,g_event)>max_match:
                match_event=r_count
                max_match=match(r_event,g_event)
        logger.info('comparing event %d with event %d'%(g_count,match_event))
        if max_match!=0:
            event_res=compare_event(name=name,info=info,g_count=g_count,r_count=match_event,big=big)
            for key in ret:
                ret[key]+=event_res[key]
        else:
            non_match.append(g_count)
    for key in ret:
        ret[key]=float(ret[key])/g_count
    ret['non_match']=non_match
    return ret


if __name__ == '__main__':
    name=sys.argv[1]
    info = dict(x.split('=', 1) for x in sys.argv[2:])
    if 'events_index' in info:
        fevent_index=info['events_index']
    #load golden events
    try:
        goldpath=info['goldpath']
        gold_events=load_gold_events(goldpath)
    except KeyError:
        gold_events=load_gold_events()
    #load retrieved events
    try:
        evfile=info['evfile']
        retrieved_events=load_event_sets(evfile)
    except KeyError:
        retrieved_events=load_event_sets()
    #generate dates dictionary:
    dates=load_dates()
    #match both
    sig_set=[st for st in gold_events if len(st)>=300]
    mod_set=[st for st in gold_events if 10<len(st)<=100]
    print compare_events(name=name,info=info, big=False)
    print compare_events(name=name, info=info, big=True)
    
    logger.info('done!!!')
