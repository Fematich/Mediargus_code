#!/usr/bin/env python
"""
@author:    Matthias Feys (matthiasfeys@gmail.com), IBCN (Ghent University)
@date:      %date
"""
import os, logging, sys, nltk
#sys.path.append(os.path.abspath("../dataprocessing"))
from dataprocessing.utils import PoorDoc
import numpy as np
from whoosh.index import open_dir
from whoosh.query import DateRange
from config import indexdir, vectordir, dociddir
from dateutil import rrule
from datetime import datetime
from collections import Counter
from nltk.tokenize import word_tokenize, sent_tokenize
from whoosh.analysis import StandardAnalyzer
from whoosh.util.text import rcompile

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger=logging.getLogger("generate month-vectors")

default_pattern = rcompile(r"\w+(\.?\w+)*")
def RegexTokenizer(text):
    for match in default_pattern.finditer(text):
        term = match.group(0)
        yield term.lower().encode("utf-8")

def getdocvector(date,didentifier):
        doc=PoorDoc(docidentifier=didentifier,date=date)
        tokens = RegexTokenizer(doc.getcontent())
        return Counter(tokens)

def get_months(batchnumber, n_batches):
    '''
    returns a list of monthranges as part of the total monthranges 
    '''
    month_range=[]
    start = datetime(2011,1,1,0,0)
    end = datetime(2012,1,1,0,0)
    first=True
    for dt in rrule.rrule(rrule.MONTHLY, dtstart=start, until=end):
        if first:
            first=False
            last_dt=dt
            continue
        else:
            month_range.append((last_dt,dt))
            last_dt=dt
    host_months=np.array_split(month_range, n_batches)
    return host_months[batchnumber]

def getdocvectors(month):
    '''
    generate file with the vectors of the documents of the current month
    '''
    ix = open_dir(indexdir)
    searcher=ix.searcher()
    res=searcher.search(DateRange("date", month[0],month[1],endexcl=True),limit=None,sortedby='date')
    for doc in res:
        yield doc
    
def generatevecs(month):
    '''
    generate a partial docids and vectors-file for the specific month
    '''
    fvectors=os.path.join(vectordir,'vectors%s-%s'%(month[0].strftime('%Y%m%d'),month[1].strftime('%Y%m%d')))
    fdocids=os.path.join(dociddir,'docids%s-%s'%(month[0].strftime('%Y%m%d'),month[1].strftime('%Y%m%d')))
    with open(fvectors,'w') as vecs, open(fdocids,'w') as docids:
        for doc in getdocvectors(month):
            vc=getdocvector(date=int(doc['date'].strftime('%Y%m%d')),didentifier=int(doc['identifier']))
            vbody=' '.join(['%s %d'%(term, vc[term]) for term in vc ])
            vecs.write('%s%s %s %s\n'%(doc['date'].strftime('%Y%m%d'),doc['identifier'],doc['date'].strftime('%Y%m%d'),vbody))
            docids.write(doc['date'].strftime('%Y%m%d')+str(doc['identifier'])+'\n')


if __name__ == '__main__':
    if not os.path.exists(vectordir):
        os.mkdir(vectordir)
    if not os.path.exists(dociddir):
        os.mkdir(dociddir)
    monthlist=get_months(int(sys.argv[1]),int(sys.argv[2]))
    for month in monthlist:
        generatevecs(month)
    
    logger.info('done!!!')