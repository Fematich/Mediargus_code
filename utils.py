#!/usr/bin/env python
"""
@author:    Matthias Feys (matthiasfeys@gmail.com), IBCN (Ghent University)
@date:      %date
"""

import logging, subprocess
import numpy as np
from config import fainfo

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger=logging.getLogger("utils")

def cosine_similarity(set1,set2,ignore=set([])):
    """
    given two sets of doc identifiers, it returns the cosine similarity between both (between 1 and 0)
    """
    set2=set2-ignore
    return float(len(set1)-len(set1-set2))/np.sqrt(len(set1)*len(set2))

def match(set1,set2):
    """
    given two sets of doc identifiers, it returns the number of overlapping docs
    """
    return len(set1)-len(set1-set2)

def precision(set1,set2,ignore=set([])):
    """
    given two sets of doc identifiers, it returns the precision of the second set, with the first set as the ground truth
    """
    logger.info('source has %d docs, best match has %d docs, %d docs match'%(len(set1),len(set2),(len(set1)-len(set1-set2))))
    set2=set2-ignore    
    return float(len(set1)-len(set1-set2))/len(set2)

def recall(set1,set2,ignore=set([])):
    """
    given two sets of doc identifiers, it returns the recall of the second set, with the first set as the ground truth
    """
    set2=set2-ignore      
    return float(len(set1)-len(set1-set2))/len(set1)

def F1(precision,recall):
    """
    given the precision and recall it returns the F1-error, defined as 2*P*R/(P+R)
    """
    if precision+recall>0:
        return float(2*precision*recall)/(precision+recall)
    else:
        return 0
def load_dates():
    dates = {}
    for line in open(fainfo):
        tokens = line.split()
        aid = int(tokens[0])
        date = int(tokens[1])
        dates[aid] = date
    return dates