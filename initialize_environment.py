#!/usr/bin/env python
"""
@author:    Matthias Feys (matthiasfeys@gmail.com), IBCN (Ghent University)
@date:      %(date)s
"""
import logging, os, sys
sys.path.append(os.path.abspath("../dataprocessing"))
from whoosh.index import open_dir
from whoosh.query import Term
from whoosh.sorting import Facets
from whoosh import sorting
from dataprocessing import mediargus, config as data_config
from config import fdates, fgvols, fvols, fainfo, max_df_perc, min_df, indexdir
from datetime import datetime
import numpy as np

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger=logging.getLogger("initialisation")


def basic_init():
    #######################generate the index#########################
    if not os.path.exists(data_config.mediargus_indexdir):
        logger.info("start indexing data")        
        mediargus.MakeIndex()
    ix = open_dir(indexdir)
    reader=ix.reader()
    searcher=ix.searcher()
    ##################################################################
    #########generate the dates-, gross_daily_volumes-file ###########
    if not (os.path.exists(fdates) or os.path.exists(fgvols)):
        logger.info("start generating the dates-, gross_daily_volumes-files")
        with open(fdates,'w') as datefile, open(fgvols,'w') as gvols:
            try:
                for date in reader.field_terms('date'):
                    datefile.write(date.strftime('%Y%m%d')+'\t')
                    postings=reader.postings('date',date)
                    gvols.write(str(sum(1 for _ in postings.all_ids()))+'\t')
            except Exception:
                logger.error('datetime error!: '+str(date))
    ###################################################################
    ########################generate ainfo-file########################
    if not os.path.exists(fainfo):
        with open(fainfo,'w') as faif:
            for _,doc in reader.iter_docs():
                faif.write('%s%s %s\n'%(doc['date'].strftime('%Y%m%d'),doc['identifier'],doc['date'].strftime('%Y%m%d')))
    ###################################################################
    ####################generate daily_volumes-file####################
    if not os.path.exists(fvols):
        logger.info("start generating the daily_volumes-file")
        myfacet=Facets().add_field("date",maptype=sorting.Count)
        dts  = [datetime.strptime(date,'%Y%m%d').replace(hour=0, minute=0) for date in open(fdates,'r').read().strip().split()]
        dates=dict(zip(dts,range(len(dts))))
        ndocs=reader.doc_count()
        with open(fvols,'w') as daily_vols:
            for term in reader.field_terms('content'):
                term=term.rstrip('\n')
                try:
                    docfreq=reader.doc_frequency('content',term)
                    if max_df_perc*ndocs>=docfreq>=min_df:
                        res=searcher.search(Term('content',term),groupedby=myfacet)
                        vols=np.zeros(len(dts))
                        for day,count in res.groups().iteritems():
                            vols[dates[day]]+=count
                        daily_vols.write('%s %d %s\n'%(term.encode("utf-8"),docfreq,' '.join([str(int(v)) for v in vols])))
                except Exception, e:
                    logger.error(e)
                    pass
    ##################################################################
if __name__ == '__main__':
    basic_init()