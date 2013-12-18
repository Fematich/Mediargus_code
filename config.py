#!/usr/bin/env python
"""
@author:    Matthias Feys (matthiasfeys@gmail.com), IBCN (Ghent University)
@date:      %date
"""
import os

envdir=os.path.abspath('/work/data/mediargus_2011_be/')
datadir=os.path.join(envdir,'poor')
indexdir=os.path.join(envdir,'index')
burstindexdir=os.path.join(envdir,'burstindex')
dateindexdir=os.path.join(envdir,'dateindex')
vectordir=os.path.join(envdir,'vectors')
dociddir=os.path.join(envdir,'docids')
splitdir=os.path.join(envdir,'splits')
fdates=os.path.join(envdir,'dates')
fgvols=os.path.join(envdir,'gross_daily_volumes')
fvols=os.path.join(envdir,'daily_volumes')
fainfo=os.path.join(envdir,'ainfo')
fgold=os.path.join(envdir,'event_file.csv')
fevent_index=os.path.join(envdir,'events_index')
faevents=os.path.join(envdir,'aevents')
fterm=os.path.join(envdir,'termlist')
fbursts=os.path.join(envdir,'bursts')

eventmall_dir='/work/eventmall'
sourcedir='/users/mfeys/data/mediargus_2011_be'
min_df=20
max_df_perc=0.15
