#!/usr/bin/env python
"""
@author:    Matthias Feys (matthiasfeys@gmail.com), IBCN (Ghent University)
@date:      %(date)s
"""
import logging, sys, os, shutil

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
logger=logging.getLogger("TODO")
sys.path.append(os.path.abspath("../dataprocessing"))
from dataprocessing import mediargus, utils, config
