#!/bin/bash

#hstnm=$(hostname)
#hostid=$(echo ${hstnm:0:18} | egrep -o '[[:digit:]]{1,2}')
#if [ $hostid -ne 1 ]
#	then
#		./EMall.sh init /work/data/mediargus_2011_be/configs/config1520
#	fi
for config in /work/data/mediargus_2011_be/configs/*
	do
		./EMall.sh mine_bursts $config
	done

#for config in work/data/mediargus_2011_be/configs/*
#	do
#		./EMall.sh cluto $config
#	done
