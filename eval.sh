#!/bin/bash

event_dir=/home/mfeys/work/data/mediargus_2011_be/events
ENV=/home/mfeys/work/data/mediargus_2011_be/mediargus_2011_be

DATE_CMD="date +%Y%m%d%H:%M:%S"
log_start() {
    echo -e "* `date +'%Y-%m-%d %H:%M:%S'` \tSTART\t $* @$ENV"
    start_sec=`date +%s`
}

log_finish() {
    echo -e "* `date +'%Y-%m-%d %H:%M:%S'` \tFINISH\t $* @$ENV"
    end_sec=`date +%s`
    echo cost `expr $end_sec - $start_sec` seconds
}

INST="evaluate events vs the gold events"
log_start $INST
for edir in $event_dir/*
	do
		cd $edir
			event_name=$(echo $edir | cut -d "/" -f 8)
			sname=$(echo $event_name | cut -d "d" -f 1)
			msim=$(echo $event_name | cut -d "d" -f 2| cut -d "m" -f 1)	
			mscore=$(echo $event_name | cut -d "d" -f 2| cut -d "m" -f 2|cut -d "s" -f 1)
			msize=$(echo $event_name | cut -d "d" -f 2| cut -d "m" -f 2|cut -d "s" -f 2)	
			CMD="python /home/mfeys/work/Mediargus_code/evaluate.py $event_name splitname=$sname min_sim=$msim min_score=$mscore min_size=$msize evfile=$edir/aevents events_index=$edir/events_index dataset=mediargus_2011_be"
			echo $CMD; $CMD
	done
log_finish $INST
