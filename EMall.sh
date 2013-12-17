#!/bin/bash

DATE_CMD="date +%Y%m%d%H:%M:%S"
sourcedir="/users/mfeys/data/event_mall"
destdir="/work/data/event_mall"
ENV="/work/data/event_mall"
lines=13706
padtowidth=2
# get batchnode number
clustersize=33
hstnm=$(hostname)
hostid=$(echo ${hstnm:0:18} | egrep -o '[[:digit:]]{1,2}')

log_start() {
    echo -e "* `date +'%Y-%m-%d %H:%M:%S'` \tSTART\t $* @$ENV"
    start_sec=`date +%s`
}

log_finish() {
    echo -e "* `date +'%Y-%m-%d %H:%M:%S'` \tFINISH\t $* @$ENV"
    end_sec=`date +%s`
    echo cost `expr $end_sec - $start_sec` seconds
}

init(){
    echo "start initialisation"
    #data
    sudo cp "$sourcedir/dates" "$destdir/dates"
    sudo cp "$sourcedir/gross_daily_volumes" "$destdir/gross_daily_volumes"
    #code
    sudo cp -r /users/mfeys/eventmall /work/eventmall
    sudo cp -r /users/mfeys/EMall /work/EMall
}
mine_bursts() {
    INST="detect term bursts"
    log_start $INST
    fname="daily_volumes"
    ## get correct subfile
    filepath=$(printf "$sourcedir/subparts/$fname.%0*d\n" $padtowidth $hostid)
    echo $filepath
    if [ ! -f $destdir/$fname ]
    then
        sudo cp "$filepath" "$destdir/$fname"
    fi
    # execute the tvburst-code
    cd /work/eventmall
    ENV=$destdir
    CMD="sudo ./build/tvburst $ENV $ENV/$burstname $TVB_RATIO $TVB_GAMMA $TVB_WINDOW"
    echo $CMD; $CMD
    # copy the new data back
    echo 'copy the data to /users/mfeys'
    filepath=$(printf "$sourcedir/bursts/$burstname/bursts.%0*d\n" $padtowidth $hostid)
    mkdir -p "$sourcedir/bursts/$burstname"
    sudo cp "$destdir/$burstname" "$filepath"
    echo 'DONE MINING BURSTS!!!'
    log_finish $INST
}
cluto() {
    INST="generate events by clustering"
    log_start $INST
    sudo rm -f $ENV/bursts
    sudo rm -rf $ENV/splits/*
    cat $sourcedir/bursts/$burstname/* > $ENV/bursts
    CMD="sudo python /work/EMall/event/split_cluto.py $hostid $clustersize $TOPICS $MIN_LEN"
    echo $CMD; $CMD
    log_finish $INST

    INST="organize events from cluto results"
    log_start $INST
    cd $ENV/splits
    for split in *; do
        echo Organize $split ...
        python /work/eventmall/src/event/organize_cluto.py $ENV $split>$ENV/splits/$split/twords 2> $ENV/splits/$split/error.txt
    done
    log_finish $INST
    # copy the new data back
    mkdir -p $sourcedir/splits/$splitname
    sudo cp -r "$destdir/splits/*" "$sourcedir/splits/$splitname/"
}

merge_events() {
    INST="generate events from cluto results"
    log_start $INST
    CMD="./src/event/gen_events.py $ENV -s2 -d.1 -m.1"
    echo $CMD; $CMD
    log_finish $INST
}

### commands to execute ###
# use the first source of the config file as the first parameter
source $1
cluto
 
