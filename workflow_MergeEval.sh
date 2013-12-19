#!/bin/bash

for splitdir in /home/mfeys/work/data/mediargus_2011_be/splits/*
do
	for min_sim in $(seq 0.1 0.1 0.5)
	do
		for min_score in 0.1
		do
			for min_size in 2
			do
				/home/mfeys/work/Mediargus_code/merge_eval.sh $splitdir $min_sim $min_score $min_size
			done
		done
	done
done

