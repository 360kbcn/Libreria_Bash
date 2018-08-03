#!/usr/bin/env bash
#
#
#
#
ls -l|awk '$5<20000{print $0}'>prue.txt
