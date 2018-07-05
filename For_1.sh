#!/usr/bin/env bash

#
#
#

for HOST in www.google.com www.altavista.com www.yahoo.command

do

  echo "----------------"
  echo $HOST
  echo "----------------"

  /usr/bin/host $HOST

  echo "----------------"

done
