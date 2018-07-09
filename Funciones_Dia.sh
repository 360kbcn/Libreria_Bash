#!/usr/bin/env bash
#
#
#
function traza{
  #statements
  PID=$$
  TIMESTAMP=$(date + "%Y%m%d%H%M%S")

  CABECERA=$PID"."$TIMESTAMP"| "
  echo $(CABECERA)$*

}
