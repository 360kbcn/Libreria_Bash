#!/usr/bin/env bash
#
#
#
Escribe el argumento $1
function traza() {

 PID=$$
 TIMESTAMP=$(date +"%Y%m%d%H%M%S")

 CABECERA=$PID"."$TIMESTAMP"| "
 echo ${CABECERA}$*

}
