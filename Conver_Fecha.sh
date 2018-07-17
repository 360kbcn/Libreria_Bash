#!/usr/bin/env bash
#
#
#
DATO=$1

echo $DATO


DATO2=$(echo $DATO | awk '{print substr($1,7,2) substr($1,4,2) substr($1,1,2)}')
echo "${DATO2} fecha obtenida con AWK a partir de ${1}"

# echo $DATO2



function traza() {
  #statements

	PID=$$
	TIMESTAMP=$(date +"%Y%m%d%H%M%S")

	CABECERA=$PID"."$TIMESTAMP"| "
	echo ${CABECERA}$*


}
