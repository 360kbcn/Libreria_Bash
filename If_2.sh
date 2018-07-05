#!/bin/bash

#
# Comparación cadenas Alfanumericas
#

CADENA1="uno"
CADENA2="uno"
CADENA3="TRES"

if [ $CADENA1 = $CADENA2 ]; then
  echo "\$CADENA1 es igual a \$CADENA2"

elif  [ $CADENA1 != $CADENA2 ]; then
  echo "\$CADENA1 no es igual a \$CADENA2"

fi

if [ -z $CADENA3 ]; then
  echo "\$CADENA3 esta vacia"

elif [ -n $CADENA3 ]; then
  echo "\$CADENA3 no esta vacia"

fi
