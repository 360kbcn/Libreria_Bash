#!/usr/bin/env bash

let A=100

let B=200

#
# funcion suma()
#Suma las Variables A y B
#

function suma() {
  let C=$A+$B
  echo "Suma: $C"
}


#
#Funcion resta()
#Resta las variables Ay b
#

function resta() {
  let C=$A-$B
  echo "Resta: $C"

}

suma

resta
