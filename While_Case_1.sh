#!/usr/bin/env bash

num=0

while [[ $num -eq 0 ]]; do
  #statements
  echo "Selecciona s|S o n|N"

  read  opcion

  case $opcion in

    s|S)

      echo "pulso la opcion S";;

    n|N)

      echo "pulso la opcion N";;

    *)

      let num=2
      echo "desconoco esa opcion";;

  esac


done
