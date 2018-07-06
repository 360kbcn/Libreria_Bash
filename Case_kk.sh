#!/usr/bin/env bash


echo "Selecciona s|S o n|N"

read  opcion

  case $opcion in

    s|S)

      echo "pulso la opcion S";;

    n|N)

      echo "pulso la opcion N";;

    *)

      echo "desconoco esa opcion";;

esac
