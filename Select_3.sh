#!/usr/bin/env bash

opc="Hola Salir"

select  opt in $opc; do

    if [ "$opt" = "Salir" ]; then
      echo done
      exit
    elif [ "$opt" = "Hola" ]; then
      echo Hola Mundo
    else
      clear
      echo opción errónea
    fi
done
