#!/usr/bin/env bash
#
# Pedir ficheros por tipo de permisos
#

clear

<<renombrado
function permisos {
  #statements
  #Asignacion de permisos al ficheros

}
renombrado


function listar {
  #statements
  #Listanmos el directorio

  find . -type f -mtime -30 -mtime +0

}

function Listar_Permisos {
  #statements
  

}

switch=0
final=1

while [[ $switch -lt $final ]]; do
  #statements
  echo "1.Listar Ficheros"
  echo "2.Listar Permisos de Fichero"
  echo "3.Cambiar Permisos"
  echo "4.Salir"
  echo

  read opc

  case $opc in

    1)

    echo "Listado"

    listar

    echo
    echo "Pulsa una tecla"

    read;;

    2)

    echo "Pendiente";;

    3)

    echo "Pendiente";;

    4)

    echo "Permisos"

    let switch=1;;

  esac

done
