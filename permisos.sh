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

  clear

  find . -type f -mtime -30 -mtime +0

}

function Listar_Permisos {
  #statements

  clear

  ls -lart

}

function mensaje {
  #statements
  clear

  echo "Los permisos del fichero $archivo han cambiado"
  ls -lart $archivo
}


function fun_case {
  #statements

      case $opc_permiso in

        1)

        chmod 775 $archivo

        mensaje;;

        2)

        chmod 760 $archivo

        mensaje;;

        3)

        chmod 644 $archivo

        mensaje;;

        4)

        chmod 650 $archivo

        mensaje;;

        5)

        chmod 600 $archivo

        mensaje;;

        6)

        chmod 700 $archivo

        mensaje;;

        7)

        echo "Volver al Menu Principal"

        clear

        let switch_1=1;;

      esac
}


function permisos {
  #statements
  #clear
  switch_1=0
  final_1=1
  while [[ $switch_1 -lt $final_1 ]]; do
    #statements
    echo "Introdude el Nombre del Fichero (0 Menu Principal)"
    read archivo
    if [[ $archivo = "0" ]]; then
      #statements
      let opc_permiso=7

      fun_case

    elif [[ $archivo != "0" ]]; then
      #statements

      ls -lart $archivo

      echo
      echo "Selecciones los Permisos para el archivo $archivo"
      echo "1. rwx-rwx-rx"
      echo "2. rwx-rx----"
      echo "3. rw--r---r-"
      echo "4. rw--r-----"
      echo "5. rw--------"
      echo "6. rwx-------"
      echo "7. volver al Menu"

      read opc_permiso

      fun_case

    fi

done
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

    echo "Listado Permisos Ficheros"

    Listar_Permisos

    echo
    echo "Pulsa una tecla"

    read;;


    3)

    echo "Cambiar Permisos"

    permisos;;



    4)

    clear

    let switch=1;;

  esac

done
