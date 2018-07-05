#!/usr/bin/env bash

clear

function suma { # Ejemplo 1 si n return
  #statements
  let num3=$num1+$num2

  echo "La suma de tus numeros es $num3"
}

function resta { # Elemplo 2 con return
  #statements
  local num3=$(( $num1-$num2 )) #Local solo esta activo dentro de la function

  return $num3 #se necesita el return para devolver el valor
}

function multi {
  #statements
  let num3=$num1*$num2

  return $num3 # Tambien podemos user el return sin necesidad de que la variable sea local

}

opc="Sumar_Numeros Resta_Numeros Multiplica_Numeros Salir"

select opt in $opc; do


  if [ "$opt" = "Sumar_Numeros" ]; then

    echo "Introduce el numero 1"
    read num1
    echo "Introduce el numero 2"
    read num2

    suma

  elif [ "$opt" = "Resta_Numeros" ]; then
    #statements
    echo "Introduce el numero 1"
    read num1
    echo "Introduce el numero 2"
    read num2

    resta

    echo "La resta es $?"


  elif [ "$opt" = "Multiplica_Numeros" ]; then
    #statements
    echo "Introduce el numero 1"
    read num1
    echo "Introduce el numero 2"
    read num2

    multi

    echo "La Multiplicacion es $num3"

  elif [ "$opt" = "Salir"  ]; then
    #statements
    clear
    exit
  else
    echo "Opcion no disponible"
    exit
  fi
done
