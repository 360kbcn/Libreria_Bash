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

function numeros{
  #statements
  echo "Introduce el Primer numero"

  read num1

  echo "Introduce el Segundo numero "

  read num2

}

switch=0
final=1

while [ $switch -eq $final ]; do



    #echo "Calculador"
    echo "Selecciona Suma, Resta, Multiplicacion, Salir"

    read opc

    case $opc in

      Suma)

      numero

      suma

      echo "El resultado de la suma es $num3"

      Resta)

      numero

      resta

      echo "El resultado de la resta es $num3"

      Multiplicacion)

      numero

      multi

      echo "El resultado de la multiplicacion es $sum3"

      Salir)

      echo "Fin de programa"

      let $switch=1

done
