#!/usr/bin/env bash

clear

function suma { # Ejemplo 1 si n return
  #statements
  let num3=$num1+$num2

  # return $sum3
}

function resta { # Elemplo 2 con return
  #statements
  let num3=$num1-$num2  #Local solo esta activo dentro de la function


}

function multi {
  #statements
  let num3=$num1*$num2


}


function numeros {
  #statements
  echo "Introduce el numero 1"
  read num1
  echo "Introduce el numero 2"
  read num2

  # return $num1, $num2
}

switch=0
final=1

while [ $switch -lt $final ]; do



    #echo "Calculador"
    echo "Selecciona 1.Suma, 2.Resta, 3.Multiplicacion, 4.Salir"

    read opc

    case $opc in

      1)

      echo "Sumas"

      numeros

      suma

      echo
      echo "El resultado de la suma es $num3"
      echo
      echo "Pulsa una tecla"

      read

      clear;;

      2)

      echo "Resta"

      numeros

      resta

      echo
      echo "El resultado de la resta es $num3"
      echo
      echo "Pulsa una tecla"

      read

      clear;;

      3)

      echo "Multiplica"

      numeros

      multi

      echo
      echo "El resultado de la multiplicacion es $num3"
      echo
      echo "Pulsa una tecla"

      read

      clear;;

      4)

      echo "Fin de programa"

      let switch=1;;

    esac

done
