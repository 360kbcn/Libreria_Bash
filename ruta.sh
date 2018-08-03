#!/usr/bin/env bash
#
#
origen=/c/Librerias_Java/Bash/destino/datos.sh

echo 'origen'=$origen;

source $origen

Nom=$nombre
Telf=$telefono

echo "$Nom";
echo "$Telf";

let num1=2
let num2=6

suma $num1
suma $num2

echo $resul;
