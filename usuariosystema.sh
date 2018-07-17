#! /bin/bash

# script que retorna
#     un SI si el primer parámetro coincide con algún usuario del sistema o
#     un NO en caso contrario

function ayuda() {

cat << DESCRIPCION_AYUDA
SYNOPSIS
	$0 NOMBRE_USUARIO
DESCRIPCION
   Devuelve:
       SI si NOMBRE_USUARIO coincide con algún usuario del sistema o
       NO si NOMBRE_USUARIO no coincide con ningún usuario del sistema
CÓDIGOS DE RETORNO
	1 Si el número de parámetros es distinto de 1
DESCRIPCION_AYUDA

}

# si número de parámetros distinto 1
if [ $# -ne 1 ]; then
	echo "El número de parámetros debe de igual a 1"
	ayuda
	exit 1
fi

ESTA_EN_SISTEMA=`grep -E ^$1: /etc/passwd`

if [ -z "$ESTA_EN_SISTEMA" ]; then
	echo "NO"
else
	echo "SI"
fi
