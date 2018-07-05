#!/usr/bin/env bash

ATRIBUTOS_SCRIPT=`/bin/ls -l $0` #Aqui se almacenan los valores que pintamos en la variable $0

echo "El usuario '$USERNAME' ha ejecutado el script $0, en el ordenador '$HOSTNAME'. "

# $USERNAME es el usuario y $HOSTNAME es la maquina

echo "Los atributos del script son: "

echo $ATRIBUTOS_SCRIPT

# Nos imprime los valores del script Variables_3.sh
