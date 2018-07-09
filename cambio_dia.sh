#!/usr/bin/env bash
#
#
#

dopaso=1

# source /c/Librerias_Java/Bash/Funciones_Dia.sh

source /c/Librerias_Java/Bash/config/params.config

sql_args="-h$sql_host-u$sql_usuario-p$sql_password-D$sql_base-s-e"

# Sentencia SQL para mostar datos

mysql$sql_args "SELECT * from 'personas';"
