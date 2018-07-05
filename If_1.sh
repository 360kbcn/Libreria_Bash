#!/usr/bin/env bash

#
# Comprobando terminacion de un comando
#

DIRECTORIO="/tmp/test"

COMANDO="/bin/mkdir $DIRECTORIO"

if $COMANDO

    then

    echo "$DIRECTORIO ha sido creado"

else

    echo "$DIRECTORIO no pude ser creado o ya existe"

fi
