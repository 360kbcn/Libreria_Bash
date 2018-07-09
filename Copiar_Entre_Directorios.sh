#!/usr/bin/env bash

# Se establecen directorios de origen y destino

origen=/c/Librerias_Java/Bash/origen
destino=/c/Librerias_Java/Bash/destino

# Nos movenmos hasta el directorio de ORIGEN

source $origen

ls -lart

#Solo nos interesa el fichero file.


echo "Introduce el nombre del fichero "

read opcion

echo "Copiando $opcion..."
#copiamos el archivo con cp
cp $opcion $destino

source cd $destino

echo "Contenido del Directorio $destino"

ls -lart

  # arch_destino= "$destino/$opcion"

  # -f nos filtra los archivos regulares, ya que de
  # nada nos sirven los directorios . -nt nos filtra
  # los archivos "mas nuevos" que aquellos
  # encontrados en la carpeta destino
<<comentado
    if [ -f $opcion ] $$ [ $opcion -nt $destino ]; then
      #statements
      echo "Copiando $opcion..."
      #copiamos el archivo con cp
      cp $opcion $destino
    fi
comentado
