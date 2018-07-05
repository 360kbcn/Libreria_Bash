#/bin/bash
#Establecemos directorios de origen y destino
ORIGEN=/c/Librerias_Java/Bash/origen
DESTINO=/c/Librerias_Java/Bash/destino
#Nos posicionamos en el de origen
cd $ORIGEN
#De todos los archivos, solo queremos aquel que se #llame ARCHIVO
for file in *
do
 ARCH_DESTINO= “$DESTINO/$file”
 # -f nos filtra los archivos regulares, ya que de
#nada nos sirven los directorios. –nt nos filtra
#los archivos “más nuevos” que aquellos
#encontrados en la carpeta destino
 if [ -f $file ] && [ $file –nt $ARCH_DESTINO ]; then
     echo “Copiando $file…”
     #copiamos el archive con cp
     cp $file $ARCH_DESTINO
 fi
done
#Hacemos cd para salir de la carpeta de origen
cd
