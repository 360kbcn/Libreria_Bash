#!/bin/sh


#Creamos un ciclo que asignará los números del 1 a 5  
#por cada  "vuelta del ciclo"

for contator in 1 2 3 4 5   #declaracion de la variable contador.

do


#Si el valor del contador el igual a 3

if  [$contador -eq 3] 

then

continue

fi

echo "$contador"

done

