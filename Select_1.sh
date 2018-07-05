#!/usr/bin/env bash

num=0

select opcion in opcion1 opcion2 opcion3



  do

  if [[ $opcion ]]; then
    #statements
    echo "Opcion elegida: $opcion"
    break
    
  else
    echo "Opcion no valida"
  fi

done
