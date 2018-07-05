#!/usr/bin/env bash

select item in Continuar Finalizar
do

  if [[ $item = "Finalizar" ]]; then
    #statements
    break
  fi
done
