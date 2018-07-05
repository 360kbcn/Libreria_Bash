#!/usr/bin/env bash

num=0

while [ $num -le 10 ]; do
  echo "\$num: $num"

  let num=$num+1

done
