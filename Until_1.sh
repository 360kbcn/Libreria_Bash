#!/usr/bin/env bash

num=0

until [[ $num -gt 10 ]]; do
  echo "\$num: $num"
  let num=$num+1
done
