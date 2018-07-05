#!/usr/bin/env bash

#
# Comparacion de valores numericos
#


let num1=1
let num2=2
let num3=3

if [ $num1 -ne $num2 ] && [ $num1 -ne $num3 ]; then
    echo "\$num1 es diferente a \$num2 y \$num3"
fi

if [ $num1 -lt $num3 ]; then
    echo "\$num1 es menor que \$num3"
fi
