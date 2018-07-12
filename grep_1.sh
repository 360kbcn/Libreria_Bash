#!/usr/bin/env bash
#
#
#
echo "Introduce la palabra a buscar"
read palabra
grep $palabra Dantes.txt
echo "-----------"
echo "Sensible a Mayusculas y Minusculas de $palabra "
echo
echo
grep -i $palabra Dantes.txt
echo "-----------"
echo "Insensible a mayusculas y minusculas de $palabra"
echo
echo
grep -c $palabra Dantes.txt
echo "-----------"
echo "Numero de $palabra Sensibles a Mayusculas y Minusculas"
echo
echo
grep -ci $palabra Dantes.txt
echo "-----------"
echo "Numero de $palabra Insensibles a Mayusculas y Minusculas"
echo
echo
grep -n $palabra Dantes.txt
echo "-----------"
echo "Prefijar numero de linea"
echo
echo
grep -o $palabra Dantes.txt
echo "-----------"
echo "Solo La parte de coincide Sensible a mayusculas y minusculas"
echo
echo
grep -io $palabra Dantes.txt
echo "-----------"
echo "Solo La parte de coincide Sensible a mayusculas y minusculas"
echo
echo
grep -E '$palabra|desesperarse' Dantes.txt
echo "-----------"
echo "Solo La parte que coincide Sensible a mayusculas y minusculas"
echo
echo
grep -v $palabra Dantes.txt
echo "-----------"
echo "Invertir la busqueda Sensible a mayusculas y minusculas "
echo
echo
grep -iv $palabra Dantes.txt
echo "-----------"
echo "Invertir la busqueda Sensible a mayusculas y minusculas "
echo
echo
grep $palabra --color=always Dantes.txt
echo "-----------"
echo "Invertir la busqueda Sensible a mayusculas y minusculas "
echo
echo
