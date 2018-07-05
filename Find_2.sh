#!/usr/bin/env bash
#
# Listar ficheros por el tipo de permiso
#
find . -type f -perm 0644 -print
