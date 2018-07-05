#!/usr/bin/env bash
#
#
#

# Pintamos poro pantalla los ficheros del directorio actual de los ultimos 30 dias

find . -type f -mtime -30 -mtime +0
