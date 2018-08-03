#!/usr/bin/env bash
#
#
#
#!/bin/sh
####Definimos lor parametros de conexion a la BBDD mysql
SQL_HOST=localhost
SQL_USER="root"
SQL_PASSWORD=""
SQL_DATABASE="contactos"
####Montamos los parametros de conexión.
SQL_ARGS="-h $SQL_HOST -u $SQL_USER -p$SQL_PASSWORD -D $SQL_DATABASE -s -e"
#### Montamos la sentencia SQL y la lanzamos
mysql $SQL_ARGS "SELECT nombre from personas;"
