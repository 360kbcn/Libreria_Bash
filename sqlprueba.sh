#!/bin/sh
#### Defino los parametros de conexión a la BD mysql
sql_host=”127.0.0.1”
slq_usuario=”root”
sql_database=”contactos”
### Se monta los parámetros de conexión
sql_args=”-h $sql_host -u $slq_usuario -D $sql_database -s -e”
### Mi sentencia Sql para que la muestre
mysql $sql_args “SELECT * from personas;”
