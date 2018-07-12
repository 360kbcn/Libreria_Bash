#!/usr/bin/env bash

#
#
doPaso8=1
#
#Obtenemos el home del usuario
USER_HOME=$HOME

#. ~/batch/comuns/funcions.sh
source ${HOME}/batch/comuns/funcions.sh


# ruta y nombre del fichero de configuración
CONFIG_FILE=${USER_HOME}/config/batch.conf


# script de tratamiento de peticiones fichero
PATH_BATCH=${USER_HOME}/batch
SH_TRATAR_INDICES=${PATH_BATCH}/ctl/crearIndices.sh
SH_TRATAR_INDICES_NOPART=${PATH_BATCH}/ctl/crearIndicesDatosNoPart.sh


# ruta del directorio de logs
LOG_DIR=$(grep LOG_DIR ${CONFIG_FILE} | awk -F= '{print($2)}')

# cadena de conexión a la base de datos (en formato usuario/password@instancia)
CONEXION_ORACLE=$(grep CONEXION_ORACLE ${CONFIG_FILE} | awk -F= '{print($2)}')

# número de días de vigencia on-line
NUM_DIAS_ONLINE=$(grep NUM_DIAS_ONLINE ${CONFIG_FILE} | awk -F= '{print($2)}')

# número de días de vigencia de las tablas de apéndice
DIAS_VIGENCIA_APENDICE=$(grep DIAS_VIGENCIA_APENDICE ${CONFIG_FILE} | awk -F= '{print($2)}')

# número de días de vigencia de las tablas de nivel
DIAS_VIGENCIA_NIVEL=2

# número de días de vigencia de las tablas de apéndice
SCHEMA=$(grep SCHEMA_TABLAS ${CONFIG_FILE} | awk -F= '{print($2)}')

# Nombre del tablespace  de nivel
NOM_TBLSPACE_NIV=$(grep NOM_TBLSPACE_NIV ${CONFIG_FILE} | awk -F= '{print($2)}')

# Nombre del tablespace de append
NOM_TBLSPACE_APPEND=$(grep NOM_TBLSPACE_APPEND ${CONFIG_FILE} | awk -F= '{print($2)}')

# Indica si el tablespace  es por dia o comun
TBLSPACE=$(grep TIPO_TBLSPACE ${CONFIG_FILE} | awk -F= '{print($2)}')

APLICACION=$(grep APLICACION ${CONFIG_FILE} | awk -F= '{print($2)}')

# Indica el grau de paralelisme a la generació de les estadístiques.
DEGREE=$(grep DEGREE ${CONFIG_FILE} | awk -F= '{print($2)}')

# Valor en % del número de mostres per la generació de les estadístiques.
EST_PERCENT=$(grep EST_PERCENT ${CONFIG_FILE} | awk -F= '{print($2)}')

PID=$$

LOG_FILE=$LOG_DIR/cambio-dia.log
exec 1>>$LOG_FILE 2>&1
traza
traza
traza
traza "Ejecución del cambio de día a "$(date +"%d/%m/%Y %H:%M:%S")

ID_PROC="CambioDia."$PID
TMP_LOG_FILE=$LOG_DIR/sqlplus.$ID_PROC.log


if [[ $TBLSPACE == 'COMUN' ]]; then

	traza "Tablespace Común, no deben realizar los pasos que alteran los tablespaces (paso2, paso4, paso5)"
	doPaso2=0
	doPaso6=0
	doPaso7=0
	doPaso8=0
	doPaso9=0

fi

#schema de las tablas
if [[ $SCHEMA == '' ]]; then
	traza "ERROR: El SCHEMA de tablas '${SCHEMA}' no está informado."
	exit 3
fi

#log
if [[ ! -d $LOG_DIR ]]; then
	traza "ERROR: El directorio de logs '${LOG_DIR}' es incorrecto."
	exit 5
fi

# comprueba el número de días on-line
if [[ $NUM_DIAS_ONLINE -lt 0 ]]; then
	traza "ERROR: Falta el número de días on-line o éste es incorrecto."
	exit 2
fi

# comprueba el número de días de vigencia de los apéndices
if [[ $DIAS_VIGENCIA_APENDICE -lt 0 ]]; then
	traza "ERROR: Falta el número de días de vigencia de apéndices o éste es incorrecto."
	exit 4
fi

# comprueba el número de días de vigencia de los apéndices
if [[ $DIAS_VIGENCIA_NIVEL -lt 0 ]]; then
	traza "ERROR: Falta el número de días de vigencia de las tablas de nivel o éste es incorrecto."
	exit 4
fi


#----------------------------------------------------------------------------------------------------------------------
# Obtención de la fecha de proceso
#----------------------------------------------------------------------------------------------------------------------

# la fecha de proceso es el primer y único parámetro del script, en formato dd/mm/yyyy
FECHA_PROCESO_ARG=$1

# la fecha de proceso es el primer y único parámetro del script, en formato dd/mm/yyyy
DEBUG=$2

if [[ $DEBUG == '-d' ]]; then
	traza "Activado el modo debug, no se eliminan ficheros".
fi


# fecha en formato anglosajón (mm/dd/yyyy), útil para trabajar con el comando 'date'
FECHA_PROCESO_ING=$(echo $FECHA_PROCESO_ARG | awk -F/ '{print $2 "/" $1 "/" $3}')

# comprueba la fecha de proceso
FF=$(date --date="${FECHA_PROCESO_ING}" +"%d/%m/%Y")
RESULTADO=$?
if [[ $RESULTADO -ne 0 ]]; then
	traza "ERROR: La fecha de proceso es incorrecta, el comando "'date'" devolvió la respuesta ${RESULTADO}."
	exit 21
fi
traza "fecha de proceso: "$FF

# fecha del día a borrar los datos
# Hay que borrar los datos de la tabla del dia siguiente para dejarla preparada para el próximo cambio de día.
FECHA_A_BORRAR=$(date +"%m/%d/%Y" --date="$(echo `expr $NUM_DIAS_ONLINE - 1`) days ago ${FECHA_PROCESO_ING} 12:00")
FECHA_A_BORRAR2=$(date +"%m/%d/%Y" --date="tomorrow ${FECHA_PROCESO_ING} 12:00")
FECHA_ESTADISTICAS=$(date +"%m/%d/%Y" --date="yesterday ${FECHA_PROCESO_ING} 12:00")

traza "FECHA_A_BORRAR: Se borran datos del $FECHA_A_BORRAR para poder poner los del $FECHA_A_BORRAR2"
#echo "FECHA_A_BORRAR2: $FECHA_A_BORRAR2"

# fecha del día a bloquear (establecer a solo/lectura) las tablas de nivel
# Se establece con una ventana de dos dias, es decir, se bloquea la tabla de dos dias antes a la actual.
FECHA_A_BLOQUEAR_NIVEL=$(date +"%m/%d/%Y" --date="2 days ago ${FECHA_PROCESO_ING} 12:00")
# fecha del día a bloquear (establecer a solo/lectura) las tablas de apéndice
FECHA_A_BLOQUEAR_APENDICE=$(date +"%m/%d/%Y" --date="${DIAS_VIGENCIA_APENDICE} days ago ${FECHA_PROCESO_ING} 12:00")


#----------------------------------------------------------------------------------------------------------------------
# paso 8: copia estadísticas en la tabla de día vacía
#----------------------------------------------------------------------------------------------------------------------
traza "paso 8: Copía estadísticas en la tabla de día entrante - Tablas dia ${DIA_A_BORRAR}"
traza "doPaso8?: ${doPaso8}"

if [ $doPaso8 -eq 1 ]; then

	QUERY_FILE=${USER_HOME}/batch/ctl/sql/copyStatsFromTemplate${APLICACION}.sql

	# elimina el contenido de cada una de las tablas (nivel y apéndice) del día calculado que hay que limpiar
sqlplus -s $CONEXION_ORACLE > $TMP_LOG_FILE << EOF
	@$QUERY_FILE ${DIA_A_BLOQUEAR_NIVEL} ${DIA_A_BORRAR}
	exit
EOF
	trazafile $TMP_LOG_FILE # igual que un 'cat' pero sin lineas en blanco
	if [[ $(grep -c ORA- $TMP_LOG_FILE) -ne 0 || $(grep -c ERR $TMP_LOG_FILE) -ne 0 ]]; then
		traza "ERROR: Se ha producido algún error durante la ejecución de sqlplus, al copiar las estadisticas."
		# exit 121
	fi
	borra $TMP_LOG_FILE

fi
