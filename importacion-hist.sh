#!/bin/sh

#======================================================================================================================
#
#	Nombre					${USER_HOME}/batch/imp/importacion-hist.sh
#
#	Descripción			Script de recuperación (importación) de datos históricos
#
#				PASO 1)		Inicialmente, elimina los datos antiguos llamando al procedimiento solicitud_hist.elimina_solicitudes_caducas,
#									que:
#										- establece como caducadas las solicitudes cuya caducidad haya sido superada
#										- para las solicitudes de soporte Pantalla:
#											- elimina las claves de la tabla de claves de solicitud
#											- además, si dichas claves no satisfacen otras solicitudes (de soporte Pantalla) y no van a ser
#												reaprovechadas para otras solicitudes a punto de satisfacerse, las elimina de la tabla de históricos
#										- para las solicitudes de soporte Fichero, si el fichero CSV no va a ser reaprovechado ni está siendo
#											utilizado por otras solicitudes (de soporte Fichero), imprime el nombre del fichero en un fichero
#											de salida, para posteriormente ser eliminado
#									Al salir del procedimiento, el proceso elimina uno a uno los ficheros csv caducados.
#
#				PASO 2)		Seguidamente, obtiene dos ficheros temporales:
#
#										- fichero de solicitudes pendientes, llamando al procedimiento solicitud_hist.get_fichero_pendientes;
#											contiene un volcado de los registros de solicitudes en estado Pendiente, tiene el siguiente formato,
#											en el que los campos se corresponden exactamente con los registros de la tabla de solicitudes:
#
#												<<id>>|<<usuario>>|<<tipoTerminal>>|<<terminal>>|<<fecha_ini>>|<<fecha_fin>>|<<soporte>>|<<fichero_csv>>|<<estado>>|<<f.intr>>|<<f.disp>>|<<f.cad>>|<<f.elim>>|<<criterios>>
#
#										- fichero de claves cargadas, llamando al procedimiento solicitud_hist.get_fichero_claves_cargadas;
#											contiene un DISTINCT de las claves cargadas (para soporte Pantalla) en la tabla de históricos, con
#											el siguiente formato:
#
#												<<tipoTerminal>>|<<terminal>>|<<fecha>>
#
#									Estos ficheros contienen toda la información necesaria para la recuperación. En el caso que no hubieran
#									solicitudes pendientes (el fichero estuviera vacío), ya no haría nada más.
#
#				PASO 3)		A partir de los 2 ficheros obtenidos, genera otros ficheros temporales:
#
#										- fichero de claves de solicitudes pendientes: a partir del fichero de solicitudes pendientes, despliega
#											sus claves (un registro por solicitud-tipoTerminal-terminal-fecha) y les calcula la partición, la semana y el nombre
#											del fichero histórico que contiene dicha clave y por lo tanto debe recuperar, quedando con el siguiente
#											formato:
#
#												<<id>>|<<partición>>|<<tipoTerminal>>|<<terminal>>|<<soporte>>|<<semana>>|<<fecha>>|<<fichero>>
#
#												donde:	- partición: es el número obtenido en finción de tipoTerminal/terminal
#																- semana: es el año y número de semana según la ISO (formato +"%G%V" del comando 'date')      <---- Atención: El año es %G en lugar de %Y, ya que el año debe corresponderse con la semana !
#																- fichero: <<semana>>_<<partición>>   [sin extensión!]
#
#										- fichero de ficheros a recuperar: a partir del fichero anterior, selecciona solo semana, partición y
#											nombre del fichero, lo ordena y le aplica un 'uniq' (opción -u de 'sort'), quedando un solo registro
#											para cada fichero a recuperar, con el siguiente formato:
#
#												<<semana>>|<<partición>>|<<fichero>>
#
#										- claves a tratar: a partir también del fichero de claves de solicitudes pendientes, obtiene para cada
#											soporte-fichero, cada una de las claves a tratar, en el siguiente formato:
#
#												<<soporte>>|<<fichero>>|<<tipoTerminal>>|<<terminal>>|<<fecha>>
#
#
#				P.4-pre)	Con toda la información recopilada, empieza la recuperación propiamente dicha. Antes que nada,
#									deshabilita (unusable) los posibles índices de la tabla de históricos para que estos no ralenticen
#									las cargas.
#
#				PASO 4)		- Divide (con 'split') el fichero de ficheros a recuperar en ficheros más pequeños cuyo número de líneas
#										no supere el máximo de lecturas de cinta en paralelo.
#
#									- Recorre cada uno de los ficheros más pequeños (llamémosles 'recuperaciones'). Para cada recuperación:
#
#											- Obtiene los nombres de los ficheros de dicha recuperación, los recupera de las cintas magnéticas y
#												los descomprime (se queda solo con la información en texto).
#
#											- Una vez tenemos los datos, reseguimos, para cada uno de los ficheros:
#
#												- reseguimos para cada una de las claves a tratar que proceden del fichero obtenido:
#
#													- si es para soporte Fichero, filtra, del fichero obtenido, la clave a tratar y concatena los
#														datos obtenidos al gran fichero de datos CSV, que es temporal y será usado más tarde.
#
#													- si es para soporte Pantalla, filtra, del fichero obtenido, la clave a tratar y concatena los
#														datos obtenidos al fichero especifico de carga, que se usará en seguida. Antes pero, comprueba
#														que la clave a tratar no esté ya cargada en la tabla de históricos; para ello, mira que no
#														exista dicha clave en el fichero de claves cargadas.
#
#											- Carga el fichero de carga, que en estos momentos tiene los datos útiles de los ficheros recuperados
#												de la 'recuperación' concurrente, mediante la herramienta 'sqlldr'.
#
#											- Una vez cargados los datos para soporte Pantalla, vuelve a reseguir cada uno de los ficheros recién
#												tratados e inserta cada una de las claves tratadas en la tabla de claves de solicitud, para que
#												queden como cargadas.
#
#									- Finalmente, resigue cada una de las solicitudes de soporte Pantalla y comprueba que hayan sido cargadas
#										todas las claves necesarias. Si es así, establece la solicitud como Disponible.
#
#
#				P.4-post)	Una vez tota la información ha sido cargada a la tabla de históricos, vuelve a habilitar (rebulid) los
#									posibles índices de dicha tabla.
#
#				PASO 5)		En teoria todas la claves necesarias para el tratamiento del fichero ya deberian estar insertadas, ya que solo
#									se puede solicitar ficheros de información mostrada por pantalla (por lo tanto disponible). 
#									Se invoca al script procesarPeticion.sh para realizar su tratamiento:
#
#										- Crea el fichero CSV con los datos solicitados, con el nombre:
#
#												<<tipoTerminal>>_<<terminal>>_<<fecha inicial>>_<<fecha_final>>.csv
#
#										- Le añade la primera línea (fija) que es la cabecera con los nombres de los campos.
#
#										- Concilia y formatea la información de acuerdo con las peticiones del usuario.
#
#									Durante los pasos 4 y 5, a medida que los ficheros temporales dejan de tener utilidad, se van eliminando
#									de manera que el directorio temporal quede vacío al final del proceso.
#
#======================================================================================================================

#----------------------------------------------------------------------------------------------------------------------
# Aquesta comanda és per fer un source del fitxer on tenim les funcions comuns
#----------------------------------------------------------------------------------------------------------------------
#. ~/batch/comuns/funcions.sh
source ${HOME}/batch/comuns/funcions.sh


#----------------------------------------------------------------------------------------------------------------------
# Funciones
#----------------------------------------------------------------------------------------------------------------------

# Escribe el argumento $1
# function traza() {

	# PID=$$	
	# TIMESTAMP=$(date +"%Y%m%d%H%M%S")

	# CABECERA=$PID"."$TIMESTAMP"| "
	# echo "${CABECERA}$* "
	
# }

function inc() {

	PID=$$	
	TIMESTAMP=$(date +"%Y%m%d%H%M%S")

	CABECERA=$PID"."$TIMESTAMP"|INCIDENCIA| "
	echo ${CABECERA}$1 >> $INCIDENCIAS_PROC
	
}

function resum() {

	PID=$$	
	TIMESTAMP=$(date +"%Y%m%d%H%M%S")

	CABECERA=$PID"."$TIMESTAMP"| "
	echo ${CABECERA}$1 >> $RESUMEN_PROC
	
}

# Mira si el proceso puede continuar comprobando si se tiene espacio en disco o se ha superado el limite
# horario. Retorna 0 si no se continua, o 1 si se continua
function checkContinuar() {
	RETORNO=1
	
	DIRECTORIO=$IMP_DIR
	FILESYSTEM_OCUPADO=$(df -Pk $DIRECTORIO | grep '%' | grep '/' | awk '{print $5}' | awk -F% '{print $1}')
	traza "Filesystem ${DIRECTORIO} al ${FILESYSTEM_OCUPADO}%"
	if [[ $FILESYSTEM_OCUPADO -gt $LIMITE ]]; then
			traza "Filesystem de ${DIRECTORIO} al COMPLETO!!! ${FILESYSTEM_OCUPADO}%"
			doMail "Filesystem de ${DIRECTORIO} al COMPLETO!!! ${FILESYSTEM_OCUPADO}%"
			RETORNO=0
	fi

	DIRECTORIO=$TMP_DIR
	FILESYSTEM_OCUPADO=$(df -Pk $DIRECTORIO | grep '%' | grep '/' | awk '{print $5}' | awk -F% '{print $1}')
	traza "Filesystem ${DIRECTORIO} al ${FILESYSTEM_OCUPADO}%"
	if [[ $FILESYSTEM_OCUPADO -gt $LIMITE ]]; then
			traza "Filesystem de ${DIRECTORIO} al COMPLETO!!! ${FILESYSTEM_OCUPADO}%"
			doMail	"Filesystem de ${DIRECTORIO} al COMPLETO!!! ${FILESYSTEM_OCUPADO}%"
			RETORNO=0
	fi
	
	HORA_ANTERIOR=$HORA
	HORA_ACTUAL=$(date +"%H%M%S")
	HORA=`expr $(echo $HORA_ACTUAL ) + 0` 
	traza "HORA:"$HORA
	traza "HORA_ANTERIOR:"$HORA_ANTERIOR
	traza "LIMITE HORARIO:"$LIMITE_HORA

	if [[ $HORA -gt $LIMITE_HORA ]]; then
		traza "Saliendo del bucle de tratamiento por Hora superior a lo espeperado. HORA: "$HORA 
		doMail -e "Saliendo del bucle de tratamiento por Hora superior a lo espeperado. HORA: "$HORA
		
		RETORNO=0
	else
		if [[ $HORA -lt $HORA_ANTERIOR ]]; then
			traza "Saliendo del bucle de tratamiento por Hora superior a lo espeperado (cambio dia). HORA: ${HORA} HORA ANTERIOR:{HORA_ANTERIOR}"  
			doMail "Saliendo del bucle de tratamiento por Hora superior a lo espeperado (cambio dia). HORA: ${HORA} HORA ANTERIOR:{HORA_ANTERIOR}"  
			RETORNO=0
		fi
	fi	
	
	traza "checkContinuar:${RETORNO}"  
	CONTINUAR=$RETORNO
}

# Mira si el proceso puede continuar comprobando si se tiene espacio en disco o se ha superado el limite
# horario. Retorna 0 si no se continua, o 1 si se continua
function checkComenzar() {
	RETORNO=1
	
	HORA_ANTERIOR=$HORA
	HORA_ACTUAL=$(date +"%H%M%S")
	HORA=`expr $(echo $HORA_ACTUAL ) + 0` 
	traza "HORA:"$HORA
	traza "HORA_ANTERIOR:"$HORA_ANTERIOR
	traza "LIMITE HORARIO COMIENZO:"$LIMITE_HORA_COMIENZO
	ps -ef | grep "${USER_HOME}/batch/imp/importacion-hist.sh" | grep -v grep > $CHECK_PROC
	
	NUM_PROCESOS=$(cat  $CHECK_PROC | wc -l )
	traza "NUM_PROCESOS:"$NUM_PROCESOS
	
	if [[ $HORA -gt $LIMITE_HORA_COMIENZO ]]; then
		if [[ $HORA -lt $LIMITE_TARDE ]]; then
			traza "Cadena nocturna empieza más tarde de lo esperado. HORA: "$HORA 
			inc "Hora de comienzo superior a lo espeperado para la cadena nocturna. HORA: ${HORA} > ${LIMITE_HORA_COMIENZO}. El proceso no realiza acciones. "
			RETORNO=0
		fi
	fi	

	if [[ $RETORNO -gt 0 ]]; then
		if [[ $NUM_PROCESOS -gt 1 ]]; then
	
			traza "EL proceso no puede empezar al existir otro en ejecución. Procesos importacion activos: "$NUM_PROCESOS 
			inc "EL proceso no puede empezar al existir otro en ejecución. El proceso no realiza acciones. "
			RETORNO=0
		fi
	fi
	
	traza "checkComenzar:${RETORNO}"  
	COMENZAR=$RETORNO
}

# copia el fichero, del servidor remoto al servidor local, especificado como argumento $1
function obtener_fichero() {
	FICHERO=$1
	DESTINO=$2
	
	
	NUM_SERVERS=$(grep SERVIDOR_BD ${CONFIG_FILE} | wc -l )
	REMOTE_USER=$(grep REPLICABD_REMOTE_USER ${CONFIG_FILE} | awk -F= '{print($2)}')
	RSA_KEY=$(grep REPLICABD_RSA_KEY ${CONFIG_FILE} | awk -F= '{print($2)}')
	NUM_ITERAC=1
	while [[ $NUM_ITERAC -le $NUM_SERVERS ]]; do
		SERVER=$(grep SERVIDOR_BD_${NUM_ITERAC} ${CONFIG_FILE} | awk -F= '{print($2)}')
		if [[ $(ping -c1 $SERVER | grep 'NOT FOUND') != '' ]]; then
			traza "Error: servidor '${SERVER}' no conectado (se ignora)."
			doMail "Error: servidor '${SERVER}' no conectado (se ignora)."
		else	
		
		#traza "Bypass: cambiamos los permisos del directorio remoto"
		#${USER_HOME}/batch/ctl/remotereadothers.sh $SERVER $TMP_DIR_PREF

		
		copia_fichero $SERVER $FICHERO $FICHERO
			#scp -i $RSA_KEY -p $REMOTE_USER@$SERVER:$FICHERO $FICHERO
		fi
		if [ -f $FICHERO ];then
			traza "Obtenido fichero'${FICHERO}' del servidor '${SERVER}'."
			VARIABLE_FITXER="$(echo "$FICHERO "| awk -F'/' '{print ($6)}')"
			$SH_BORRAR_FICHERO_BD ${SERVER} tmp ${VARIABLE_FITXER}
			break;
		fi
	((NUM_ITERAC+=1))
	done	
	if [ ! -f $FICHERO ];then
		traza "Error: no se ha podido obtener el fichero '${FICHERO} de ninguno de los servidores remotos."
		sendMailError "Error: no se ha podido obtener el fichero '${FICHERO} de ninguno de los servidores remotos."
		exit 24;
	fi	

	if [[ $DESTINO != '' ]]; then
		$(mv $FICHERO $DESTINO)
		traza "Movido el fichero '${FICHERO}' al directorio '${DESTINO}'."
	fi
	
}

# copia el fichero, del servidor remoto al servidor local, especificado como argumento $1
function obtener_codigos() {

	NUM_CODES=$(grep CODE ${CODE_FILE} | wc -l )
	NUM_ITERAC=1
	while [[ $NUM_ITERAC -le $NUM_CODES ]]; do
		CODE=$(grep CODE${NUM_ITERAC}"=" ${CODE_FILE} | awk -F= '{print($2)}')
		if [[ $CODE == '' ]]; then
			traza "Error: Codigo 'CODE${$NUM_ITERAC}' incorrecto (se ignora)."
		else	
			if [[ $CODIGOS != '' ]]; then
				CODIGOS=$CODIGOS"|"
			fi
		  CODIGOS=$CODIGOS$CODE
		fi

	((NUM_ITERAC+=1))
	done	
	if [[ $CODIGOS != '' ]]; then	
		CODIGOS="/("$CODIGOS")/"
	fi
}

#Borra ficher en remoto
function borraRemoto() {
	FILE_PATH=$1
	NUM_SERVERS=$(grep SERVIDOR_REPLICA ${CONFIG_FILE} | wc -l )
	REMOTE_USER=$(grep REPLICA_REMOTE_USER ${CONFIG_FILE} | awk -F= '{print($2)}')
	RSA_KEY=$(grep REPLICA_RSA_KEY ${CONFIG_FILE} | awk -F= '{print($2)}')
	NUM_ITERAC=1
	while [[ ${NUM_ITERAC} -le ${NUM_SERVERS} ]]; do
		SERVER=$(grep SERVIDOR_REPLICA_${NUM_ITERAC} ${CONFIG_FILE} | awk -F= '{print($2)}')
		if [[ $SERVER != $(get_hostservicename) ]]; then
			COUNT=$(ssh -l ${REMOTE_USER} -i ${RSA_KEY} $SERVER "wc -l" ${FILE_PATH} | awk '{print $1}')		
			ssh -l ${REMOTE_USER} -i ${RSA_KEY} $SERVER "rm -f "${FILE_PATH}
			RESULTADO=$?
			if [[ $RESULTADO -ne 0 ]]; then
				traza "Error: no se ha podido borrar el fichero '${FILE_PATH}' en '${REMOTE_USER}@${SERVER}', el comando 'ssh' devolvió ${RESULTADO}."
			else
				traza "fichero '"$FICHERO_A_ELIMINAR"' con "$COUNT" registros, eliminado en el servidor ${SERVER}"
			
			fi
		else
			COUNT=$(wc -l ${FILE_PATH} | awk '{print $1}')		
			rm -f ${FILE_PATH}
			RESULTADO=$?
			if [[ $RESULTADO -ne 0 ]]; then
				traza "Error: no se ha podido borrar el fichero '${FILE_PATH}' en '(LOCAL) ${SERVER}', el comando 'ssh' devolvió ${RESULTADO}."
			else
				traza "fichero '"$FICHERO_A_ELIMINAR"' con "$COUNT" registros, eliminado en el servidor ${SERVER}"
			
			fi
		fi
		
		((NUM_ITERAC+=1))
	done
}

function marcarIncidente() {
ID_SOL=$1
RETMSG=$2
	ESTADO_PETICION='I'
	SUPLEM=""
	if [ $RETMSG == 'INC' ]; then
		ASUNTOMAIL="Ha fallado la ejecución de fichero diferido con número de solicitud: ${ID_SOL} por estar incompleta.\nRevisar el log ${LOG_FILE}."
	elif [ $RETMSG == 'SQL' ]; then
		ASUNTOMAIL="Ha fallado la ejecución de fichero diferido con número de solicitud: ${ID_SOL} en ejecución del SQL.\nRevisar el log ${LOG_FILE}."
	elif [ $RETMSG == 'REC' ] || [ $RETMSG == 'COD' ]; then
		ASUNTOMAIL="Ha fallado la ejecución de fichero diferido con número de solicitud: ${ID_SOL} en ejecución del script.\nRevisar el log ${LOG_FILE}."
	else
		echo 'RETMSG desconocido ${RETMSG}.'
		ASUNTOMAIL="Ha fallado la ejecución de fichero diferido con número de solicitud: ${ID_SOL} en ejecución con error desconocido.\nRevisar el log ${LOG_FILE}."
	fi
	FECHA_ACTUAL=$(date +"%Y%m%d")
	FECHA_PROC_AUX=$(date --date="7 day ${FECHA_ACTUAL}" +"%Y%m%d")
	echo "fecha caducidad ${FECHA_PROC_AUX}"
	if [ $ESTADO_PETICION == 'I' ]; then
		rm -f $TMP_DIR/marcarIncidencias.log
		TMP=$TMP_DIR/marcarIncidencias.log
		echo "Vamos a realizar el update de la petición en BBDD. Estado petición:"$ESTADO_PETICION
		sqlplus -s $CONEXION_ORACLE > $TMP << EOF
			set serveroutput on;
			begin
			update solicitudes_hist 
				set estado = '${ESTADO_PETICION}' , f_caducidad = to_timestamp('${FECHA_PROC_AUX}','yyyymmdd') 
				where id_solicitud_hist = ${ID_SOL} and f_caducidad is null;
				commit;
			exception when others then
				dbms_output.put_line('ERRCOD: ' || sqlcode);
				dbms_output.put_line('ERRMSG: ${FECHA_PROC_AUX}' || sqlerrm);
			end;
			/
			exit
EOF
		
		# esto no debería pasar nunca !!!
		if [[ $(grep -c "solicitud incompleta" $TMP) -ne 0 ]]; then
			traza "marcarIncidente - ADVERTENCIA: La solicitud con id '"$ID_SOL"' no está completa y ha quedado en estado Pendiente"
			echo -e "marcarIncidente - ADVERTENCIA: La solicitud con id '"$ID_SOL"' no está completa y ha quedado en estado Pendiente" >> $TMP_LOG_FILE_DOS
			cat $TMP >> $TMP_LOG_FILE_DOS
		fi
		
		if [[ $(grep -c ORA- $TMP) -ne 0 || $(grep -c ERR $TMP) -ne 0 ]]; then
			traza "marcarIncidente - ERROR: Se ha producido algún error durante la ejecución de sqlplus.${ID_SOL}"
			echo -e "marcarIncidente - ERROR: Se ha producido algún error durante la ejecución de sqlplus.${ID_SOL}" >> $TMP_LOG_FILE_DOS
			cat $TMP >> $TMP_LOG_FILE_DOS
		else
			echo -e "marcarIncidente - Sin errores." >> $TMP_LOG_FILE_DOS
		fi
		doMail $ASUNTOMAIL
	fi
		
}

function doMail(){
	# Reestablim el lang original perquè amb el que posem per al procés d'importació s'envia el correu amb 'xinos' i el cos arriba annexat
	export LANG=${ORI_LANG}
	ASUNTO=$*
	echo -e $ASUNTO >> $FICHERO_MAIL_INC
	if [[ -f $TMP_LOG_FILE_DOS ]]; then
		cat $TMP_LOG_FILE_DOS >> $FICHERO_MAIL_INC
		borra $TMP_LOG_FILE_DOS
	fi;
	# Tornem a posar el lang per a seguir amb la importació
	export LANG=${IMPORT_LANG}
}

function sendMail(){
	# Reestablim el lang original perquè amb el que posem per al procés d'importació s'envia el correu amb 'xinos' i el cos arriba annexat
	export LANG=${ORI_LANG}
	ENTORNO=$(grep ENTORNO_MAIL ${CONFIG_FILE} | awk -F= '{print($2)}')
	CABECERA_MAIL=$(grep CABECERA_MAIL ${CONFIG_FILE} | awk -F= '{print($2)}')
	TITULO_MAIL="${CABECERA_MAIL} - AVISO: Fallo de solicitud histórica - importacion-hist - entorno ${ENTORNO}"
	
	echo -e "\n${SEPARADOR}\n"  >> $FICHERO_MAIL_INC
	echo -e "CORREO ENVIADO AUTOMÁTICAMENTE - NO RESPONDER A ESTA DIRECCIÓN DE CORREO\n" >> $FICHERO_MAIL_INC
	
	DESTINATARIOS=$(grep DESTINATARIOS_MAIL ${CONFIG_FILE} | awk -F= '{print($2)}')
	
	
	cat $FICHERO_MAIL_INC | mailx -v -s "${TITULO_MAIL}" $DESTINATARIOS
	traza "lanzando: cat ${FICHERO_MAIL_INC} | mailx -v -s \"${TITULO_MAIL}\" ${DESTINATARIOS}"
	
	borra $FICHERO_MAIL_INC
	borra $TMP_LOG_FILE_DOS
	# Tornem a posar el lang per a seguir amb la importació
	export LANG=${IMPORT_LANG} 
}

function sendMailError(){
	# Reestablim el lang original perquè amb el que posem per al procés d'importació s'envia el correu amb 'xinos' i el cos arriba annexat
	export LANG=${ORI_LANG}
	BODY=$*
	ENTORNO=$(grep ENTORNO_MAIL ${CONFIG_FILE} | awk -F= '{print($2)}')
	CABECERA_MAIL=$(grep CABECERA_MAIL ${CONFIG_FILE} | awk -F= '{print($2)}')
	TITULO_MAIL="${CABECERA_MAIL} - AVISO: Fallo de solicitud histórica - importacion-hist - entorno ${ENTORNO}"
	DESTINATARIOS=$(grep DESTINATARIOS_MAIL ${CONFIG_FILE} | awk -F= '{print($2)}')
	
	 
	echo -e "\n${SEPARADOR}\n $BODY\n\nCORREO ENVIADO AUTOMÁTICAMENTE - NO RESPONDER A ESTA DIRECCIÓN DE CORREO\n"| mailx -v -s "${TITULO_MAIL}" $DESTINATARIOS
	
	# Tornem a posar el lang per a seguir amb la importació
	export LANG=${IMPORT_LANG}
}

function urlencode(){

	ENC_STRING=$@
	OUT2="$(echo "${ENC_STRING}" | sed -f ${PATH_BATCH}/fich/urlencode.sed)"
	echo -n "$OUT2"
}

function sqlescape(){

	ENC_STRING=$@
	OUT2="$(echo "${ENC_STRING}" | sed -f ${PATH_BATCH}/fich/sqlescape.sed)"
	echo -n "$OUT2"
}

function sqlunescape(){

	ENC_STRING=$@
	OUT2="$(echo "${ENC_STRING}" | sed -f ${PATH_BATCH}/fich/sqlunescape.sed)"
	echo -n "$OUT2"
}

function grepescape(){

	ENC_STRING=$@
	OUT2="$(echo "${ENC_STRING}" | sed -f ${PATH_BATCH}/fich/grepescape.sed)"
	echo -n "$OUT2"
}

function grepunescape(){

	ENC_STRING=$@
	OUT2="$(echo "${ENC_STRING}" | sed -f ${PATH_BATCH}/fich/grepunescape.sed)"
	echo -n "$OUT2"
}

#----------------------------------------------------------------------------------------------------------------------
# Obtención de las variables de configuración
#----------------------------------------------------------------------------------------------------------------------
#Obtenim el HOME de l'usuari
USER_HOME=$HOME
# ruta y nombre del fichero de configuración
CONFIG_FILE=${USER_HOME}/config/batch.conf
CODE_FILE=${USER_HOME}/batch/imp/codigos.conf

# script de tratamiento de peticiones fichero
PATH_BATCH=${USER_HOME}/batch
SH_PROCESAR_FICHERO=${PATH_BATCH}/fich/procesarFichero.sh
SH_PROCESAR_MULTIPLE=${PATH_BATCH}/fich/procesarMultiple.sh
SH_PROCESAR_NORMAL=${PATH_BATCH}/fich/procesarFicheroNormal.sh
SH_PROCESAR_INMEDIATO=${PATH_BATCH}/fich/procesarFicheroInmediato.sh
SH_DATOS_NEGOCIO=${PATH_BATCH}/imp/cargarDatosNegocio.sh
SH_TRATAR_INDICES=${PATH_BATCH}/ctl/crearIndicesDatosNoPart.sh
SH_BORRAR_FICHERO_BD=${PATH_BATCH}/ctl/rmRemoteOracleFiles.sh

# ruta del directorio de exportación
EXP_DIR=$(grep EXP_DIR ${CONFIG_FILE} | awk -F= '{print($2)}') # SOLO MIENTRAS NO HAYA CINTAS REALES !!!

# ruta del directorio de importación
IMP_DIR=$(grep IMP_DIR ${CONFIG_FILE} | awk -F= '{print($2)}')

# ruta del directorio de logs
LOG_DIR=$(grep LOG_DIR ${CONFIG_FILE} | awk -F= '{print($2)}')

# ruta del File System donde acaban los ficheros csv (con datos históricos) solicitados por los usuarios
FS_DIR=$(grep FS_DIR ${CONFIG_FILE} | awk -F= '{print($2)}')

# ruta del directorio temporal
TMP_DIR_PREF=$(grep TMP_DIR ${CONFIG_FILE} | awk -F= '{print($2)}')
#TMP_DIR=$(grep TMP_DIR ${CONFIG_FILE} | awk -F= '{print($2)}')

#APLICACIÓN
APLICACION=$(grep APLICACION ${CONFIG_FILE} | awk -F= '{print($2)}')

# cadena de conexión a la base de datos (en formato usuario/password@instancia)
CONEXION_ORACLE=$(grep CONEXION_ORACLE ${CONFIG_FILE} | awk -F= '{print($2)}')

# cadena de conexión a la base de datos para los indices (en formato usuario/password@instancia)
CONEXION_INDICES_ORACLE=$(grep CONEXION_INDICES_ORACLE ${CONFIG_FILE} | awk -F= '{print($2)}')

# número de particiones de la aplicación
NUM_PARTICIONES=$(grep NUM_PARTICIONES ${CONFIG_FILE} | awk -F= '{print($2)}')

# número máximo de lecturas a cinta en paralelo
NUM_PARALEL_READ=$(grep NUM_PARALEL_READ ${CONFIG_FILE} | awk -F= '{print($2)}')

#parametros para saber si estamos en Test o en PRE/PRO (para en ssh de ficheros)
SERVER_ACTUAL=$(get_hostservicename)
SERVER_REMOTO=$(grep SERVIDOR_BD_1 ${CONFIG_FILE} | awk -F= '{print($2)}')

# Dias de vigencia de las peticiones de oficina
DIAS_VIGENCIA_OFICINA=$(grep DIAS_VIGENCIA_OFICINA ${CONFIG_FILE} | awk -F= '{print($2)}')

# Dias de vigencia de las peticiones de auditoria
DIAS_VIGENCIA_AUDITORIA=$(grep DIAS_VIGENCIA_AUDITORIA ${CONFIG_FILE} | awk -F= '{print($2)}')

#Dias que una solicitud permanece como caduca hasta que se elimina para oficinas
DIAS_CADUCAS_OFICINA=$(grep DIAS_CADUCAS_OFICINA ${CONFIG_FILE} | awk -F= '{print($2)}')

#Dias que una solicitud permanece como caduca hasta que se elimina para auditoría
DIAS_CADUCAS_AUDITORIA=$(grep DIAS_CADUCAS_AUDITORIA ${CONFIG_FILE} | awk -F= '{print($2)}')
# Indica si el tablespace  es por dia o comun
TIPORESTORE=$(grep TIPO_RESTORE ${CONFIG_FILE} | awk -F= '{print($2)}')

MAX_ROWNUM=$(grep MAX_ROWNUM ${CONFIG_FILE} | awk -F= '{print($2)}')
#Numero de filas a esborrar a les taules de BD a cada iteració (reducció consum UNDO)

# log
if [[ ! -d $LOG_DIR ]]; then
	traza "ERROR: El directorio de logs '$LOG_DIR' es incorrecto."
	sendMailError "ERROR: El directorio de logs '$LOG_DIR' es incorrecto."
	exit 5
fi
LOG_FILE=$LOG_DIR/importacion-hist.log

#Comprovació i rotació de logs si fa falta

comprovaLogFile $LOG_FILE

exec 1>>$LOG_FILE 2>&1
traza
traza
traza
traza "Ejecución de la importación de históricos a "$(date +"%d/%m/%Y %H:%M:%S")
PID=$$
ID_PROC="ImpHist".$PID

PID=$$	
TIMESTAMP=$(date +"%Y%m%d%H%M")
NOM_DIR_TMP="imp."$PID"."$TIMESTAMP".dir"
TMP_DIR=$TMP_DIR_PREF"/"$NOM_DIR_TMP
$(mkdir $TMP_DIR)
traza "Creado directorio para fichero temporales de la ejecucion actual: '"$TMP_DIR"'"

TMP_LOG_FILE=$LOG_DIR/sqlplus$ID_PROC.log
TMP_LOG_FILE_DOS=$LOG_DIR/incidenciasMarca$ID_PROC.log
TMP_LOG_FILE_CINTA=$LOG_DIR/cinta$ID_PROC.log

doScript=0

if [[ $TIPORESTORE == 'SCRIPT' ]]; then
	
	traza "Tipo de restore a realizar, se invoca el script de root proporcionado para restore de cinta"
	doScript=1
fi



AHORA=$(date +"%Y%m%d%H%M%S")
CLAVEFICHERO=$AHORA

traza "Ficheros obtenidos de BD con referencia: "$CLAVEFICHERO

#Fitxer mail incidencies
FICHERO_MAIL_INC=$TMP_DIR/mailProcesoIncidencias.$CLAVEFICHERO.inc
echo > $FICHERO_MAIL_INC
echo -e "DETALLES INCIDENCIAS\n" >> $FICHERO_MAIL_INC
echo -e " -----------------------\n\n" >> $FICHERO_MAIL_INC

# comprueba el directorio de exportación
if [[ ! -d $EXP_DIR ]]; then
	traza "ERROR: El directorio de datos EXP_DIR es incorrecto:'{$EXP_DIR}'"
	sendMailError "ERROR: El directorio de datos EXP_DIR es incorrecto:'{$EXP_DIR}'"
	exit 6
fi

# comprueba el directorio de importación
if [[ ! -d $IMP_DIR ]]; then
	traza "ERROR: El directorio de importación IMP_DIR es incorrecto:'{$IMP_DIR}'"
	sendMailError "ERROR: El directorio de importación IMP_DIR es incorrecto:'{$IMP_DIR}'"
	exit 7
fi

# comprueba el File System de csv
if [[ ! -d $FS_DIR ]]; then
	traza "ERROR: El directorio del File System FS_DIR es incorrecto:'{$FS_DIR}'"
	sendMailError "ERROR: El directorio del File System FS_DIR es incorrecto:'{$FS_DIR}'"
	exit 11
fi

# comprueba el directorio temporal
if [[ ! -d $TMP_DIR ]]; then
	traza "ERROR: El directorio temporal TMP_DIR es incorrecto:'{$TMP_DIR}'"
	sendMailError "ERROR: El directorio temporal TMP_DIR es incorrecto:'{$TMP_DIR}'"
	exit 12
fi

# comprueba el número de particiones
if [[ $NUM_PARTICIONES -lt 1 || $NUM_PARTICIONES -gt 100 ]]; then
	traza "ERROR: Falta el número de particiones o éste es incorrecto."
	sendMailError "ERROR: Falta el número de particiones o éste es incorrecto."
	exit 1
fi

# comrpueba el número máximo de lecturas a cinta en paralelo
if [[ $NUM_PARALEL_READ -lt 1 || $NUM_PARALEL_READ -gt 100 ]]; then
	traza "ERROR: Falta el número máximo de lecturas en paralelo."
	sendMailError "ERROR: Falta el número máximo de lecturas en paralelo."
	exit 10
fi


# posiciones de los campos dentro del fichero de solicitudes pendientes
POS_CAMPO_ID=1 					# campo 'ID_SOLICITUD_HIST'
POS_CAMPO_TIPO_TERMINAL=3		# campo 'CRIT_TIPO_TERMINAL'
POS_CAMPO_TERMINAL=4 			# campo 'CRIT_TERMINAL'
POS_CAMPO_FECHA_INI=5			# campo 'CRIT_FECHA_INI'
POS_CAMPO_FECHA_FIN=6			# campo 'CRIT_FECHA_FIN'
POS_CAMPO_SOPORTE=7				# campo 'SOPORTE'
POS_CAMPO_TIPO=8	   			# campo 'TIPO'
POS_CAMPO_ESTADO=9				# campo 'ESTADO'

# ruta y nombre del fichero de control de sql*loader para cargar datos a la tabla NIVEL_HIST
CTL_FILE=${USER_HOME}/batch/imp/nivel_hist.ctl
# ruta y nombre del fichero de control de sql*loader para cargar datos a la tabla DATOS_NO_PART_IP_HIST
CTL_FILE_AUX=
if [[ $APLICACION == 'CA' ]];then
	CTL_FILE_AUX=${USER_HOME}/batch/imp/activ_hist.ctl
else
	CTL_FILE_AUX=${USER_HOME}/batch/imp/ip_hist.ctl
fi

#ruta y nombre del fichero de versiones de los registros exportacion
VERSIO_HIST=${USER_HOME}/batch/imp/versio_hist.conf

# Configuración de la cadena
#paso 0: Eliminación de los datos antiguos
#paso 1: Peticiones multiples
#paso 2: deshabilita los índices de la tabla de históricos
#paso 3: obtiene lista de solicitudes pendientes
#paso 4: obtiene lista de claves de solicitud cargadas
#paso 5: calcula partición y semana, y obtiene el nombre de los ficheros a recuperar
#paso 6: recupera los ficheros de cinta magnética
#paso 7: Prepara fichero de carga
#paso 8: Carga de los ficheros
#paso 9: Inserción de la claves tratadas 
#paso 10: Modificacion de estado para peticiones de oficina
#paso 11: Modificacion de estado para peticiones de auditoria
#paso 12: Modificacion estado peticiones multiples oficina
#paso 13: Modificacion estado peticiones multiples auditoria
#paso 14: Habilitar los índices de la tabla de históricos
#paso 15: Genera los ficheros csv
#paso 16: Eliminacion de ficheros
#paso 17: Pasa estadísticas de NIVEL_HIST y DATOS_NEGOCIO_HIST
#paso 18: Genera y envía correo resumen del proceso
#doPaso0=1
#doPaso1=0
##bucle...
#doPaso2=1
#doPaso3=1
#doPaso4=1
#doPaso5=1
#doPaso6=1
#doPaso7=1
#doPaso8=1
#doPaso9=1
#doPaso10=1
#doPaso11=1
##...fin bucle
#doPaso12=1
#doPaso13=1
#doPaso14=1
#doPaso15=1
#doPaso16=1
#doPaso17=0

#doPaso0=1
#doPaso1=1
##bucle...
#doPaso2=0
#doPaso3=1
#doPaso4=1
#doPaso5=1
#doPaso6=1
#doPaso7=1
#doPaso8=1
#doPaso9=1
#doPaso10=1
#doPaso11=1
##...fin bucle
#doPaso12=1
#doPaso13=1
#doPaso14=1
#doPaso15=1
#doPaso16=1
#doPaso17=0
#doPaso18=1

doPaso0=1
doPaso1=1
#bucle...
doPaso2=0
doPaso3=1
doPaso4=1
doPaso5=1
doPaso6=1
doPaso7=1
doPaso8=1
doPaso9=1
doPaso10=1
doPaso11=1
#...fin bucle
doPaso12=1
doPaso13=1
doPaso14=0
doPaso15=1
doPaso16=1
doPaso17=0
doPaso18=1
CONTINUAR=1
TOTAL_TRATADAS=0
doQuitIndex=1
doRebuildIndex=0

#----------------------------------------------------------------------------------------------------------------------
# Obtención de la fecha de proceso
#----------------------------------------------------------------------------------------------------------------------

# la fecha de proceso es el primer y único parámetro del script, en formato dd/mm/yyyy
FECHA_PROCESO_ESP=$1

# la fecha de proceso es el primer y único parámetro del script, en formato dd/mm/yyyy
DEBUG=$2

# fecha en formato anglosajón (mm/dd/yyyy), útil para trabajar con el comando 'date'
FECHA_PROCESO_ING=$(echo $FECHA_PROCESO_ESP | awk -F/ '{print $2 "/" $1 "/" $3}')

# comprueba la fecha de proceso
FF=$(date --date="${FECHA_PROCESO_ING}" +"%d/%m/%Y")

# comprueba que la fecha de proceso coincida con el día de la semana de grabación
DIA=$(date --date="${FECHA_PROCESO_ING}" +"%u")

RESULTADO=$?
if [[ $RESULTADO -ne 0 ]]; then
	traza "ERROR: La fecha de proceso es incorrecta, el comando 'date' devolvió la respuesta ${RESULTADO}."
	sendMailError "ERROR: La fecha de proceso es incorrecta, el comando 'date' devolvió la respuesta ${RESULTADO}."
	exit 21
fi

if [[ $DEBUG == '-d' ]]; then
	traza "Activado el modo debug, no se eliminan ficheros".
fi

traza "fecha de proceso: "$FF
# Fecha de caducidad para  de las peticiones de oficina
FAUX=$(echo $FECHA_PROCESO_ESP | awk -F/ '{print $3 $2 $1}')
traza "FAUX:"$FAUX

# Fecha de tratamiento para  de las peticiones de oficina

FECHA_ACTIVACION_OFICINA=$(date --date="${FAUX}" +"%Y%m%d")
traza "fecha de activación oficina: "$FECHA_ACTIVACION_OFICINA
FECHA_CADUCIDAD_OFICINA=$(date --date="${FAUX} ${DIAS_VIGENCIA_OFICINA} day" +"%Y%m%d")
traza "fecha de caducidad oficina: "$FECHA_CADUCIDAD_OFICINA
FECHA_ELIMINACION_OFICINA=$(date --date="${FAUX} ${DIAS_VIGENCIA_OFICINA} day" +"%Y%m%d")
traza "fecha de eliminación oficina: "$FECHA_ELIMINACION_OFICINA
FECHA_DEL_CADUCAS_OFICINA=$(date --date="${FECHA_CADUCIDAD_OFICINA} ${DIAS_CADUCAS_OFICINA} day" +"%Y%m%d")
echo "fecha eliminacion sol. caducas oficina: "$FECHA_DEL_CADUCAS_OFICINA

# Fecha de tratamiento para  de las peticiones de auditoria
FECHA_ACTIVACION_AUDITORIA=$(date --date="${FAUX}" +"%Y%m%d")
traza "fecha de activación auditoria: "$FECHA_ACTIVACION_AUDITORIA
FECHA_CADUCIDAD_AUDITORIA=$(date --date="${FAUX} ${DIAS_VIGENCIA_AUDITORIA} day"  +"%Y%m%d")
traza "fecha de caducidad auditoria: "$FECHA_CADUCIDAD_AUDITORIA
FECHA_ELIMINACION_AUDITORIA=$(date --date="${FAUX} ${DIAS_VIGENCIA_AUDITORIA} day" +"%Y%m%d")
traza "fecha de eliminación auditoria: "$FECHA_ELIMINACION_AUDITORIA
FECHA_DEL_CADUCAS_AUDITORIA=$(date --date="${FECHA_CADUCIDAD_AUDITORIA} ${DIAS_CADUCAS_AUDITORIA} day" +"%Y%m%d")
echo "fecha eliminacion sol. caducas auditoria: "$FECHA_DEL_CADUCAS_OFICINA


# Ficheros donde se acumulan las claves que se estan tratando y las que ya se han tratado
HORA_ACTUAL=$(date +"%H%M%S")
HORA=`expr $(echo $HORA_ACTUAL) + 0` 
HORA_ANTERIOR=$HORA
traza "HORA ACTUAL (COMIENZO):"$HORA
traza "HORA ANTERIOR (COMIENZO):"$HORA_ANTERIOR

#LIMITE=80
LIMITE=70
LIMITE_HORA=60000
LIMITE_HORA_COMIENZO=80000
LIMITE_TARDE=120000

#LIMITE_HORA=220000


traza "Dia de la semana: "$DIA
if [[ $DIA -eq 6 ]]; then
	LIMITE_HORA=90000
	LIMITE_HORA_COMIENZO=120000
	traza "Cadena lanzada en horario de fin de semana (sabado). HORA LIMITE: "$LIMITE_HORA
fi

if [[ $DIA -eq 7 ]]; then
	LIMITE_HORA=90000
	LIMITE_HORA_COMIENZO=120000
	traza "Cadena lanzada en horario de fin de semana (domingo). HORA LIMITE: "$LIMITE_HORA
fi
if [[ $HORA -gt $LIMITE_TARDE ]]; then
	LIMITE_HORA=220000
	#LIMITE_HORA=172500
	traza "Cadena lanzada en horario de tarde. HORA LIMITE: "$LIMITE_HORA
else 
	traza "Cadena lanzada en horario matinal. HORA LIMITE: "$LIMITE_HORA
fi


traza 'Inicializando parametros lengua del sistema: LANG=es_ES.UTF-8'
IMPORT_LANG=es_ES.UTF-8
ORI_LANG=en_US.iso885915
export LANG=${IMPORT_LANG}		
export NLS_LANG=SPANISH_SPAIN.AL32UTF8

FICHERO_CLAVES_VOLCADAS=$TMP_DIR/claves_volcadas.$CLAVEFICHERO.tmp
FICHERO_CLAVES_ACTUALES=$TMP_DIR/claves_actuales.$CLAVEFICHERO.tmp
FICHERO_CLAVES_ACTIVADAS=$TMP_DIR/claves_activadas.$CLAVEFICHERO.tmp
FICHERO_FICHEROS_CARGADOS=$TMP_DIR/ficheros_cargados.$CLAVEFICHERO.tmp
FICHERO_BORRADO_TXT=$TMP_DIR/fichero_a_borrar.tmp

FICHEROS_A_RECUPERAR=$TMP_DIR/ficheros_a_recuperar.$CLAVEFICHERO.tmp
echo > $FICHERO_CLAVES_VOLCADAS	
echo > $FICHERO_CLAVES_ACTUALES

RESUMEN_PROC=$TMP_DIR/resumenProceso.$CLAVEFICHERO.res
INCIDENCIAS_PROC=$TMP_DIR/incidenciasProceso.$CLAVEFICHERO.inc
CHECK_PROC=$TMP_DIR/incidenciasProceso.$CLAVEFICHERO.chk
echo > $RESUMEN_PROC
echo > $INCIDENCIAS_PROC
echo -e " RESUMEN:\n" >> $RESUMEN_PROC
echo -e " --------\n\n" >> $RESUMEN_PROC
echo -e " INCIDENCIAS DETECTADAS:\n" >> $INCIDENCIAS_PROC
echo -e " -----------------------\n\n" >> $INCIDENCIAS_PROC

#----------------------------------------------------------------------------------------------------------------------
# Eliminación de los datos antiguos
#----------------------------------------------------------------------------------------------------------------------

#----------------------------------------------------------------------------------------------------------------------
# Eliminación de los datos antiguos
#----------------------------------------------------------------------------------------------------------------------
traza 'paso Previo: Control Horario'

	COMENZAR=1
	checkComenzar
	traza "Variable bucle COMENZAR: "$COMENZAR
	if [[ $COMENZAR == 0 ]]; then
			traza "Hora de comienzo superior a la esperada. No se realizan actuaciones"
			inc "El proceso termina sin realizar acciones"
			doPaso0=0
			doPaso1=0
			#bucle...
			doPaso2=0
			doPaso3=0
			doPaso4=0
			doPaso5=0
			doPaso6=0
			doPaso7=0
			doPaso8=0
			doPaso9=0
			doPaso10=0
			doPaso11=0
			#...fin bucle
			doPaso12=0
			doPaso13=0
			doPaso14=0
			doPaso15=0
			doPaso16=0
			doPaso17=0
			doPaso18=1
	fi
	doPaso0=1

if [[ $DIA -eq 6 ]]; then
	traza "Activamos el tratamiento de indices en horario de fin de semana (sabado). HORA LIMITE: "$LIMITE_HORA
	doPaso2=1
	doPaso14=1	
	doPaso17=1	
fi
	

traza 'paso 0: elimina datos caducos'
traza "doPaso0?: ${doPaso0}"


if [ $doPaso0 -eq 1 ]; then

# nombre del fichero con la lista de ficheros que deben eliminarse
# (sin ruta, Oracle siempre lo creará en el directorio temporal)
FICHERO_FICHEROS_A_ELIMINAR=ficheros_a_eliminar.$CLAVEFICHERO.tmp
rm -f $FICHERO_FICHEROS_A_ELIMINAR

# llamada a la función pl/sql 'solicitud_hist.elimina_solicitudes_caducas', que establece a caducadas las solicitudes
# que así lo requieren, elimina las claves que ya no satisfacen ninguna solicitud y elimina los datos correspondientes
# en la tabla de históricos; además, cera un fichero que contiene aquellos ficheros csv que deben eliminarse (los datos
# para soporte Pantalla se eliminan dentro de la función pero, los ficheros csv para soporte Fichero deben eliminarse
# desde fuera)
sqlplus -s $CONEXION_ORACLE > $TMP_LOG_FILE << EOF
	set serveroutput on;
	declare
    l_num_caducadas_pantalla number;
    l_num_caducadas_fichero number;
    l_num_claves_eliminadas number;
    l_num_claves_hist_eliminadas number;
    l_num_regs_hist_eliminados number;
    l_num_ficheros_a_eliminar number;
	begin
		solicitud_hist.elimina_solicitudes_caducas(l_num_caducadas_pantalla, l_num_caducadas_fichero, l_num_claves_eliminadas,
			l_num_claves_hist_eliminadas, l_num_regs_hist_eliminados, l_num_ficheros_a_eliminar, '${FICHERO_FICHEROS_A_ELIMINAR}');
		commit;
		dbms_output.put_line('  soporte pantalla:');
		dbms_output.put_line('    solicitudes caducadas: ' || l_num_caducadas_pantalla);
		dbms_output.put_line('    claves eliminadas: ' || l_num_claves_eliminadas);
		dbms_output.put_line('    claves hist.eliminadas: ' || l_num_claves_hist_eliminadas);
		dbms_output.put_line('    registros hist.eliminados: ' || l_num_regs_hist_eliminados);
		dbms_output.put_line('  soporte fichero:');
		dbms_output.put_line('    solicitudes caducadas: ' || l_num_caducadas_fichero);
		dbms_output.put_line('    ficheros a eliminar: ' || l_num_ficheros_a_eliminar);
	exception when others then
		rollback;
		dbms_output.put_line('ERRCOD: ' || sqlcode);
		dbms_output.put_line('ERRMSG: ' || sqlerrm);
	end;
	/
	exit
EOF

trazafile $TMP_LOG_FILE
# igual que un 'cat' pero sin lineas en blanco
if [[ $(grep -c ORA- $TMP_LOG_FILE) -ne 0 || $(grep -c ERR $TMP_LOG_FILE) -ne 0 ]]; then
	traza "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
	cat $TMP_LOG_FILE >> $TMP_LOG_FILE_DOS
	doMail "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
	sendMail
	exit 151
fi
rm $TMP_LOG_FILE

# añade la ruta al fichero con la lista de ficheros que deben eliminarse
FICHERO_FICHEROS_A_ELIMINARaux=$TMP_DIR_PREF/$FICHERO_FICHEROS_A_ELIMINAR
FICHERO_FICHEROS_A_ELIMINAR=$TMP_DIR/$FICHERO_FICHEROS_A_ELIMINAR

if [[ $SERVER_ACTUAL != $SERVER_REMOTO ]]; then

  rm -f $FICHERO_FICHEROS_A_ELIMINAR
  obtener_fichero $FICHERO_FICHEROS_A_ELIMINARaux $TMP_DIR
	
fi

# comprueba el fichero con la lista de ficheros que deben eliminarse
if [[ ! -f $FICHERO_FICHEROS_A_ELIMINAR ]]; then
	traza "ERROR: No se ha creado el fichero '"$FICHERO_FICHEROS_A_ELIMINAR"' con los ficheros a eliminar."
	sendMailError "ERROR: No se ha creado el fichero '"$FICHERO_FICHEROS_A_ELIMINAR"' con los ficheros a eliminar."
	exit 152
fi

# resigue el fichero con la lista de ficheros que deben eliminarse...
cat $FICHERO_FICHEROS_A_ELIMINAR | while read FICHERO_A_ELIMINAR
do
	if [[ $FICHERO_A_ELIMINAR != "" ]]; then
		traza "fichero a eliminar: '"$FICHERO_A_ELIMINAR"' en la màquina de as"
		FICHERO_A_ELIMINAR=$(basename ${FICHERO_A_ELIMINAR})
	
		FICHERO_A_ELIMINAR=$FS_DIR/$FICHERO_A_ELIMINAR
		traza "path fichero a eliminar: '"$FICHERO_A_ELIMINAR"'"

		# ...elimina cada uno de los ficheros caducos
		#SBR: borrar en todas maquina AS
		#borraRemoto $FICHERO_A_ELIMINAR
		borraMaquinas_as $FICHERO_A_ELIMINAR
	fi
done

cat $FICHERO_FICHEROS_A_ELIMINAR | while read FICHERO_A_ELIMINAR
do
	if [[ $FICHERO_A_ELIMINAR != "" ]]; then
		traza "fichero a eliminar: '"$FICHERO_A_ELIMINAR"' en la màquina de consulta."
		FICHERO_A_ELIMINAR=$(basename ${FICHERO_A_ELIMINAR})
	
		FICHERO_A_ELIMINAR=$FS_DIR/$FICHERO_A_ELIMINAR
		traza "path fichero a eliminar: '"$FICHERO_A_ELIMINAR"'"

		# ...elimina cada uno de los ficheros caducos
		#SBR: borrar en todas maquina QS
		#borraRemoto $FICHERO_A_ELIMINAR
		borraMaquinas_qs $FICHERO_A_ELIMINAR
	fi
done

# elimina el fichero con la lista de ficheros que deben eliminarse

 borra $FICHERO_FICHEROS_A_ELIMINAR

fi

#----------------------------------------------------------------------------------------------------------------------
# Obtiene lista de solicitudes multiples pendientes
#----------------------------------------------------------------------------------------------------------------------
traza 'paso 1: obtiene lista de solicitudes multiples pendientes'
traza "doPaso1?: ${doPaso1}"

if [ $doPaso1 -eq 1 ]; then

AHORA=$(date +"%Y%m%d%H%M%S")
CLAVEFICHERO=$AHORA

traza "Ficheros obtenidos de BD con referencia: "$CLAVEFICHERO

# nombre del fichero con la lista de solicitudes multiples pendientes (sin ruta, Oracle siempre lo creará en el directorio temporal)
FICHERO_SOLICITUDES_MULTIPLES=solicitudes_multiples.$CLAVEFICHERO.tmp

# nombre del fichero con la lista de solicitudes multiples incidentes(sin ruta, Oracle siempre lo creará en el directorio temporal)
FICHERO_SOLICITUDES_MULTIPLES_INC=solicitudes_multiples.incidentes.$CLAVEFICHERO.tmp
# llamada a la función pl/sql 'solicitud_hist.get_fich_multiples_pendientes', que crea un fichero con las solicitudes pendientes de tipo multiples
# en formato de texto separado por '|'
sqlplus -s $CONEXION_ORACLE > $TMP_LOG_FILE << EOF
	set serveroutput on;
	declare
		l_num_solicitudes_pendientes number;
	begin
		l_num_solicitudes_pendientes := solicitud_hist.get_fich_multiples_pendientes('${FICHERO_SOLICITUDES_MULTIPLES}');
	end;
	/
	exit
EOF
#sed '/^$/d' $TMP_LOG_FILE # igual que un 'cat' pero sin líneas en blanco
trazafile $TMP_LOG_FILE
if [[ $(grep -c ORA- $TMP_LOG_FILE) -ne 0 || $(grep -c ERR $TMP_LOG_FILE) -ne 0 ]]; then
	traza "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
	cat $TMP_LOG_FILE >> $TMP_LOG_FILE_DOS
	doMail "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
	sendMail
	exit 154
fi
rm $TMP_LOG_FILE

# llamada a la función pl/sql 'solicitud_hist.get_fich_multiples_incidentes', que crea un fichero con las solicitudes incidentes de tipo multiples
# en formato de texto separado por '|'
sqlplus -s $CONEXION_ORACLE > $TMP_LOG_FILE << EOF
	set serveroutput on;
	declare
		l_num_solicitudes_incidentes number;
	begin
		l_num_solicitudes_incidentes := solicitud_hist.get_fich_multiples_incidentes('${FICHERO_SOLICITUDES_MULTIPLES_INC}');
	end;
	/
	exit
EOF
#sed '/^$/d' $TMP_LOG_FILE # igual que un 'cat' pero sin líneas en blanco
trazafile $TMP_LOG_FILE

if [[ $(grep -c ORA- $TMP_LOG_FILE) -ne 0 || $(grep -c ERR $TMP_LOG_FILE) -ne 0 ]]; then
	traza "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
	echo -e "ERROR: Se ha producido algún error durante la ejecución de sqlplus." > $TMP_LOG_FILE_DOS
	cat $TMP_LOG_FILE >> $TMP_LOG_FILE_DOS
	sendMail
	doExit 154
fi
rm $TMP_LOG_FILE

# añade la ruta al fichero de solicitudes pendientes
FICHERO_SOLICITUDES_MULTIPLESaux=$TMP_DIR_PREF/$FICHERO_SOLICITUDES_MULTIPLES
FICHERO_SOLICITUDES_MULTIPLES=$TMP_DIR/$FICHERO_SOLICITUDES_MULTIPLES

FICHERO_SOLICITUDES_MULTIPLES_INCaux=$TMP_DIR_PREF/$FICHERO_SOLICITUDES_MULTIPLES_INC
FICHERO_SOLICITUDES_MULTIPLES_INC=$TMP_DIR/$FICHERO_SOLICITUDES_MULTIPLES_INC
if [[ $SERVER_ACTUAL != $SERVER_REMOTO ]]; then

	rm -f $FICHERO_SOLICITUDES_MULTIPLESaux
  obtener_fichero $FICHERO_SOLICITUDES_MULTIPLESaux $TMP_DIR
	
fi


# comprueba el fichero de solicitudes pendientes
if [[ ! -f $FICHERO_SOLICITUDES_MULTIPLES ]]; then
	traza "ERROR: No se ha creado el fichero '"$FICHERO_SOLICITUDES_MULTIPLES"' con las solicitudes multiples pendientes."
	echo -e "ERROR: No se ha creado el fichero '"$FICHERO_SOLICITUDES_MULTIPLES"' con las solicitudes multiples pendientes." > $TMP_LOG_FILE_DOS
	doMail
	sendMail
	exit 155
fi

if [[ $SERVER_ACTUAL != $SERVER_REMOTO ]]; then

	rm -f $FICHERO_SOLICITUDES_MULTIPLES_INCaux
  obtener_fichero $FICHERO_SOLICITUDES_MULTIPLES_INCaux $TMP_DIR
	
fi


# comprueba el fichero de solicitudes incidentes

if [[ ! -f $FICHERO_SOLICITUDES_MULTIPLES_INC ]]; then
	traza "ERROR: No se ha creado el fichero '"$FICHERO_SOLICITUDES_MULTIPLES_INC"' con las solicitudes multiples incidentes."
	echo -e "ERROR: No se ha creado el fichero '"$FICHERO_SOLICITUDES_MULTIPLES_INC"' con las solicitudes multiples incidentes." > $TMP_LOG_FILE_DOS
	sendMail
	doExit 155
fi

cat $FICHERO_SOLICITUDES_MULTIPLES_INC >> $FICHERO_SOLICITUDES_MULTIPLES
# número de solicitudes pendientes e incidentes
NUM_SOLICITUDES_MULTIPLES=$(wc -l $FICHERO_SOLICITUDES_MULTIPLES | awk '{print $1}')
traza "solicitudes pendientes: "$NUM_SOLICITUDES_MULTIPLES


	# resigue el fichero de solicitudes pendientes creado por oracle...
	cat $FICHERO_SOLICITUDES_MULTIPLES | while read SOLICITUD_MULTIPLE
	do
	
			traza "antes:"$SOLICITUD_MULTIPLE"|antes"
			SOLICITUD_MULTIPLE=$(echo $SOLICITUD_MULTIPLE | sed 's/ /#.#/g')
			SOLICITUD_MULTIPLE=$(echo $SOLICITUD_MULTIPLE | sed 's/=/%3D/g')
			SOLICITUD_MULTIPLE=$(echo $SOLICITUD_MULTIPLE | sed 's/&/%26/g')			
			traza "despues:"$SOLICITUD_MULTIPLE"|despues"

      traza "- Se procesa la siguiente petición multiple_______________________"
			traza $SOLICITUD_MULTIPLE

			$SH_PROCESAR_MULTIPLE $SOLICITUD_MULTIPLE $DEBUG
			RESULTADO=$?
			if [[ $RESULTADO -ne 0 ]]; then
				traza "- ERROR: Petición no procesada con resultado "$RESULTADO
				inc "No se ha podido procesar petición múltiple: '${SOLICITUD_MULTIPLE}'"
				echo -e "No se ha podido procesar petición múltiple: '${SOLICITUD_MULTIPLE}'" > $TMP_LOG_FILE_DOS
				marcarIncidente ${SOLICITUD_MULTIPLE} REC
			else 
				traza "- OK: Petición procesada " 
			fi

			traza "_________________________________________________________________________________"	
	
	done

 borra $FICHERO_SOLICITUDES_MULTIPLES

	
fi
  
	
#----------------------------------------------------------------------------------------------------------------------
# Bucle principal de la aplicacion
#----------------------------------------------------------------------------------------------------------------------
traza 'paso 2..12: Bucle principal'
traza "Comienza el bucle"
resum "Comienza el bucle"

CONTINUAR=1

while [ $CONTINUAR == 1 ];
do

rm -f $FICHERO_CLAVES_ACTIVADAS
rm -f $FICHERO_FICHEROS_CARGADOS

	traza "Variable bucle CONTINUAR: "$CONTINUAR
	checkContinuar
	traza "Variable bucle CONTINUAR: "$CONTINUAR
	if [[ $CONTINUAR == 0 ]]; then
			traza "Saliendo del bucle de tratamiento de peticiones"
			break;
	fi

#----------------------------------------------------------------------------------------------------------------------
# Deshabilitación de los índices de la tabla de históricos
#----------------------------------------------------------------------------------------------------------------------
traza 'paso 2: deshabilita los índices de la tabla de históricos'
traza "doPaso2?: ${doPaso2}"
traza "doQuitIndex?: ${doQuitIndex}"

if [ $doPaso2 -eq 1 ]; then

if [ $doQuitIndex -eq 1 ]; then
	doIndices=0
	#No se desactiva los indices principales
	doQuitIndex=0
	#Se pone a cero para que no se elimine más de una vez los indices
	
	if [[ $doIndices -ne 0 ]]; then
	
	# establece lso índices a 'unusable', para que no interfieran en la carga del sql*loader
	sqlplus -s $CONEXION_INDICES_ORACLE > $TMP_LOG_FILE << EOF
		alter index pk_nivel_hist unusable;
		alter index idx_nivel_padre_hist unusable;
		
		exit
EOF
# Falla, no se si es un problema de grants ya que el index es del Idecloi1
	#sed '/^$/d' $TMP_LOG_FILE # igual que un 'cat' pero sin líneas en blanco
	trazafile $TMP_LOG_FILE
	if [[ $(grep -c ORA- $TMP_LOG_FILE) -ne 0 || $(grep -c ERR $TMP_LOG_FILE) -ne 0 ]]; then
		traza "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
		cat $TMP_LOG_FILE >> $TMP_LOG_FILE_DOS
		doMail "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
		sendMail
		exit 158
	fi
	rm $TMP_LOG_FILE
	fi

	NUMDIA_INDICE="HIST"
	INDICES=$(grep INDICESHIST ${CONFIG_FILE} | awk -F= '{print($2)}')
	echo "Indices a tratar: ${INDICES}"

	count=`echo $INDICES | awk -F, {'print NF'}`

	i=1
	while [ $i -le $count ]
	do
		str[$i]=`echo $INDICES | cut -d, -f${i}`
		NOM_INDEX=${str[$i]}
		traza "Eliminando índice ${NOM_INDEX} para el dia ${NUMDIA_INDICE}"	
		$SH_TRATAR_INDICES $NOM_INDEX $NUMDIA_INDICE U N N N
		RESULTADO=$?
		if [[ $RESULTADO -ne 0 ]]; then
			traza $(date +"%d/%m/%Y %H:%M:%S")" - ERROR: al eliminar indice ${NOM_INDEX}: "$RESULTADO >> $LOG_FILE
			exit $RESULTADO
		fi
		i=`expr $i + 1`
	done	
	#Si se eliminan los indices se necesita el rebuild
	doRebuildIndex=1
fi
	
fi	
#----------------------------------------------------------------------------------------------------------------------
# Obtención de la lista de solicitudes pendientes, de las que deberían recuperarse sus datos históricos
#----------------------------------------------------------------------------------------------------------------------

traza 'paso 3: obtiene lista de solicitudes pendientes'
traza "doPaso3?: ${doPaso3}"

if [ $doPaso3 -eq 1 ]; then

	AHORA=$(date +"%Y%m%d%H%M%S")
	CLAVEFICHERO=$AHORA

	traza Ficheros obtenidos de BD con referencia: $CLAVEFICHERO

	# nombre del fichero con la lista de solicitudes pendientes (sin ruta, Oracle siempre lo creará en el directorio temporal)
	FICHERO_SOLICITUDES_PENDIENTES=solicitudes_pendientes.$CLAVEFICHERO.tmp
	FICHERO_SOLICITUDES_INCIDENCIAS=solicitudes_incidencias.$CLAVEFICHERO.tmp


	# llamada a la función pl/sql 'solicitud_hist.get_fichero_pendientes', que crea un fichero con las solicitudes pendientes
	# en formato de texto separado por '|'
	sqlplus -s $CONEXION_ORACLE > $TMP_LOG_FILE << EOF
		set serveroutput on;
		declare
			l_num_solicitudes_pendientes number;
		begin
			l_num_solicitudes_pendientes := solicitud_hist.get_fichero_pendientes('${FICHERO_SOLICITUDES_PENDIENTES}');
		end;
		/
		exit
EOF
	#sed '/^$/d' $TMP_LOG_FILE # igual que un 'cat' pero sin líneas en blanco
	trazafile $TMP_LOG_FILE
	if [[ $(grep -c ORA- $TMP_LOG_FILE) -ne 0 || $(grep -c ERR $TMP_LOG_FILE) -ne 0 ]]; then
		traza "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
		cat $TMP_LOG_FILE >> $TMP_LOG_FILE_DOS
		doMail "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
		sendMail
		exit 154
	fi
	rm $TMP_LOG_FILE

	#recuperamos tambien las operaciones incidencia pl/sql 'solicitud_hist.get_fichero_incidencias'
	sqlplus -s $CONEXION_ORACLE > $TMP_LOG_FILE << EOF
		set serveroutput on;
		declare
			l_num_solicitudes_pendientes number;
		begin
			l_num_solicitudes_pendientes := solicitud_hist.get_fichero_incidencias('${FICHERO_SOLICITUDES_INCIDENCIAS}');
		end;
		/
		exit
EOF
	#sed '/^$/d' $TMP_LOG_FILE # igual que un 'cat' pero sin líneas en blanco
	trazafile $TMP_LOG_FILE

	if [[ $(grep -c ORA- $TMP_LOG_FILE) -ne 0 || $(grep -c ERR $TMP_LOG_FILE) -ne 0 ]]; then
		traza "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
		cat $TMP_LOG_FILE >> $TMP_LOG_FILE_DOS
		doMail "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
		sendMail
		exit 154
	fi
	rm $TMP_LOG_FILE

	# añade la ruta al fichero de solicitudes pendientes
	FICHERO_SOLICITUDES_PENDIENTESaux=$TMP_DIR_PREF/$FICHERO_SOLICITUDES_PENDIENTES
	FICHERO_SOLICITUDES_PENDIENTES=$TMP_DIR/$FICHERO_SOLICITUDES_PENDIENTES
	FICHERO_SOLICITUDES_INCIDENCIASaux=$TMP_DIR_PREF/$FICHERO_SOLICITUDES_INCIDENCIAS
	FICHERO_SOLICITUDES_INCIDENCIAS=$TMP_DIR/$FICHERO_SOLICITUDES_INCIDENCIAS

	if [[ $SERVER_ACTUAL != $SERVER_REMOTO ]]; then

		rm -f $FICHERO_SOLICITUDES_PENDIENTESaux
	  obtener_fichero $FICHERO_SOLICITUDES_PENDIENTESaux $TMP_DIR
		
		rm -f $FICHERO_SOLICITUDES_INCIDENCIASaux
	  obtener_fichero $FICHERO_SOLICITUDES_INCIDENCIASaux $TMP_DIR
	fi


	# comprueba el fichero de solicitudes pendientes
	if [[ ! -f $FICHERO_SOLICITUDES_PENDIENTES ]]; then
		traza "ERROR: No se ha creado el fichero '"$FICHERO_SOLICITUDES_PENDIENTES"' con las solicitudes pendientes."
		sendMailError "ERROR: No se ha creado el fichero '"$FICHERO_SOLICITUDES_PENDIENTES"' con las solicitudes pendientes."
		exit 155
	fi
	if [[ ! -f $FICHERO_SOLICITUDES_INCIDENCIAS ]]; then
		traza "ERROR: No se ha creado el fichero '"$FICHERO_SOLICITUDES_INCIDENCIAS"' con las solicitudes incidencias."
		sendMailError "ERROR: No se ha creado el fichero '"$FICHERO_SOLICITUDES_INCIDENCIAS"' con las solicitudes incidentes."
		exit 155
	fi

	# número de solicitudes pendientes
	NUM_SOLICITUDES_PENDIENTES=$(wc -l $FICHERO_SOLICITUDES_PENDIENTES | awk '{print $1}')
	traza "solicitudes pendientes: "$NUM_SOLICITUDES_PENDIENTES
	resum "Solicitudes pendientes: "$NUM_SOLICITUDES_PENDIENTES
	NUM_SOLICITUDES_INCIDENCIAS_TOT=$(wc -l $FICHERO_SOLICITUDES_INCIDENCIAS | awk '{print $1}')
	traza "solicitudes incidencias: "$NUM_SOLICITUDES_INCIDENCIAS_TOT
	resum "solicitudes incidencias: "$NUM_SOLICITUDES_INCIDENCIAS_TOT

	cat $FICHERO_SOLICITUDES_INCIDENCIAS >> $FICHERO_SOLICITUDES_PENDIENTES
	NUM_SOLICITUDES_PENDIENTES=$(wc -l $FICHERO_SOLICITUDES_PENDIENTES | awk '{print $1}')

fi

#----------------------------------------------------------------------------------------------------------------------
# Obtención de la lista de claves de solicitud que hay cargadas en la tabla de históricos
#----------------------------------------------------------------------------------------------------------------------
traza 'paso 4: obtiene lista de claves de solicitud cargadas'
traza "doPaso4?: ${doPaso4}"


if [ $doPaso4 -eq 1 ]; then


	# nombre del fichero con la lista de claves de solicitud cargadas (sin ruta, Oracle siempre lo creará en el directorio temporal)
	FICHERO_CLAVES_CARGADAS=claves_cargadas.$CLAVEFICHERO.tmp
	rm -f $FICHERO_CLAVES_CARGADAS

	FICHEROS_A_OBTENER=$TMP_DIR/ficheros_a_obtener.$CLAVEFICHERO.tmp
	rm -f $FICHEROS_A_OBTENER

	# llamada a la función pl/sql 'solicitud_hist.get_fichero_claves_cargadas', que crea un fichero con las claves de solicitud
	# cargadas en la tabla de históricos, en formato de texto separado por '|'
	sqlplus -s $CONEXION_ORACLE > $TMP_LOG_FILE << EOF
		set serveroutput on;
		declare
			l_num_claves_cargadas number;
		begin
			l_num_claves_cargadas := solicitud_hist.get_fichero_claves_cargadas('${FICHERO_CLAVES_CARGADAS}');
		end;
		/
		exit
EOF
	#sed '/^$/d' $TMP_LOG_FILE # igual que un 'cat' pero sin líneas en blanco
	trazafile $TMP_LOG_FILE
	if [[ $(grep -c ORA- $TMP_LOG_FILE) -ne 0 || $(grep -c ERR $TMP_LOG_FILE) -ne 0 ]]; then
		traza "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
		cat $TMP_LOG_FILE >> $TMP_LOG_FILE_DOS
		doMail "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
		sendMail
		exit 154
	fi
	rm $TMP_LOG_FILE

	# añade la ruta al fichero de solicitudes pendientes
	FICHERO_CLAVES_CARGADASaux=$TMP_DIR_PREF/$FICHERO_CLAVES_CARGADAS
	FICHERO_CLAVES_CARGADAS=$TMP_DIR/$FICHERO_CLAVES_CARGADAS

	if [[ $SERVER_ACTUAL != $SERVER_REMOTO ]]; then

		rm -f $FICHERO_CLAVES_CARGADASaux
	  obtener_fichero $FICHERO_CLAVES_CARGADASaux $TMP_DIR	
	fi

# comprueba el fichero de claves cargadas
	if [[ ! -f $FICHERO_CLAVES_CARGADAS ]]; then
		traza "ERROR: No se ha creado el fichero '"$FICHERO_CLAVES_CARGADAS"' con las claves cargadas."
		sendMailError "ERROR: No se ha creado el fichero '"$FICHERO_CLAVES_CARGADAS"' con las claves cargadas."
		exit 155
	fi

	# número de claves cargadas
	NUM_CLAVES_CARGADAS=$(wc -l $FICHERO_CLAVES_CARGADAS | awk '{print $1}')
	traza "claves cargadas: "$NUM_CLAVES_CARGADAS

fi


#----------------------------------------------------------------------------------------------------------------------
# los pasos 4, 5 y 6 no hacen falta si no hay solicitudes pendientes
if [[ $NUM_SOLICITUDES_PENDIENTES -lt 1 ]]; then
doPaso5=0
doPaso6=0
doPaso7=0
doPaso8=0
doPaso9=0
doPaso10=0
doPaso11=0
doPaso13=0
CONTINUAR=0
traza "No hay peticiones. pasos 4..13 desactivados "
fi

#----------------------------------------------------------------------------------------------------------------------
# Calcula partición y semana, y obtiene el nombre de los ficheros a recuperar
#----------------------------------------------------------------------------------------------------------------------
traza 'paso 5: calcula partición y semana, y obtiene el nombre de los ficheros a recuperar'
traza "doPaso5?: ${doPaso5}"

if [ $doPaso5 -eq 1 ]; then

	# ruta y nombre del fichero de solicitudes pendientes con las claves desplegadas
	# (el que devuelve oracle tiene un registro por solicitud, este tendrá un registro por clave solicitud-tipoTerminal-terminal-fecha)
	CLAVES_PENDIENTES=$TMP_DIR/claves_solicitudes_pendientes.$CLAVEFICHERO.tmp
	rm -f $CLAVES_PENDIENTES

	# resigue el fichero de solicitudes pendientes creado por oracle...
	cat $FICHERO_SOLICITUDES_PENDIENTES | while read SOLICITUD_PENDIENTE
	do

		# obtiene sus campos de la solicitud
		ID=$(echo $SOLICITUD_PENDIENTE | awk -F'|' -v pos=${POS_CAMPO_ID} '{print $pos}')
		TIPO_TERMINAL=$(echo $SOLICITUD_PENDIENTE | awk -F'|' -v pos=${POS_CAMPO_TIPO_TERMINAL} '{print $pos}')
		TERMINAL=$(echo $SOLICITUD_PENDIENTE | awk -F'|' -v pos=${POS_CAMPO_TERMINAL} '{print $pos}')		
		FECHA_INI=$(echo $SOLICITUD_PENDIENTE | awk -F'|' -v pos=${POS_CAMPO_FECHA_INI} '{print $pos}')
		FECHA_FIN=$(echo $SOLICITUD_PENDIENTE | awk -F'|' -v pos=${POS_CAMPO_FECHA_FIN} '{print $pos}')
		SOPORTE=$(echo $SOLICITUD_PENDIENTE | awk -F'|' -v pos=${POS_CAMPO_SOPORTE} '{print $pos}')
		TIPO=$(echo $SOLICITUD_PENDIENTE | awk -F'|' -v pos=${POS_CAMPO_TIPO} '{print $pos}')
		
		traza "Invocando particion:= utils.get_num_particion(${TIPO_TERMINAL},$TERMINAL); usando ${TMP_LOG_FILE}"
		
		TERMINAL="$(echo "$(sqlescape "$TERMINAL")")"
		
		# calcula la partición correspondiente al tipo terminal/terminal
		#PARTICION=$((10#$OFICINA % $NUM_PARTICIONES))
		sqlplus -s $CONEXION_ORACLE > $TMP_LOG_FILE << EOF
			set serveroutput on;
			declare
				particion number;
			begin
				particion:= utils.get_num_particion('${TIPO_TERMINAL}','${TERMINAL}');
				dbms_output.put_line('PARTICION=' || particion);
			exception when others then
				dbms_output.put_line('ERRCOD: ' || sqlcode);
				dbms_output.put_line('ERRMSG: ' || sqlerrm);
			end;
			/
			exit
EOF
		TERMINAL="$(echo "$(sqlunescape "$TERMINAL")")"
		
		traza "Fin invocando particion:= utils.get_num_particion(${TIPO_TERMINAL},$TERMINAL);"
		if [[ $(grep -c ORA- $TMP_LOG_FILE) -ne 0 || $(grep -c ERR $TMP_LOG_FILE) -ne 0 ]]; then
			traza "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
			rm -f $TMP_LOG_FILE_DOS
			cat $TMP_LOG_FILE > $TMP_LOG_FILE_DOS
			marcarIncidente $ID SQL
			rm -f $TMP_LOG_FILE_DOS
			exit 158
		fi

		PARTICION=$(grep PARTICION= ${TMP_LOG_FILE} | awk -F= '{print($2)}')
		#PARTICION=12
		echo "particion: ${PARTICION}"
		#cat $TMP_LOG_FILE >> /apps/decloi/batch/logs/prova.txt
		rm $TMP_LOG_FILE

		# resigue los días desde la fecha inicial a la fecha final...
		FECHA=$FECHA_INI
		
		traza "Tratando peticion $ID para TipoTerm-Terminal (${TIPO_TERMINAL} - ${TERMINAL}) desde $FECHA hasta $FECHA_FIN. (Partición ${PARTICION}) "

		while [[ $FECHA -le $FECHA_FIN ]]; do
			FECHA_MDY=$(echo $FECHA | awk '{print substr($1, 5, 2) "/" substr($1, 7, 2) "/" substr($1, 1, 4)}')
			#echo $(date +"%Y%m%d%H%M%S") "Fecha MDY $FECHA_MDY"
			
			# calcula la semana correspondiente a la fecha
			SEMANA=$(date --date="$FECHA_MDY" +"%G%V")
			
			# calcula el nombre del fichero histórico que debe haber en cintas magnéticas
			FICHERO=${SEMANA}_$(echo $PARTICION | awk -F: '{printf("%02d", $1)}')

			# escribe un registro en el fichero de solicitudes pendientes con las claves desplegadas
			echo $ID"|"$PARTICION"|"$TIPO_TERMINAL"|"$TERMINAL"|"$SOPORTE"|"$TIPO"|"$SEMANA"|"$FECHA"|"$FICHERO >> $CLAVES_PENDIENTES

			# comprueba si la clave en cuestión existe ya en la tabla de históricos, buscándola en el fichero
			# de claves cargadas; si es así no la vuelve a cargar
			TERMINAL="$(echo "$(grepescape "${TERMINAL}")")"
			grepescape ${TERMINAL}

			echo "terminal : ${TERMINAL}"
			if [[ $(grep "^${TIPO_TERMINAL}|${TERMINAL}|${FECHA}" $FICHERO_CLAVES_CARGADAS | wc -l) -lt 1 ]]; then
					# Si no estaba se deben activar tambien para esta peticion
					if [[ $(grep "^${SEMANA}|${PARTICION}|${FICHERO}" $FICHEROS_A_OBTENER | wc -l) -lt 1 ]]; then
						echo "${SEMANA}|${PARTICION}|${FICHERO}" >> $FICHEROS_A_OBTENER
						traza "($ID) Datos TipoTerm: ${TIPO_TERMINAL} Terminal: ${TERMINAL} FECHA: ${FECHA}. NO en la BD. Se pide ${FICHERO} en "$FICHEROS_A_OBTENER
					else 
						traza "($ID) Datos TipoTerm: ${TIPO_TERMINAL} Terminal: ${TERMINAL} FECHA: ${FECHA}. NO en la BD. (fichero ${FICHERO} ya pedido en el proceso)"
					fi	
			else
					if [[ $(grep "^${TIPO_TERMINAL}|${TERMINAL}|${FECHA}" $FICHERO_CLAVES_ACTUALES | wc -l) -lt 1 ]]; then
						echo "${TIPO_TERMINAL}|${TERMINAL}|${FECHA}" >> $FICHERO_CLAVES_ACTUALES
						traza "($ID) Datos TipoTerm: ${TIPO_TERMINAL} Terminal: ${TERMINAL} FECHA: ${FECHA}. SI en la BD. Clave apuntada en "$FICHERO_CLAVES_ACTUALES
						
					else
						traza "($ID) Datos TipoTerm: ${TIPO_TERMINAL} Terminal: ${TERMINAL} FECHA: ${FECHA}. SI en la BD. (Clave ya apuntada en el proceso)" 
					fi	
							
			fi
			TERMINAL="$(echo "$(grepunescape "${TERMINAL}")")"
			
			
			FECHA=$(date --date="1 days $FECHA_MDY 12:00" +"%Y%m%d")
		done
	done
	traza "claves de solicitudes pendientes: "$(wc -l $CLAVES_PENDIENTES | awk '{print $1}')

	# ruta y nombre del fichero con la lista de ficheros a recuperar
	FICHEROS_A_RECUPERAR=$TMP_DIR/ficheros_a_recuperar.$CLAVEFICHERO.tmp
	rm -f $FICHEROS_A_RECUPERAR

	# a partir del fichero de solicitudes pendientes con las claves desplegadas, obtiene los ficheros a recuperar:
	# mostrando solo semana, partición y nombre de fichero, ordenando por semana y partición, y estableciendo la
	# opción '-u' que no muestra registros duplicados
	cat $CLAVES_PENDIENTES | awk -F'|' '{print $7 "|" $2 "|" $9}' | sort -u > $FICHEROS_A_RECUPERAR	
	RESULTADO=$?
	if [[ $RESULTADO -ne 0 ]]; then
		traza "ERROR: Al obtener los ficheros a recuperar, el comando "'sort'" devolvió la respuesta "$RESULTADO.
		sendMailError "ERROR: Al obtener los ficheros a recuperar, el comando "'sort'" devolvió la respuesta "$RESULTADO.
		exit 156
	fi
	
	if [ ! -f $FICHEROS_A_OBTENER ]; then
		traza "hacemos un touch al fichero ${FICHEROS_A_OBTENER} para evitar que falle el split."
		touch ${FICHEROS_A_OBTENER}
		
	fi
	traza "ficheros a recuperar: "$(wc -l $FICHEROS_A_RECUPERAR | awk '{print $1}')
	traza "ficheros a obtener: "$(wc -l $FICHEROS_A_OBTENER | awk '{print $1}')

	# ruta y nombre del fichero con las claves a tratar
	CLAVES_A_TRATAR=$TMP_DIR/claves.$CLAVEFICHERO.tmp
	rm -f $CLAVES_A_TRATAR

	# a partir del fichero de solicitudes pendientes con las claves desplegadas, obtiene las claves a tratar:
	# mostrando solo soporte, fichero, tipoTerminal, terminal , fecha y tipo, y pasándolo por un 'uniq' que no muestra duplicados
	cat $CLAVES_PENDIENTES | awk -F'|' '{print $5 "|" $9 "|" $3 "|" $4 "|" $8 "|" $6}' | uniq > $CLAVES_A_TRATAR	
	RESULTADO=$?
	if [[ $RESULTADO -ne 0 ]]; then
		traza "ERROR: Al obtener las claves a tratar, se devolvió la respuesta $RESULTADO."
		sendMailError "ERROR: Al obtener las claves a tratar, se devolvió la respuesta $RESULTADO."
		exit 157
	fi
	traza "claves a tratar: "$(wc -l $CLAVES_A_TRATAR | awk '{print $1}')

fi	



#----------------------------------------------------------------------------------------------------------------------
# Recuperación de ficheros históricos de cinta y carga de las solicitudes para soporte Pantalla (Fichero solo preparación)
#----------------------------------------------------------------------------------------------------------------------
traza 'paso 6: recupera los ficheros de cinta magnética'
traza "doPaso6?: ${doPaso6}"

if [ $doPaso6 -eq 1 ]; then

	#split --lines=$NUM_PARALEL_READ --suffix-length=2 --numeric-suffixes $FICHEROS_A_RECUPERAR $FICHEROS_A_RECUPERAR.
	split --lines=$NUM_PARALEL_READ --suffix-length=2 --numeric-suffixes $FICHEROS_A_OBTENER $FICHEROS_A_OBTENER.
	RESULTADO=$?
	if [[ $RESULTADO -ne 0 ]]; then
		#echo ERROR: Al dividir los ficheros a recuperar "'"$FICHEROS_A_RECUPERAR"'" en grupos de $NUM_PARALEL_READ, el comando "'split'" devolvió la respuesta $RESULTADO.
		traza "ERROR: Al dividir los ficheros a recuperar '"$FICHEROS_A_OBTENER"' en grupos de ${NUM_PARALEL_READ}, el comando 'split' devolvió la respuesta ${RESULTADO}."
		sendMailError "ERROR: Al dividir los ficheros a recuperar '"$FICHEROS_A_OBTENER"' en grupos de ${NUM_PARALEL_READ}, el comando 'split' devolvió la respuesta ${RESULTADO}."
		exit 159
	fi

	# ruta y nombre del fichero donde van a ir a parar temporalmente todos los datos históricos necesarios para
	# posteriormente confeccionar los ficheros csv correspondientes a las solicitudes para soporte Fichero
	GRAN_FICHERO_DE_CSV=$TMP_DIR/csv.tmp
	rm -f $GRAN_FICHERO_DE_CSV

	# número de recuperación
	POS_RECUPERACION=0
		
		# resigue cada uno de los ficheritos (divididos/agrupados por recuperación) que contienen los ficheros a recuperar
		#ls -l $FICHEROS_A_RECUPERAR.?? | awk '{print $9}' | while read FICHERO_RECUPERACION	
		SIN_ESPACIO=0
		
		ls -l $FICHEROS_A_OBTENER.?? | awk '{print $9}' | while read FICHERO_RECUPERACION
		do
		
			traza "Tratando fichero recuperado:"$FICHERO_RECUPERACION
			traza "Variable SALIR: "$SIN_ESPACIO
			checkContinuar
			if [[ $CONTINUAR == 0 ]]; then
					traza "Saliendo del bucle de obtencion de ficheros"
					break;
			fi
			
			if [[ $SIN_ESPACIO == 1 ]]; then
					traza "Saliendo del bucle de obtencion de ficheros"
					break;
			fi
			
			while read LINEA_FICHERO_A_RECUPERAR
			do

				DIRECTORIO=$IMP_DIR
				FILESYSTEM_OCUPADO=$(df -Pk $DIRECTORIO | grep '%' | grep '/' | awk '{print $5}' | awk -F% '{print $1}')
				traza "Filesystem ${DIRECTORIO} al ${FILESYSTEM_OCUPADO}%"
				if [[ $FILESYSTEM_OCUPADO -gt $LIMITE ]]; then
					traza "Filesystem de ${DIRECTORIO} al COMPLETO!!! ${FILESYSTEM_OCUPADO}%"		
					SIN_ESPACIO=1
				fi
				
				DIRECTORIO=$TMP_DIR
				FILESYSTEM_OCUPADO=$(df -Pk $DIRECTORIO | grep '%' | grep '/' | awk '{print $5}' | awk -F% '{print $1}')
				traza "Filesystem ${DIRECTORIO} al ${FILESYSTEM_OCUPADO}%"
				if [[ $FILESYSTEM_OCUPADO -gt $LIMITE ]]; then
					traza "Filesystem de ${DIRECTORIO} al COMPLETO!!! ${FILESYSTEM_OCUPADO}%"		
					SIN_ESPACIO=1
				fi
			
				traza "Variable SALIR: "$SIN_ESPACIO
				checkContinuar
				if [[ $SIN_ESPACIO == 1 ]]; then
					traza "Saliendo del bucle de obtencion de ficheros"
					break;
				fi


				if [[ $CONTINUAR == 0 ]]; then
					traza "Saliendo del bucle de obtencion de ficheros por no poder continuar (CONTINUAR=0)"
					break;
				fi
			
				FICHERO_A_RECUPERAR=$(echo $LINEA_FICHERO_A_RECUPERAR | awk -F'|' '{print $3}')
				traza "    fichero a recuperar: '"$FICHERO_A_RECUPERAR".zip'"

				# ATENCIÓN:
				# aquí debería lanzar el comando necesario para indicar al servicio de cintas magnéticas que debe recuperar
				# el fichero pero, como no tenemos (o no sabemos) entorno de pruebas para dicho servicio, de momento y con
				# carácter de pruebas, lo recuperamos del directorio '${USER_HOME}/data/exp/~cinta'; obviamente, no hay
				# paralelización
				COMPLETO_FICHERO_A_RECUPERAR=${EXP_DIR}/~cinta/${FICHERO_A_RECUPERAR}.zip
				traza "    fichero completo a recuperar: '"$COMPLETO_FICHERO_A_RECUPERAR"'"
				FICHERO_ZIP_RECUPERADO=${IMP_DIR}/${FICHERO_A_RECUPERAR}.zip
				if [ -f $FICHERO_ZIP_RECUPERADO ]; then
					traza " Fichero ya en el temporal de importación."
					echo "No procesa." > $TMP_LOG_FILE_CINTA
				elif [[ -f $COMPLETO_FICHERO_A_RECUPERAR ]]; then
					cp $COMPLETO_FICHERO_A_RECUPERAR $FICHERO_ZIP_RECUPERADO
					traza " Recuperamos el fichero del temporal de exp/~cinta."
					echo "No procesa." > $TMP_LOG_FILE_CINTA
				elif [ -f ${IMP_DIR}/${FICHERO_A_RECUPERAR}.txt ]; then
					traza " Fichero ya en el temporal de importación."
					echo "No procesa." > $TMP_LOG_FILE_CINTA
				else
					traza " El script predescarrega-fitxer-historificat.sh ha fallado o no se ha ejecutado, se procede a recuperar el fichero de cinta."
					if [ $doScript -eq 1 ]; then
							yes 3 | sudo -u root /tools/scripts/decdec/dsmcDEC${APLICACION}.sh 1 $COMPLETO_FICHERO_A_RECUPERAR $IMP_DIR"/"	> $TMP_LOG_FILE_CINTA
							RESULTADO=$?
							traza "Lanzando recuperación: yes 3 | sudo -u root /tools/scripts/decdec/dsmcDEC${APLICACION}.sh 1 ${COMPLETO_FICHERO_A_RECUPERAR} ${IMP_DIR}/ "				
					
							if [[ $RESULTADO -ne 0 ]]; then
									traza "Recuperación Cinta: Código retorno inesperado (${RESULTADO}) al buscar ${COMPLETO_FICHERO_A_RECUPERAR} usando dsmcDEC${APLICACION}.sh"
									resum "Recuperación Cinta: Código retorno inesperado (${RESULTADO}) al buscar ${COMPLETO_FICHERO_A_RECUPERAR} usando dsmcDEC${APLICACION}.sh"
							fi #DEBUG
					else
							dsmc restore "${COMPLETO_FICHERO_A_RECUPERAR}" "${FICHERO_ZIP_RECUPERADO}" > $TMP_LOG_FILE_CINTA
							RESULTADO=$?
							traza "Lanzando recuperación: dsmc restore ${COMPLETO_FICHERO_A_RECUPERAR} ${FICHERO_ZIP_RECUPERADO}"
							
							if [[ $RESULTADO -ne 0 ]]; then
									traza "Recuperación Cinta: Código retorno inesperado (${RESULTADO}) al buscar ${COMPLETO_FICHERO_A_RECUPERAR} usando restore"
									resum "Recuperación Cinta: Código retorno inesperado (${RESULTADO}) al buscar ${COMPLETO_FICHERO_A_RECUPERAR} usando restore"
							fi #DEBUG
					fi	
				fi
				trazafile $TMP_LOG_FILE_CINTA
				
				echo "buscando "$FICHERO_ZIP_RECUPERADO 
				
				if [[ -f $FICHERO_ZIP_RECUPERADO ]]; then
					#unzip -u $IMP_DIR/$FICHERO_A_RECUPERAR.zip -d $IMP_DIR
					echo "descomprimiendo "$IMP_DIR/$FICHERO_A_RECUPERAR.zip 				
					gunzip -S .zip $IMP_DIR/$FICHERO_A_RECUPERAR.zip				
					mv $IMP_DIR/$FICHERO_A_RECUPERAR $IMP_DIR/$FICHERO_A_RECUPERAR.txt
					traza "Existe el fichero '"$EXP_DIR"/~cinta/"$FICHERO_A_RECUPERAR".zip' en cinta y se descomprime en "$IMP_DIR
					echo "creado "$IMP_DIR/$FICHERO_A_RECUPERAR.txt
					borraMaquinas_as $IMP_DIR/$FICHERO_A_RECUPERAR.zip
					RESULTADO=$?
					if [[ $RESULTADO -ne 0 ]]; then
						traza "Error al intentar borrar el fichero ${IMP_DIR}/${FICHERO_A_RECUPERAR}.zip en alguna de las máquinas AS."
					fi
					echo $IMP_DIR/$FICHERO_A_RECUPERAR.txt >> $FICHERO_BORRADO_TXT
				elif [ -f ${IMP_DIR}/${FICHERO_A_RECUPERAR}.txt ]; then
					echo "No se descomprime .zip. Fichero " ${FICHERO_A_RECUPERAR}.txt "ya existente."
					echo $IMP_DIR/$FICHERO_A_RECUPERAR.txt >> $FICHERO_BORRADO_TXT
				else
					traza "no existe el fichero '"$EXP_DIR"/~cinta/"$FICHERO_A_RECUPERAR".zip' en cinta"
					inc "no existe el fichero '"$EXP_DIR"/~cinta/"$FICHERO_A_RECUPERAR".zip' en cinta"
					echo -e "Linea del fichero a recuperar: ${LINEA_FICHERO_A_RECUPERAR}" >> $TMP_LOG_FILE_DOS
					cat $TMP_LOG_FILE_CINTA >> $TMP_LOG_FILE_DOS
					
					doMail "no existe el fichero '"$EXP_DIR"/~cinta/"$FICHERO_A_RECUPERAR".zip' en cinta"
				fi
							
							# ATENCIÓN:
							# esto es solo para probar
							#if [[ -f $EXP_DIR/~cinta/$FICHERO_A_RECUPERAR.zip ]]; then
							#	cp $EXP_DIR/~cinta/$FICHERO_A_RECUPERAR.zip $IMP_DIR/$FICHERO_A_RECUPERAR.zip
							#	unzip -u $IMP_DIR/$FICHERO_A_RECUPERAR.zip -d $IMP_DIR
							#	rm $IMP_DIR/$FICHERO_A_RECUPERAR.zip
							#else
							#	echo no existe el fichero "'"$EXP_DIR/~cinta/$FICHERO_A_RECUPERAR.zip"'" en cinta
							#fi
							# esto es solo para probar
							
				rm $TMP_LOG_FILE_CINTA

			done < $FICHERO_RECUPERACION
			# recuperación siguiente
			((POS_RECUPERACION+=1))
		done

	fi


	
#----------------------------------------------------------------------------------------------------------------------
# Prepara el fichero de carga con los registros a insertar a partir de los ficheros recuperados
#----------------------------------------------------------------------------------------------------------------------
traza 'paso 7: Prepara fichero de carga'
traza "doPaso7?: ${doPaso7}"

if [ $doPaso7 -eq 1 ]; then

		
		# ATENCIÓN:
		# aquí debería haber el mecanismo de espera que notificara que ha terminado la recuperación de los ficheros y
		# el correspondiente tratamiento del código de retorno; como no hay paralelización porque no hay servicio de
		# cintas de pruebas, en estos momentos no existe ni sucedanio de dicho mecanismo
		

	# número de recuperación
	POS_RECUPERACION=0

	
	# resigue cada uno de los ficheritos (divididos/agrupados por recuperación) que contienen los ficheros a recuperar
	#ls -l $FICHEROS_A_RECUPERAR.?? | awk '{print $9}' | while read FICHERO_RECUPERACION
	
	ls -l $FICHEROS_A_OBTENER.?? | awk '{print $9}' | while read FICHERO_RECUPERACION
	do
		traza "Tratando fichero recuperación: "$FICHERO_RECUPERACION

		# ruta y nombre del fichero de datos a cargar correspondiente a la recuperación en curso
		GRAN_FICHERO_A_CARGAR=$TMP_DIR/carga_$POS_RECUPERACION.tmp
		rm -f $GRAN_FICHERO_A_CARGAR
		
		# resigue, de nuevo, cada uno de los ficheros correspondientes a dicha recuperación...
		cat $FICHERO_RECUPERACION | while read LINEA_FICHERO_RECUPERADO
		do
			FICHERO_RECUPERADO=$(echo $LINEA_FICHERO_RECUPERADO | awk -F'|' '{print $3}')
			traza "Tratando fichero recuperado: "$FICHERO_RECUPERADO
			
			# puede ser que el fichero no se haya recuperado porque no existe
			if [[ ! -f $IMP_DIR/$FICHERO_RECUPERADO.txt ]]; then
				traza "ADVERTENCIA: No se recuperó el fichero '"$FICHERO_RECUPERADO"' porque no existía en cinta"
			else
				traza "Se recuperó el fichero '"$FICHERO_RECUPERADO"' y comienza a tratarse"
			
				# filtra y resigue las claves a tratar que coincidan con el fichero recuperado
				grep "^.*|${FICHERO_RECUPERADO}" $CLAVES_A_TRATAR | while read CLAVE_A_TRATAR
				do
				
					# obtiene los campos soporte, oficina y fecha
					SOPORTE=$(echo $CLAVE_A_TRATAR | awk -F'|' '{print $1}')
					TIPO_TERMINAL=$(echo $CLAVE_A_TRATAR | awk -F'|' '{print $3}')
					TERMINAL=$(echo $CLAVE_A_TRATAR | awk -F'|' '{print $4}')
					TERMINAL_ENCODE="$(echo "$(urlencode "${TERMINAL}")")"
					echo "TERMINAL_ENCODE:" $TERMINAL_ENCODE
					FECHA=$(echo $CLAVE_A_TRATAR | awk -F'|' '{print $5}')
					
					YA_EXISTE=NO
					#NOMBRE_FICHERO="carga_"$TIPO_TERMINAL"_"$TERMINAL"_"$FECHA
					NOMBRE_FICHERO="carga_"$TIPO_TERMINAL"_"$TERMINAL_ENCODE"_"$FECHA
					FICHERO_VOLCADO=$TMP_DIR"/"$NOMBRE_FICHERO".tmp"									
					
					# para soporte Pantalla, se concatenan los datos al fichero de carga
					if [[ $SOPORTE == 'P' ]]; then
						GRAN_FICHERO=$GRAN_FICHERO_A_CARGAR
						
						# comprueba si la clave en cuestión existe ya en la tabla de históricos, buscándola en el fichero
						# de claves cargadas; si es así no la vuelve a cargar
						
						echo "buscando ${TIPO_TERMINAL}|${TERMINAL}|${FECHA} en "$FICHERO_CLAVES_CARGADAS
						TERMINAL="$(echo "$(grepescape "${TERMINAL}")")"
						if [[ $(grep "^${TIPO_TERMINAL}|${TERMINAL}|${FECHA}" $FICHERO_CLAVES_CARGADAS | wc -l) -gt 0 ]]; then
							YA_EXISTE=SI
						fi
						
					# para soporte Fichero, se concatenan los datos al fichero de csv
					else
						GRAN_FICHERO=$GRAN_FICHERO_DE_CSV
					fi
					
					# solo si la clave no está ya cargada...
					if [[ $YA_EXISTE != "SI" ]]; then
						#if [[ $(grep "^${TIPO_TERMINAL}|${TERMINAL}|${FECHA}" $FICHERO_CLAVES_VOLCADAS | wc -l) -gt 0 ]]; then
						if [[ $(grep "^${TIPO_TERMINAL}|${TERMINAL}|${FECHA}" $FICHERO_CLAVES_VOLCADAS | wc -l) -gt 0 ]]; then
								traza "La clave TipoTerm: ${TIPO_TERMINAL} Terminal: ${TERMINAL} FECHA: ${FECHA} ya se ha volcado previamente"
						else 	
								# Se vuelva los datos de la oficina para esa fecha en el fichero a cargar
								traza "Cargando TipoTerm: ${TIPO_TERMINAL} Terminal: ${TERMINAL} FECHA: ${FECHA} sobre ${FICHERO_VOLCADO}"
						
								# filtra los datos del fichero recuperado con la clave en tratamiento y los concatena al fichero correspondiente
								#grep --text "^${TIPO_TERMINAL}|${TERMINAL}|[^|]*|[^|]*|${FECHA}" $IMP_DIR/$FICHERO_RECUPERADO.txt >> $GRAN_FICHERO
								#grep --text "^${TIPO_TERMINAL}|${TERMINAL}|[^|]*|[^|]*|${FECHA}" $IMP_DIR/$FICHERO_RECUPERADO.txt >> $FICHERO_VOLCADO
								#grep --text "${FECHA}[^|]*|${TIPO_TERMINAL}|${TERMINAL}|" $IMP_DIR/$FICHERO_RECUPERADO.txt >> $GRAN_FICHERO
								#grep --text "${FECHA}[^|]*|${TIPO_TERMINAL}|${TERMINAL}|" $IMP_DIR/$FICHERO_RECUPERADO.txt >> $FICHERO_VOLCADO
								grep --text "^[0-9]\{1,2\}|${FECHA}[^|]*|${TIPO_TERMINAL}|${TERMINAL}|" $IMP_DIR/$FICHERO_RECUPERADO.txt >> $GRAN_FICHERO
								grep --text "^[0-9]\{1,2\}|${FECHA}[^|]*|${TIPO_TERMINAL}|${TERMINAL}|" $IMP_DIR/$FICHERO_RECUPERADO.txt >> $FICHERO_VOLCADO
								echo "grep ^[0-9]\{1,2\}|${FECHA}[^|]*|${TIPO_TERMINAL}|${TERMINAL}| sobre $IMP_DIR/$FICHERO_RECUPERADO. lineas: "$(wc -l $IMP_DIR/$FICHERO_RECUPERADO.txt | awk '{print $1}')". Resultado grep: " $(wc -l $GRAN_FICHERO | awk '{print $1}')
								echo "${NOMBRE_FICHERO}|${TIPO_TERMINAL}|${TERMINAL}|${FECHA}" >> $FICHERO_FICHEROS_CARGADOS
								echo "${TIPO_TERMINAL}|${TERMINAL}|${FECHA}" >> $FICHERO_CLAVES_VOLCADAS
						fi		
					else
								# Si no estaba se deben activar tambien para esta peticion
							if [[ $(grep "^${TIPO_TERMINAL}|${TERMINAL}|${FECHA}" $FICHERO_CLAVES_VOLCADAS | wc -l) -lt 1 ]]; then
								echo "${TIPO_TERMINAL}|${TERMINAL}|${FECHA}" >> $FICHERO_CLAVES_VOLCADAS
							fi	
					fi
					TERMINAL="$(echo "$(grepunescape "${TERMINAL}")")"
				done

				# todos los datos útiles del fichero recuperado ya están en algún lado: para soporte Pantalla están en el
				# fichero de carga a punto de ser cargados; para Fichero están en un fichero temporal
				# ya podemos eliminar dicho fichero
				
				borra $IMP_DIR/$FICHERO_RECUPERADO.txt #DEBUG
				
				RESULTADO=$?
				if [[ $RESULTADO -ne 0 ]]; then
					echo "ERROR: Al eliminar el fichero recuperado '"$IMP_DIR/$FICHERO_RECUPERADO".txt', el comando 'rm' devolvió la respuesta "$RESULTADO.
					sendMailError "ERROR: Al eliminar el fichero recuperado '"$IMP_DIR/$FICHERO_RECUPERADO".txt', el comando 'rm' devolvió la respuesta "$RESULTADO.
					exit 160
				fi #DEBUG
			
			fi

		done

		# los datos para soporte Fichero de la recuperación en curso están en un fichero
		# en carácter temporal para tratarlos posteriormente

		# recuperación siguiente
		((POS_RECUPERACION+=1))
	done

fi	
	
#----------------------------------------------------------------------------------------------------------------------
# Realiza la carga del fichero en la tabla de movimientos históricos
#----------------------------------------------------------------------------------------------------------------------
traza 'paso 8: Carga de los ficheros '
traza "doPaso8?: ${doPaso8}"

if [ $doPaso8 -eq 1 ]; then

  
	traza 'Carga de los ficheros de  '$FICHERO_FICHEROS_CARGADOS


		# ruta y nombre del fichero de datos a cargar correspondiente a la recuperación en curso

		# si no se han encontrado registros para las claves en los ficheros recuperados, no hace falta cargar
		if [[ ! -f $FICHERO_FICHEROS_CARGADOS ]]; then
			traza "ADVERTENCIA: no se han econtrado registros, no hay carga"
		else
		
			# resigue, de nuevo, cada uno de los ficheros correspondientes a dicha recuperación...
			cat $FICHERO_FICHEROS_CARGADOS | while read LINEA_NOMBRE_FICHERO
		do

			NOMBRE_FICHERO=$(echo $LINEA_NOMBRE_FICHERO | awk -F'|' '{print $1}')
			TIPO_TERMINAL=$(echo $LINEA_NOMBRE_FICHERO | awk -F'|' '{print $2}')
			TERMINAL="$(echo "$LINEA_NOMBRE_FICHERO" | awk -F'|' '{print $3}')"
			FECHA=$(echo $LINEA_NOMBRE_FICHERO | awk -F'|' '{print $4}')
		
			GRAN_FICHERO_A_CARGAR_TMP=$TMP_DIR"/"$NOMBRE_FICHERO".tmp"
			GRAN_FICHERO_A_CARGAR=$TMP_DIR"/"$NOMBRE_FICHERO".1.tmp"
			tr '\000\032\015' ' ' < $GRAN_FICHERO_A_CARGAR_TMP > $GRAN_FICHERO_A_CARGAR
			
			GRAN_FICHERO_A_CARGAR_IP=$TMP_DIR"/"$NOMBRE_FICHERO"_ip.tmp"
			
			# ruta y nombre del fichero de log de la carga
			LOG_GRAN_FICHERO=$TMP_DIR/$NOMBRE_FICHERO.log
			rm -f $LOG_GRAN_FICHERO

			# ruta y nombre del fichero de errores de la carga
			BAD_GRAN_FICHERO=$TMP_DIR/$NOMBRE_FICHERO.bad
			rm -f $BAD_GRAN_FICHERO

			# ruta y nombre del fichero de descartes de la carga
			DIS_GRAN_FICHERO=$TMP_DIR/$NOMBRE_FICHERO.dis
			rm -f $DIS_GRAN_FICHERO
			
			NOM_AUX="ip"
			if [[ $APLICACION == 'CA' ]];then
				NOM_AUX="activ"
			fi
			# ruta y nombre del fichero de log de la carga
			LOG_GRAN_FICHERO_IP=$TMP_DIR/$NOMBRE_FICHERO"_${NOM_AUX}".log
			rm -f $LOG_GRAN_FICHERO_IP

			# ruta y nombre del fichero de errores de la carga
			BAD_GRAN_FICHERO_IP=$TMP_DIR/$NOMBRE_FICHERO"_${NOM_AUX}".bad
			rm -f $BAD_GRAN_FICHERO_IP

			# ruta y nombre del fichero de descartes de la carga
			DIS_GRAN_FICHERO_IP=$TMP_DIR/$NOMBRE_FICHERO"_${NOM_AUX}".dis
			rm -f $DIS_GRAN_FICHERO_IP
		

		traza "Borrando datos de ${TIPO_TERMINAL} Terminal ${TERMINAL} para el dia ${FECHA}"
		
		# calcula la partición correspondiente a la oficina
		DATOS_AUX=datos_no_part_ip_hist
		if [[ $APLICACION == 'CA' ]];then
			DATOS_AUX=datos_no_part_activ_hist
		fi
		TERMINAL="$(echo "$(sqlescape "${TERMINAL}")")"
		sqlplus -s $CONEXION_ORACLE > $TMP_LOG_FILE << EOF
			set serveroutput on; 
			DECLARE k_start_time CONSTANT TIMESTAMP := SYSTIMESTAMP;
			BEGIN dbms_output.put_line( 'Start time: ' || SYSTIMESTAMP); END;
			/
			set timing on;
			DECLARE COUNTER INT := 0; I INT :=0;
			begin loop delete from nivel_hist 
			where 
			k_particion = utils.get_num_particion('${TIPO_TERMINAL}','${TERMINAL}')
			and k_tipo_terminal = '${TIPO_TERMINAL}'
			and k_terminal = '${TERMINAL}'
			and k_tsinsercion >= to_timestamp('${FECHA}','yyyymmdd')
			and k_tsinsercion < to_timestamp('${FECHA}','yyyymmdd') + INTERVAL '1' DAY
			and rownum <= $MAX_ROWNUM; exit when SQL%rowcount = 0; I := I+1; COUNTER := COUNTER + SQL%ROWCOUNT; commit; end loop; DBMS_OUTPUT.put_line('*** Total rows deleted: '|| COUNTER || ' | Iterations: '|| I||' ***'); dbms_output.put_line( 'End time: ' || SYSTIMESTAMP); end;
			/
			set timing off;
			DECLARE k_start_time CONSTANT TIMESTAMP := SYSTIMESTAMP;
			BEGIN dbms_output.put_line( 'Start time: ' || SYSTIMESTAMP); END;
			/
			set timing on;
			DECLARE COUNTER INT := 0; I INT :=0;			
			begin loop delete from ${DATOS_AUX} 
			where 
			K_PARTICION = utils.get_num_particion('${TIPO_TERMINAL}','${TERMINAL}')
			and K_TIPO_TERMINAL = '${TIPO_TERMINAL}'
			and K_TERMINAL = '${TERMINAL}'
			and K_TSINSERCION >= to_timestamp('${FECHA}','yyyymmdd')
			and K_TSINSERCION < to_timestamp('${FECHA}','yyyymmdd')  + INTERVAL '1' DAY 
			and rownum <= $MAX_ROWNUM; exit when SQL%rowcount = 0; I := I+1; COUNTER := COUNTER + SQL%ROWCOUNT; commit; end loop; DBMS_OUTPUT.put_line('*** Total rows deleted: '|| COUNTER || ' | Iterations: '|| I||' ***'); dbms_output.put_line( 'End time: ' || SYSTIMESTAMP); end;
			/			
			set timing off;
			exit
EOF
		TERMINAL="$(echo "$(sqlunescape "${TERMINAL}")")"
		traza "Fin Borrando datos de datos TipoTerm ${TIPO_TERMINAL} Terminal ${TERMINAL} para el dia ${FECHA}"
		
		if [[ $(grep -c ORA- $TMP_LOG_FILE) -ne 0 || $(grep -c ERR $TMP_LOG_FILE) -ne 0 ]]; then
			traza "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
			trazafile $TMP_LOG_FILE
			cat $TMP_LOG_FILE > $TMP_LOG_FILE_DOS
			doMail "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
		fi			

		if [[ $APLICACION != 'CA' ]];then
			TERMINAL="$(echo "$(sqlescape "${TERMINAL}")")"
			sqlplus -s $CONEXION_ORACLE > $TMP_LOG_FILE << EOF
			set serveroutput on; 
			DECLARE k_start_time CONSTANT TIMESTAMP := SYSTIMESTAMP;
			BEGIN dbms_output.put_line( 'Start time: ' || SYSTIMESTAMP); END;
			/
			set timing on;
			DECLARE COUNTER INT := 0; I INT :=0;
			begin loop delete from datos_negocio_hist 
			where 
			k_particion = utils.get_num_particion('${TIPO_TERMINAL}','${TERMINAL}')
			and k_tipo_terminal = '${TIPO_TERMINAL}'
			and k_terminal = '${TERMINAL}'
			and k_tsinsercion >= to_timestamp('${FECHA}','yyyymmdd')
			and k_tsinsercion < to_timestamp('${FECHA}','yyyymmdd') + INTERVAL '1' DAY
			and rownum <= $MAX_ROWNUM; exit when SQL%rowcount = 0; I := I+1; COUNTER := COUNTER + SQL%ROWCOUNT; commit; end loop; DBMS_OUTPUT.put_line('*** Total rows deleted: '|| COUNTER || ' | Iterations: '|| I||' ***'); dbms_output.put_line( 'End time: ' || SYSTIMESTAMP); end;
			/
			set timing off; 
			exit
EOF
			TERMINAL="$(echo "$(sqlunescape "${TERMINAL}")")"
			traza "Fin Borrando datos de datos TipoTerm ${TIPO_TERMINAL} Terminal ${TERMINAL} para el dia ${FECHA}"
		
			if [[ $(grep -c ORA- $TMP_LOG_FILE) -ne 0 || $(grep -c ERR $TMP_LOG_FILE) -ne 0 ]]; then
				traza "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
				trazafile $TMP_LOG_FILE
				cat $TMP_LOG_FILE > $TMP_LOG_FILE_DOS
				doMail "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
			fi			
		fi
		
			#traza "Se modifican los registros segun visibilidad de ${GRAN_FICHERO_A_CARGAR}.ord en ${GRAN_FICHERO_A_CARGAR}.vis" 
			
			#REG_POS_CODE=13
			#REG_POS_TIPREG=6
			## Version anterior se ponen a negativo los tipos de las operaciones QDEPP200 y QDEPP300... esto deberia se configurable por properties 
			##cat $GRAN_FICHERO_A_CARGAR.ord | awk -F'|' -v code=${REG_POS_CODE} -v tipoReg=${REG_POS_TIPREG} '{ OFS="|"; if ( $code == "QDEPP200" || $code == "QDEPP300" ) { $tipoReg=-1*$tipoReg } print;  }' > $GRAN_FICHERO_A_CARGAR.vis
			##echo "Generado ${GRAN_FICHERO_A_CARGAR}.vis" 

			#CODIGOS=''
			#obtener_codigos
			
			##CODIGOS="/QDEPP200/,/QDEPP300/,/CA\.DEC\.diariCentral\.testView/"			
			
			#traza "Codigos a tratar (pasan a tipo negativo): "$CODIGOS
			
			# Se ponen a negativo los tipos de las operaciones obtenidas del fichero
			#cat $GRAN_FICHERO_A_CARGAR.ord | awk -F'|' -v code=${REG_POS_CODE} -v tipoReg=${REG_POS_TIPREG} 'BEGIN {OFS="|"} $code ~ '$CODIGOS' {$tipoReg=-1*$tipoReg; print;} $code !~ '$CODIGOS' {print;} END {}' > $GRAN_FICHERO_A_CARGAR.vis
			#cat $GRAN_FICHERO_A_CARGAR.ord > $GRAN_FICHERO_A_CARGAR.vis
			REGISTROS_TMP=$TMP_DIR/$NOMBRE_FICHERO"_TRATADOS"
			REGISTROS_A_TRATAR=$TMP_DIR/$NOMBRE_FICHERO"_A_TRATAR"
			###### FEM EL TRACTAMENT DEL VERSIONAT
			echo "Realizamos el tratamiento del versionado de los registros de importacion"
			###### FEM EL TRACTAMENT DEL VERSIONAT
			# Si no hi ha versió s'ha d'afegir 4 separadors (per les noves columnes)
			cat $GRAN_FICHERO_A_CARGAR > $REGISTROS_A_TRATAR
			cat $VERSIO_HIST | while read LINEA_VERSION
			do
				VERSION=$(echo $LINEA_VERSION | awk -F= '{print($1)}')
				echo "VESION: "$VERSION
				CAMPOS=$(echo $LINEA_VERSION | awk -F= '{print($2)}')
				echo "CAMPOS: "$CAMPOS
				
				# si no hi ha versió s'ha d'afegir els camps que falten
				# Fem servir ¬ com a separació del sed per què al registre pot ser que els caràcters/,:,; es facin servir
				#sed '$!s/$/<br>/g'
				cat $REGISTROS_A_TRATAR | grep -v $VERSION | sed "s¬$¬${CAMPOS}¬" >> $REGISTROS_TMP
				#si hi ha versió l'eliminem del registre
				#s/ /#.#/g
				cat $REGISTROS_A_TRATAR | grep $VERSION | sed "s¬${VERSION}|¬¬g" >> $REGISTROS_TMP 		
				rm $REGISTROS_A_TRATAR
				cat $REGISTROS_TMP > $REGISTROS_A_TRATAR
				rm $REGISTROS_TMP
			done
			
			traza "Se ordena el fichero y se elimina los duplicados de ${GRAN_FICHERO_A_CARGAR} en ${GRAN_FICHERO_A_CARGAR}.ord" 
			
			tac $REGISTROS_A_TRATAR | sort -k 1,9 -u -t '|' > $GRAN_FICHERO_A_CARGAR.ord
			#cat $REGISTROS_A_TRATAR | sort -u > $GRAN_FICHERO_A_CARGAR.ord
			traza "Generado ${GRAN_FICHERO_A_CARGAR}.ord"
			
#cat $GRAN_FICHERO_A_CARGAR.ord > $GRAN_FICHERO_A_CARGAR.vis

			traza "Generado ${GRAN_FICHERO_A_CARGAR}.vis" 
			
            traza "Se modifican los registros segun visibilidad de ${GRAN_FICHERO_A_CARGAR}.ord en ${GRAN_FICHERO_A_CARGAR}.vis" 
			
			REG_POS_CODE=22
			REG_POS_TIPREG=5
			REG_POS_DATOS=90
			# Version anterior se ponen a negativo los tipos de las operaciones QDEPP200 y QDEPP300... esto deberia se configurable por properties 
			#cat $GRAN_FICHERO_A_CARGAR.ord | awk -F'|' -v code=${REG_POS_CODE} -v tipoReg=${REG_POS_TIPREG} '{ OFS="|"; if ( $code == "QDEPP200" || $code == "QDEPP300" ) { $tipoReg=-1*$tipoReg } print;  }' > $GRAN_FICHERO_A_CARGAR.vis
			#echo "Generado ${GRAN_FICHERO_A_CARGAR}.vis" 

			CODIGOS=''
			obtener_codigos
			
			#CODIGOS="/QDEPP200/,/QDEPP300/,/CA\.DEC\.diariCentral\.testView/"			
			
			traza "Codigos a tratar (pasan a tipo negativo): "$CODIGOS
			
			FILE_TEMP=
			touch $FILE_TEMP
			
			if [[ $CODIGOS != '' ]]; then
				# Se ponen a negativo los tipos de las operaciones obtenidas del fichero
				cat $GRAN_FICHERO_A_CARGAR.ord | awk -F'|' -v code=${REG_POS_CODE} -v tipoReg=${REG_POS_TIPREG} 'BEGIN {OFS="|"} $code ~ '$CODIGOS' {$tipoReg=-1*$tipoReg; print;} $code !~ '$CODIGOS' {print;} END {}' > $GRAN_FICHERO_A_CARGAR.ord.aux 
			else
				mv $GRAN_FICHERO_A_CARGAR.ord $GRAN_FICHERO_A_CARGAR.ord.aux
			fi
			traza "Generado ${GRAN_FICHERO_A_CARGAR}.ord.aux"
				
			# Se ponen a negativo los tipos de las operaciones obtenidas que continen N_O_V_I_S en datos de negocio
			cat $GRAN_FICHERO_A_CARGAR.ord.aux | awk -F'|' -v datos=${REG_POS_DATOS} -v tipoReg=${REG_POS_TIPREG} 'BEGIN {OFS="|"} $datos ~ /.*N_O_V_I_S.*/ {$tipoReg=-1*$tipoReg; print;} $datos !~ /.*N_O_V_I_S.*/ {print;} END {}' > $GRAN_FICHERO_A_CARGAR.vis
			traza "Generado ${GRAN_FICHERO_A_CARGAR}.vis"

			
			### Version anterior se ponen a negativo los tipos de las operaciones QDEPP200 y QDEPP300... esto deberia se configurable por properties 
			NOM_AUX="ip"
			if [[ $APLICACION == 'CA' ]];then
				NOM_AUX="activ"
				REG_POS_ACTIV=14
				REG_POS_TIPO_ACTIV=15
				cat $GRAN_FICHERO_A_CARGAR.vis | awk -F'|' -v activ=${REG_POS_ACTIV} -v tipo=${REG_POS_TIPO_ACTIV} '{ if ( $activ != "" && $tipo != "" ) { print; } }' > $GRAN_FICHERO_A_CARGAR_IP.vis
				traza "Generado ${GRAN_FICHERO_A_CARGAR_IP}.vis"  
			else
				REG_POS_IP=16
				#cat $GRAN_FICHERO_A_CARGAR.ord | awk -F'|' -v ip=${REG_POS_IP} '{ print ($ip); }' > $GRAN_FICHERO_A_CARGAR_IP.ip
				#traza "Generado ${GRAN_FICHERO_A_CARGAR_IP}.ip"  
				#cat $GRAN_FICHERO_A_CARGAR.ord | awk -F'|' -v ip=${REG_POS_IP} '{ if ( $ip != "" ) { print ($ip); } }' > $GRAN_FICHERO_A_CARGAR_IP.ip2
				#traza "Generado ${GRAN_FICHERO_A_CARGAR_IP}.ip2"  
				cat $GRAN_FICHERO_A_CARGAR.vis | awk -F'|' -v ip=${REG_POS_IP} '{ if ( $ip != "" ) { print; } }' > $GRAN_FICHERO_A_CARGAR_IP.vis
				traza "Generado ${GRAN_FICHERO_A_CARGAR_IP}.vis"  
			fi
	# a partir del fichero de solicitudes pendientes con las claves desplegadas, obtiene los ficheros a recuperar:
	# mostrando solo semana, partición y nombre de fichero, ordenando por semana y partición, y estableciendo la
	# opción '-u' que no muestra registros duplicados			
			
			RESULTADO=0
			traza 'Tratamiento de Datos de Negocio: Invocación a cargarDatosNegocio.sh'

			
			$SH_DATOS_NEGOCIO $GRAN_FICHERO_A_CARGAR.vis $DEBUG
			RESULTADO=$?
			if [[ $RESULTADO -ne 0 ]]; then
				traza "- ERROR: Datos de Negocio procesados con resultado "$RESULTADO 
				traza "- ERROR: No se cargan los registros de  "$GRAN_FICHERO_A_CARGAR" en la tabla de históricos" 
			else 
				traza "- OK: Datos de Negocio procesados " 

			fi
			#RESULTADO=2
	
			# carga el contenido del fichero de datos a la tabla de históricos
			if [[ $RESULTADO -eq 0 ]]; then
				sqlldr userid=$CONEXION_ORACLE, control=$CTL_FILE, log=$LOG_GRAN_FICHERO, data=$GRAN_FICHERO_A_CARGAR.vis, bad=$BAD_GRAN_FICHERO, discard=$DIS_GRAN_FICHERO, rows=64, silent=FEEDBACK
				RESULTADO=$?
				traza "Cargando los datos de '"$GRAN_FICHERO_A_CARGAR".vis', el comando 'sqlldr' devolvió la respuesta ${RESULTADO}"				
				if [[ $RESULTADO -eq 2 ]]; then
						traza "WARNING: Al cargar el fichero de datos '"$GRAN_FICHERO_A_CARGAR".vis', el comando 'sqlldr' devolvió la respuesta ${RESULTADO}. Se han rechazado registros pero el proceso continua"
				else 
					if [[ $RESULTADO -ne 0 ]]; then
						traza "ERROR: Al cargar el fichero de datos '"$GRAN_FICHERO_A_CARGAR".vis', el comando 'sqlldr' devolvió la respuesta ${RESULTADO}."
						sendMailError "ERROR: Al cargar el fichero de datos '"$GRAN_FICHERO_A_CARGAR".vis'. Revisar ejecución."
						exit 161
					else
							traza "Fichero ${NOMBRE_FICHERO} : Se ha cargado correctamente datos de ${TIPO_TERMINAL} ${TERMINAL} en la fecha ${FECHA}"
							echo "${TIPO_TERMINAL}|"${TERMINAL}"|${FECHA}" >> $FICHERO_CLAVES_ACTUALES				

			# carga el contenido del fichero de datos a la tabla de ip históricos
			if [[ $RESULTADO -eq 0 ]]; then
				sqlldr userid=$CONEXION_ORACLE, control=$CTL_FILE_AUX, log=$LOG_GRAN_FICHERO_IP, data=$GRAN_FICHERO_A_CARGAR_IP.vis, bad=$BAD_GRAN_FICHERO_IP, discard=$DIS_GRAN_FICHERO_IP, rows=64, silent=FEEDBACK
				RESULTADO=$?
				traza "Cargando los datos de ${NOM_AUX} de '"$GRAN_FICHERO_A_CARGAR_IP".vis', el comando 'sqlldr' devolvió la respuesta ${RESULTADO}"				
				if [[ $RESULTADO -eq 2 ]]; then
						traza "WARNING: Al cargar el fichero de datos de ${NOM_AUX} de '"$GRAN_FICHERO_A_CARGAR_IP".vis', el comando 'sqlldr' devolvió la respuesta ${RESULTADO}. Se han rechazado registros pero el proceso continua"
				else 
					if [[ $RESULTADO -ne 0 ]]; then
						traza "ERROR: Al cargar el fichero de datos de ${NOM_AUX} de '"$GRAN_FICHERO_A_CARGAR_IP".vis', el comando 'sqlldr' devolvió la respuesta ${RESULTADO}."
						sendMailError "ERROR: Al cargar el fichero de datos '"$GRAN_FICHERO_A_CARGAR".vis'. Revisar ejecución."
						exit 161
					else
							traza "Fichero ${NOMBRE_FICHERO} : Se ha cargado correctamente datos de ${TIPO_TERMINAL} ${TERMINAL} en la fecha ${FECHA}"
							echo "${TIPO_TERMINAL}|"${TERMINAL}"|${FECHA}" >> $FICHERO_CLAVES_ACTUALES				
					fi
				fi
			fi
							
					fi
				fi
			fi

			
			
			if [[ $RESULTADO -eq 0 ]]; then
				# los datos para soporte Pantalla de la recuperación en curso ya están cargados,
				# elimina los ficheros relacionados con la carga recién hecha
				borra $GRAN_FICHERO_A_CARGAR.vis #DEBUG
				borra $GRAN_FICHERO_A_CARGAR_IP.vis #DEBUG				
				borra $GRAN_FICHERO_A_CARGAR.ord.aux #DEBUG
				borra $GRAN_FICHERO_A_CARGAR #DEBUG
				borra $LOG_GRAN_FICHERO
				borra $BAD_GRAN_FICHERO
				borra $DIS_GRAN_FICHERO #DEBUG
				borra $LOG_GRAN_FICHERO_IP
				borra $BAD_GRAN_FICHERO_IP
				borra $DIS_GRAN_FICHERO_IP #DEBUG
				borra $REGISTOR_TRATADOS
			else
				if [[ $RESULTADO -eq 2 ]]; then
					resum "Se han detectado warnings al tratar ${NOMBRE_FICHERO} tratando los datos de ${TIPO_TERMINAL} Terminal ${TERMINAL} en la fecha ${FECHA} : No se ha cargado todos los datos "
					doMail "Se han detectado warnings al tratar ${NOMBRE_FICHERO} tratando los datos de ${TIPO_TERMINAL} Terminal ${TERMINAL} en la fecha ${FECHA} : No se ha cargado todos los datos "
				else
					inc "Problemas al tratar ${NOMBRE_FICHERO} : No se ha cargado correctamente datos de ${TIPO_TERMINAL} Terminal ${TERMINAL} en la fecha ${FECHA}"
					doMail "Problemas al tratar ${NOMBRE_FICHERO} : No se ha cargado correctamente datos de ${TIPO_TERMINAL} Terminal ${TERMINAL} en la fecha ${FECHA}"
				fi
			fi
			
			done
			
		fi

fi		

#----------------------------------------------------------------------------------------------------------------------
# Realiza la inserción de las claves activadas a partir de la carga de registros
#----------------------------------------------------------------------------------------------------------------------
traza "paso 9: Inserción de la claves tratadas ${FICHERO_CLAVES_ACTUALES}"
traza "doPaso9?: ${doPaso9}"

if [ $doPaso9 -eq 1 ]; then
			
#export NLS_LANG=SPANISH_SPAIN.AL32UTF8;
#export LANG=es_ES.UTF-8; 

			# resigue, de nuevo, cada uno de los ficheros correspondientes a dicha recuperación...
			cat $FICHERO_CLAVES_ACTUALES | while read LINEA_CLAVES
			do
					# obtiene los campos soporte, oficina y fecha
					traza "CLAVE VOLCADA: "$LINEA_CLAVES
					
					TIPO_TERMINAL=$(echo $LINEA_CLAVES | awk -F'|' '{print $1}')
					TERMINAL="$(echo $LINEA_CLAVES | awk -F'|' '{print $2}')"					
					FECHA=$(echo $LINEA_CLAVES | awk -F'|' '{print $3}')
					TERMINAL="$(echo "$(grepescape "${TERMINAL}")")"

				# resigue, para cada fichero cargado, las claves de soporte Pantalla...
				grep "^.*|.*|${TIPO_TERMINAL}|${TERMINAL}|.*|.*|${FECHA}|.*" $CLAVES_PENDIENTES | awk -F'|' '{print $1 "|" $3 "|" $4 "|" $8}' | while read CLAVE
				do
					# obtiene los campos id, oficina y fecha
					traza "CLAVE A ACTIVAR: "$CLAVE
					ID_SOLICITUD=$(echo $CLAVE | awk -F'|' '{print $1}')
					TIPO_TERMINAL=$(echo $CLAVE | awk -F'|' '{print $2}')
					TERMINAL="$(echo $CLAVE | awk -F'|' '{print $3}')"
					FECHA=$(echo $CLAVE | awk -F'|' '{print $4}')
					TERMINAL="$(echo "$(sqlescape "${TERMINAL}")")"
					# inserta, para cada clave cargada de cada solicitud, el registro correspondiente en la tabla de claves
					sqlplus -s $CONEXION_ORACLE > $TMP_LOG_FILE << EOF 
						-- ALTER SESSION SET NLS_LANGUAGE= 'SPANISH' LANG='es_ES.UTF-8';
--						alter session set NLS_LANGUAGE='SPANISH';
--						alter session set NLS_TERRITORY='SPAIN';
						insert into solicitudes_hist_claves (id_solicitud_hist, tipo_terminal, terminal, fecha)
						values (${ID_SOLICITUD}, '${TIPO_TERMINAL}', '${TERMINAL}' , to_date('${FECHA}', 'yyyymmdd'));
						commit;
						exit
EOF
					TERMINAL="$(echo "$(sqlunescape "${TERMINAL}")")"
					#sed '/^$/d' $TMP_LOG_FILE # igual que un 'cat' pero sin líneas en blanco
					trazafile $TMP_LOG_FILE
					if [[ $(grep -c ORA- $TMP_LOG_FILE) -ne 0 || $(grep -c ERR $TMP_LOG_FILE) -ne 0 ]]; then
						traza "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
						#exit 162
					fi
					rm $TMP_LOG_FILE
				done
			done
fi			

#----------------------------------------------------------------------------------------------------------------------
# Modifica el estado de las peticiones, si todas las claves necesarias se encuentran disponibles
#----------------------------------------------------------------------------------------------------------------------
traza 'paso 10: Modificacion de estado para peticiones de oficina' 
traza "doPaso10?: ${doPaso10}"

if [ $doPaso10 -eq 1 ]; then
			

	# todas las solicitudes para soporte Pantalla de Oficina deberían haber quedado satisfechas,
	# comprueba, solicitud a solicitud (de soporte Pantalla) que sus claves estén de
	# alta en la tabla de claves; en caso afirmativo, deja la solicitud a estado Disponible
	grep "^.*|.*|.*|.*|.*|P|O|" $FICHERO_SOLICITUDES_PENDIENTES | awk -F'|' '{print $1}' | while read ID_SOLICITUD
	do
		sqlplus -s $CONEXION_ORACLE > $TMP_LOG_FILE << EOF
			set serveroutput on;
			declare
				l_num_claves number;
				l_num_dias number;
			begin
				select count(*) into l_num_claves
				from solicitudes_hist s
					inner join solicitudes_hist_claves c
						on (s.id_solicitud_hist = c.id_solicitud_hist)
				where s.id_solicitud_hist = ${ID_SOLICITUD}
					and (c.fecha between s.crit_fecha_ini and s.crit_fecha_fin);
				select crit_fecha_fin - crit_fecha_ini + 1 into l_num_dias
				from solicitudes_hist where id_solicitud_hist = ${ID_SOLICITUD};				
				dbms_output.put_line('Solicitud:'|| ${ID_SOLICITUD});
				dbms_output.put_line('Dias existentes:'|| l_num_claves);
				dbms_output.put_line('Dias necesarios:'|| l_num_dias);				
				if (l_num_claves = l_num_dias) then
					update solicitudes_hist 
						set estado = 'D' , 
							  f_disponible = to_timestamp('${FECHA_ACTIVACION_OFICINA}','yyyymmdd'), 		
							  f_caducidad = to_timestamp('${FECHA_CADUCIDAD_OFICINA}','yyyymmdd'), 						
							  f_eliminacion = to_timestamp('${FECHA_DEL_CADUCAS_OFICINA}','yyyymmdd') 						
								
					where id_solicitud_hist = ${ID_SOLICITUD} and f_eliminacion is null;
					dbms_output.put_line('ACTIVADA| Peticion disponible:'|| ${ID_SOLICITUD});			
					commit;
				else
					dbms_output.put_line('solicitud incompleta');
				end if;
				
			end;
			/
			exit
EOF
		
		# esto no debería pasar nunca !!!
		if [[ $(grep -c "solicitud incompleta" $TMP_LOG_FILE) -ne 0 ]]; then
			traza "ADVERTENCIA: La solicitud con id '"$ID_SOLICITUD"' no está completa y ha quedado en estado Pendiente"
			rm -f $TMP_LOG_FILE_DOS
			cat $TMP_LOG_FILE > $TMP_LOG_FILE_DOS
			marcarIncidente $ID_SOLICITUD INC
		fi

		#sed '/^$/d' $TMP_LOG_FILE # igual que un 'cat' pero sin líneas en blanco
		trazafile $TMP_LOG_FILE
		if [[ $(grep -c ORA- $TMP_LOG_FILE) -ne 0 || $(grep -c ERR $TMP_LOG_FILE) -ne 0 ]]; then
			traza "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
			echo -e "ERROR: Se ha producido algún error durante la ejecución de sqlplus." > $TMP_LOG_FILE_DOS
			cat $TMP_LOG_FILE >> $TMP_LOG_FILE_DOS
			marcarIncidente $ID_SOLICITUD SQL
			rm -f $TMP_LOG_FILE_DOS
			exit 163
		fi

		grep "ACTIVADA|" $TMP_LOG_FILE >> $FICHERO_CLAVES_ACTIVADAS
		
		rm $TMP_LOG_FILE
	done

fi	

#----------------------------------------------------------------------------------------------------------------------
# Modifica el estado de las peticiones, si todas las claves necesarias se encuentran disponibles
#----------------------------------------------------------------------------------------------------------------------
traza 'paso 11: Modificacion de estado para peticiones de auditoria' 
traza "doPaso11?: ${doPaso11}"

if [ $doPaso11 -eq 1 ]; then
			

	# todas las solicitudes para soporte Pantalla de Auditoría deberían haber quedado satisfechas,
	# comprueba, solicitud a solicitud (de soporte Pantalla) que sus claves estén de
	# alta en la tabla de claves; en caso afirmativo, deja la solicitud a estado Disponible
	grep "^.*|.*|.*|.*|.*|P|A|" $FICHERO_SOLICITUDES_PENDIENTES | awk -F'|' '{print $1}' | while read ID_SOLICITUD
	do
		traza "entrando en el bucle $NUM_AUDIT_OK	:"$NUM_AUDIT_OK	
		
		sqlplus -s $CONEXION_ORACLE > $TMP_LOG_FILE << EOF
			set serveroutput on;
			declare
				l_num_claves number;
				l_num_dias number;
			begin
				select count(*) into l_num_claves
				from solicitudes_hist s
					inner join solicitudes_hist_claves c
						on (s.id_solicitud_hist = c.id_solicitud_hist)
				where s.id_solicitud_hist = ${ID_SOLICITUD}
					and (c.fecha between s.crit_fecha_ini and s.crit_fecha_fin);
				select crit_fecha_fin - crit_fecha_ini + 1 into l_num_dias
				from solicitudes_hist where id_solicitud_hist = ${ID_SOLICITUD};				
				dbms_output.put_line('Solicitud:'|| ${ID_SOLICITUD});
				dbms_output.put_line('Dias existentes:'|| l_num_claves);
				dbms_output.put_line('Dias necesarios:'|| l_num_dias);			
				if (l_num_claves = l_num_dias) then
					update solicitudes_hist 
						set estado = 'D' , 
							  f_disponible = to_timestamp('${FECHA_ACTIVACION_AUDITORIA}','yyyymmdd'), 		
							  f_caducidad = to_timestamp('${FECHA_CADUCIDAD_AUDITORIA}','yyyymmdd'), 						
							  f_eliminacion = to_timestamp('${FECHA_DEL_CADUCAS_AUDITORIA}','yyyymmdd') 						
								
					where id_solicitud_hist = ${ID_SOLICITUD} and f_eliminacion is null;
					dbms_output.put_line('ACTIVADA| Peticion disponible:'|| ${ID_SOLICITUD});			
					commit;
				else
					dbms_output.put_line('solicitud incompleta');
				end if;
			end;
			/
			exit
EOF

		# esto no debería pasar nunca !!!
		if [[ $(grep -c "solicitud incompleta" $TMP_LOG_FILE) -ne 0 ]]; then
			traza "ADVERTENCIA: La solicitud con id '"$ID_SOLICITUD"' no está completa y ha quedado en estado Pendiente"
			echo -e "ADVERTENCIA: La solicitud con id '"$ID_SOLICITUD"' no está completa y ha quedado en estado Pendiente" > $TMP_LOG_FILE_DOS
			cat $TMP_LOG_FILE >> $TMP_LOG_FILE_DOS
			marcarIncidente $ID_SOLICITUD INC
			rm -f $TMP_LOG_FILE_DOS
		fi

		#sed '/^$/d' $TMP_LOG_FILE # igual que un 'cat' pero sin líneas en blanco
		trazafile $TMP_LOG_FILE
		if [[ $(grep -c ORA- $TMP_LOG_FILE) -ne 0 || $(grep -c ERR $TMP_LOG_FILE) -ne 0 ]]; then
			traza "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
			echo -e "ERROR: Se ha producido algún error durante la ejecución de sqlplus." > $TMP_LOG_FILE_DOS
			cat $TMP_LOG_FILE >> $TMP_LOG_FILE_DOS
			marcarIncidente $ID_SOLICITUD SQL
			rm -f $TMP_LOG_FILE_DOS
			exit 163
		fi
		
		grep "ACTIVADA|" $TMP_LOG_FILE >> $FICHERO_CLAVES_ACTIVADAS
		
		rm $TMP_LOG_FILE
		
	done
	
fi


NUM_TRAT=$(cat $FICHERO_CLAVES_ACTIVADAS | wc -l) 
TOTAL_TRATADAS=`expr $TOTAL_TRATADAS  + $(echo $NUM_TRAT)`

traza "Actualizaciones de peticiones tratadas:"$NUM_TRAT
resum "Se han activado las siguientes peticiones:"$NUM_TRAT

CONTINUAR=0
if [ $NUM_TRAT -gt 0 ]; then
		traza "Se han actualizado ${NUM_TRAT} peticiones. Continuamos en el bucle"
		resum "Se han tratado peticiones. Sigue el proceso"
		CONTINUAR=1
else 
		traza "No se han actualizado peticiones. Se sale del bucle"
		resum "No se han tratado peticiones. Termina el proceso"
fi

#----------------------------------------------------------------------------------------------------------------------
# Continuacion del bucle principal
#----------------------------------------------------------------------------------------------------------------------
traza "Parte final del bucle. Se sale del bucle"

HORA_ANTERIOR=$HORA
HORA_ACTUAL=$(date +"%H%M%S")
HORA=`expr $(echo $HORA_ACTUAL ) + 0` 
#HORA=$(echo $HORA_ACTUAL | awk -F: '{print($1)}')
traza "HORA:"$HORA
traza "HORA_ANTERIOR:"$HORA_ANTERIOR
traza "LIMITE HORARIO:"$LIMITE_HORA

if [[ $HORA -gt $LIMITE_HORA ]]; then
	traza "Saliendo del bucle de tratamiento por Hora superior a lo espeperado. HORA: "$HORA 
	CONTINUAR=0
else
	if [[ $HORA -lt $HORA_ANTERIOR ]]; then
		traza "Saliendo del bucle de tratamiento por Hora superior a lo espeperado (cambio dia). HORA: ${HORA} HORA ANTERIOR:{HORA_ANTERIOR}"  
		CONTINUAR=0
	fi
fi

#CONTINUAR=0
traza "Variable bucle CONTINUAR: "$CONTINUAR
if [[ $CONTINUAR != 1 ]]; then
			traza "Saliendo del bucle de tratamiento de peticiones"

fi
done

traza "Finalizado el bucle."
traza "Peticiones históricas activadas (TOTAL): "$TOTAL_TRATADAS
resum "Finalizado el bucle."
resum "Peticiones históricas activadas (TOTAL): "$TOTAL_TRATADAS


#----------------------------------------------------------------------------------------------------------------------
# Fin del bucle principal
#----------------------------------------------------------------------------------------------------------------------

#----------------------------------------------------------------------------------------------------------------------
# paso 12: Modificacion estado peticiones multiples oficina
#----------------------------------------------------------------------------------------------------------------------
traza 'paso 12: Modificacion estado peticiones multiples oficina'

traza "doPaso12?: ${doPaso12} $(date +"%d/%m/%Y %H:%M:%S")"

if [ $doPaso12 -eq 1 ]; then

			
# llamada a la función pl/sql 'solicitud_hist.INSERT_SOLICITUDES_HIST', que inserta las solicitudes multiples

sqlplus -s $CONEXION_ORACLE > $TMP_LOG_FILE << EOF
	set serveroutput on;
DECLARE
  l_num_updated NUMBER;
BEGIN
select count(*) into l_num_updated
from solicitudes_hist 
where 
id_solicitud_hist in
(
  select id_sol_padre from
  (
    select id_sol_padre,
    sum(solicitudes) num_solicitudes,
    sum(disponible) num_disponibles
    from
    ( 
      select 
      1 as solicitudes,
      decode(estado, 'D',1,0) disponible, id_sol_padre
      from solicitudes_hist
      where
      id_sol_padre is not null
    )
    group by id_sol_padre
  ) hijos
  where
    num_solicitudes = num_disponibles
)
and (estado = 'P' or estado = 'I')
and tipo = 'O'
and soporte = 'P';

DBMS_OUTPUT.PUT_LINE('Peticion oficina multiples a cambiar = ' || l_num_updated);

if (l_num_updated > 0) then
	DBMS_OUTPUT.PUT_LINE('Modificadas las peticiones');

update solicitudes_hist 
set estado = 'D' , 
f_disponible = to_timestamp('${FECHA_ACTIVACION_OFICINA}','yyyymmdd'), 		
f_caducidad = to_timestamp('${FECHA_CADUCIDAD_OFICINA}','yyyymmdd'), 						
f_eliminacion = to_timestamp('${FECHA_DEL_CADUCAS_OFICINA}','yyyymmdd') 						
where 
id_solicitud_hist in
(
  select id_sol_padre from
  (
    select id_sol_padre,
    sum(solicitudes) num_solicitudes,
    sum(disponible) num_disponibles
    from
    ( 
      select 
      1 as solicitudes,
      decode(estado, 'D',1,0) disponible, id_sol_padre
      from solicitudes_hist
      where
      id_sol_padre is not null
    )
    group by id_sol_padre
  ) hijos
  where
    num_solicitudes = num_disponibles
)
and (estado = 'P' or estado = 'I')
and tipo = 'O'
and soporte = 'P';

commit;
end if;
	
END;
	/
	exit
EOF

	trazafile $TMP_LOG_FILE
	# igual que un 'cat' pero sin lineas en blanco
	if [[ $(grep -c ORA- $TMP_LOG_FILE) -ne 0 || $(grep -c ERR $TMP_LOG_FILE) -ne 0 ]]; then
		traza "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
		echo -e "ERROR: Se ha producido algún error durante la ejecución de sqlplus." > $TMP_LOG_FILE_LOG_DOS
		cat $TMP_LOG_FILE >> $TMP_LOG_FILE_DOS
		sendMail
		exit 151
	fi
	rm $TMP_LOG_FILE			
			
fi	


	#----------------------------------------------------------------------------------------------------------------------
	# paso 13: Modificacion estado peticiones multiples auditoria
	#----------------------------------------------------------------------------------------------------------------------
traza 'paso 13: Modificacion estado peticiones multiples auditoria'

traza "doPaso13?: ${doPaso13} $(date +"%d/%m/%Y %H:%M:%S")"

if [ $doPaso13 -eq 1 ]; then

			
# llamada a la función pl/sql 'solicitud_hist.INSERT_SOLICITUDES_HIST', que inserta las solicitudes multiples

sqlplus -s $CONEXION_ORACLE > $TMP_LOG_FILE << EOF
	set serveroutput on;
DECLARE
  l_num_updated NUMBER;
BEGIN
select count(*) into l_num_updated
from solicitudes_hist 
where 
id_solicitud_hist in
(
  select id_sol_padre from
  (
    select id_sol_padre,
    sum(solicitudes) num_solicitudes,
    sum(disponible) num_disponibles
    from
    ( 
      select 
      1 as solicitudes,
      decode(estado, 'D',1,0) disponible, id_sol_padre
      from solicitudes_hist
      where
      id_sol_padre is not null
    )
    group by id_sol_padre
  ) hijos
  where
    num_solicitudes = num_disponibles
)
and (estado = 'P' or estado = 'I')
and tipo = 'A'
and soporte = 'P';

DBMS_OUTPUT.PUT_LINE('Peticion oficina multiples a cambiar = ' || l_num_updated);

if (l_num_updated > 0) then
	DBMS_OUTPUT.PUT_LINE('Modificadas las peticiones');

update solicitudes_hist 
set estado = 'D' , 
f_disponible = to_timestamp('${FECHA_ACTIVACION_AUDITORIA}','yyyymmdd'), 		
f_caducidad = to_timestamp('${FECHA_CADUCIDAD_AUDITORIA}','yyyymmdd'), 						
f_eliminacion = to_timestamp('${FECHA_DEL_CADUCAS_AUDITORIA}','yyyymmdd') 						
where 
id_solicitud_hist in
(
  select id_sol_padre from
  (
    select id_sol_padre,
    sum(solicitudes) num_solicitudes,
    sum(disponible) num_disponibles
    from
    ( 
      select 
      1 as solicitudes,
      decode(estado, 'D',1,0) disponible, id_sol_padre
      from solicitudes_hist
      where
      id_sol_padre is not null
    )
    group by id_sol_padre
  ) hijos
  where
    num_solicitudes = num_disponibles
)
and (estado = 'P' or estado = 'I')
and tipo = 'A'
and soporte = 'P';

commit;
end if;
	
END;
	/
	exit
EOF
		#sed 's/^/HOLA/' inputline
		
	#sed '/^$/d' $TMP_LOG_FILE
	trazafile $TMP_LOG_FILE
	# igual que un 'cat' pero sin lineas en blanco
	if [[ $(grep -c ORA- $TMP_LOG_FILE) -ne 0 || $(grep -c ERR $TMP_LOG_FILE) -ne 0 ]]; then
		traza "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
		echo -e "ERROR: Se ha producido algún error durante la ejecución de sqlplus." > $TMP_LOG_FILE_LOG_DOS
		cat $TMP_LOG_FILE >> $TMP_LOG_FILE_DOS
		sendMail
		exit 151
	fi
	rm $TMP_LOG_FILE			
			
fi	

#----------------------------------------------------------------------------------------------------------------------
# Rehabilitación de los índices de la tabla de históricos
#----------------------------------------------------------------------------------------------------------------------
traza 'paso 14: Habilitar los índices de la tabla de históricos'
traza "doPaso14?: ${doPaso14}"
traza "doRebuildIndex?: ${doRebuildIndex}"
if [ $doPaso14 -eq 1 ]; then

	if [ $doRebuildIndex -eq 1 ]; then

		sqlplus -s $CONEXION_INDICES_ORACLE > $TMP_LOG_FILE<< EOF
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_1;
		alter index pk_nivel_hist rebuild partition NIVEL_HIST_2;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_3;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_4;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_5;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_6;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_7;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_8;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_9;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_10;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_11;
		alter index pk_nivel_hist rebuild partition NIVEL_HIST_12;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_13;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_14;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_15;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_16;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_17;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_18;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_19;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_20;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_21;
		alter index pk_nivel_hist rebuild partition NIVEL_HIST_22;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_23;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_24;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_25;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_26;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_27;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_28;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_29;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_30;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_31;
		alter index pk_nivel_hist rebuild partition NIVEL_HIST_32;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_33;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_34;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_35;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_36;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_37;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_38;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_39;
			alter index pk_nivel_hist rebuild partition NIVEL_HIST_40;
			
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_1;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_2;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_3;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_4;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_5;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_6;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_7;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_8;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_9;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_10;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_11;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_12;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_13;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_14;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_15;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_16;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_17;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_18;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_19;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_20;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_21;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_22;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_23;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_24;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_25;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_26;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_27;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_28;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_29;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_30;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_31;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_32;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_33;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_34;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_35;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_36;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_37;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_38;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_39;
			alter index idx_nivel_padre_hist rebuild partition NIVEL_HIST_40;	
			
			exit
EOF
#-	analyze table nivel_hist compute statistics for table for all indexes;

#el indice antes era idx_nivel_hist_nif . Falla, no se si es un problema de grants ya que el index es del Idecloi1
	#sed '/^$/d' $TMP_LOG_FILE # igual que un 'cat' pero sin líneas en blanco
		trazafile $TMP_LOG_FILE
		if [[ $(grep -c ORA- $TMP_LOG_FILE) -ne 0 || $(grep -c ERR $TMP_LOG_FILE) -ne 0 ]]; then
			traza "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
			echo -e "ERROR: Se ha producido algún error durante la ejecución de sqlplus." > $TMP_LOG_FILE_LOG_DOS
			cat $TMP_LOG_FILE >> $TMP_LOG_FILE_DOS
			sendMail
			exit 164
		fi
		rm $TMP_LOG_FILE
		
		NUMDIA_INDICE="HIST"
			
		INDICES=$(grep INDICESHIST ${CONFIG_FILE} | awk -F= '{print($2)}')
		echo "Indices a tratar: ${INDICES}"

		count=`echo $INDICES | awk -F, {'print NF'}`

		i=1
		while [ $i -le $count ]
		do
			str[$i]=`echo $INDICES | cut -d, -f${i}`
			NOM_INDEX=${str[$i]}
			echo "Creando índice ${NOM_INDEX} para el dia ${NUMDIA_INDICE}"	
			if [[ $NOM_INDEX == "AXN" ]];then
				$SH_TRATAR_INDICES $NOM_INDEX $NUMDIA_INDICE N R S S
				RESULTADO=$?
				if [[ $RESULTADO -ne 0 ]]; then
					traza $(date +"%d/%m/%Y %H:%M:%S")" - ERROR al crear indice ${NOM_INDEX}: "$RESULTADO >> $LOG_FILE
					exit $RESULTADO
				fi
			else
				$SH_TRATAR_INDICES $NOM_INDEX $NUMDIA_INDICE N R N S
				RESULTADO=$?
				if [[ $RESULTADO -ne 0 ]]; then
					traza $(date +"%d/%m/%Y %H:%M:%S")" - ERROR al crear indice ${NOM_INDEX}: "$RESULTADO >> $LOG_FILE
					exit $RESULTADO
				fi
			fi
			i=`expr $i + 1`
		done		

	fi	
	
fi	
	

	
#----------------------------------------------------------------------------------------------------------------------
# Generación de ficheros csv
#----------------------------------------------------------------------------------------------------------------------
traza 'paso 15: Genera los ficheros csv'
traza "doPaso15?: ${doPaso15}"

if [ $doPaso15 -eq 1 ]; then
	
# Reestablim el lang original per a tractar els fitxers volcats a csv perquè amb el que posem per al procés d'importació s'envia el correu amb 'xinos' i el cos arriba annexat
#export LANG=${ORI_LANG}

# nombre del fichero con la lista de solicitudes fichero pendientes (sin ruta, Oracle siempre lo creará en el directorio temporal)
FICHERO_SOLICITUDES_FICHERO=solicitudes_fichero.$CLAVEFICHERO.tmp

# llamada a la función pl/sql 'solicitud_hist.get_fichero_pendientesFich', que crea un fichero con las solicitudes pendientes de soporte fichero
# en formato de texto separado por '|'
sqlplus -s $CONEXION_ORACLE > $TMP_LOG_FILE << EOF
	set serveroutput on;
	declare
		l_num_solicitudes_pendientes number;
	begin
		l_num_solicitudes_pendientes := solicitud_hist.get_fichero_pendientesFich('${FICHERO_SOLICITUDES_FICHERO}');
	end;
	/
	exit
EOF
#sed '/^$/d' $TMP_LOG_FILE # igual que un 'cat' pero sin líneas en blanco
	trazafile $TMP_LOG_FILE
	if [[ $(grep -c ORA- $TMP_LOG_FILE) -ne 0 || $(grep -c ERR $TMP_LOG_FILE) -ne 0 ]]; then
		traza "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
		cat $TMP_LOG_FILE >> $TMP_LOG_FILE_DOS
		doMail "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
		sendMail
		exit 154
	fi
	rm $TMP_LOG_FILE

# añade la ruta al fichero de solicitudes pendientes
FICHERO_SOLICITUDES_FICHEROaux=$TMP_DIR_PREF/$FICHERO_SOLICITUDES_FICHERO
FICHERO_SOLICITUDES_FICHERO=$TMP_DIR/$FICHERO_SOLICITUDES_FICHERO

	if [[ $SERVER_ACTUAL != $SERVER_REMOTO ]]; then

		rm -f $FICHERO_SOLICITUDES_FICHERO
		obtener_fichero $FICHERO_SOLICITUDES_FICHEROaux $TMP_DIR	
	
	fi


	# comprueba el fichero de solicitudes pendientes
	if [[ ! -f $FICHERO_SOLICITUDES_FICHERO ]]; then
		traza "ERROR: No se ha creado el fichero '"$FICHERO_SOLICITUDES_FICHERO"' con las solicitudes fichero pendientes."
		sendMailError "ERROR: No se ha creado el fichero '"$FICHERO_SOLICITUDES_FICHERO"' con las solicitudes fichero pendientes."
		exit 155
	fi
	
		
# nombre del fichero con la lista de solicitudes fichero pendientes (sin ruta, Oracle siempre lo creará en el directorio temporal)
FICHERO_SOLICITUDES_FICHERO_INC=solicitudes_fichero_inc.$CLAVEFICHERO.tmp



# llamada a la función pl/sql 'solicitud_hist.get_fichero_pendientesFich', que crea un fichero con las solicitudes pendientes de soporte fichero
# en formato de texto separado por '|'
sqlplus -s $CONEXION_ORACLE > $TMP_LOG_FILE << EOF
	set serveroutput on;
	declare
		l_num_solicitudes_incidentes number;
	begin
		l_num_solicitudes_incidentes := solicitud_hist.get_fichero_incidenciaFich('${FICHERO_SOLICITUDES_FICHERO_INC}');
	end;
	/
	exit
EOF
#sed '/^$/d' $TMP_LOG_FILE # igual que un 'cat' pero sin líneas en blanco
	trazafile $TMP_LOG_FILE
	if [[ $(grep -c ORA- $TMP_LOG_FILE) -ne 0 || $(grep -c ERR $TMP_LOG_FILE) -ne 0 ]]; then
		traza "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
		cat $TMP_LOG_FILE >> $TMP_LOG_FILE_DOS
		doMail "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
		sendMail
		exit 154
	fi
	rm $TMP_LOG_FILE

# añade la ruta al fichero de solicitudes pendientes
FICHERO_SOLICITUDES_FICHERO_INCaux=$TMP_DIR_PREF/$FICHERO_SOLICITUDES_FICHERO_INC
FICHERO_SOLICITUDES_FICHERO_INC=$TMP_DIR/$FICHERO_SOLICITUDES_FICHERO_INC

	if [[ $SERVER_ACTUAL != $SERVER_REMOTO ]]; then

		rm -f $FICHERO_SOLICITUDES_FICHERO_INC
		obtener_fichero $FICHERO_SOLICITUDES_FICHERO_INCaux $TMP_DIR	
	
	fi


	# comprueba el fichero de solicitudes pendientes
	if [[ ! -f $FICHERO_SOLICITUDES_FICHERO_INC ]]; then
		traza "ERROR: No se ha creado el fichero '"$FICHERO_SOLICITUDES_FICHERO_INC"' con las solicitudes fichero incidentes."
		sendMailError "ERROR: No se ha creado el fichero '"$FICHERO_SOLICITUDES_FICHERO_INC"' con las solicitudes fichero incidentes."
		exit 155
	else
		cat $FICHERO_SOLICITUDES_FICHERO_INC >> $FICHERO_SOLICITUDES_FICHERO
	fi
	
	rm -f $FICHERO_SOLICITUDES_FICHERO_INC
	
	# comprueba el fichero con la lista de ficheros que deben eliminarse
	if [[ ! -f $FICHERO_SOLICITUDES_FICHERO ]]; then
		traza "No hay fichero de peticiones, no se generan ficheros CSV."
	else

		traza 'Tratamiento de ficheros: Todas las peticiones de ficheros deberian haberse realizado online'
		# filtra las solicitudes para soporte Fichero, muestra solo los campos oficina, fecha inicial y fecha final, les aplica
		# un 'uniq' para eliminar duplicados, y resigue su contenido...
		NUM_PETICIONES=$(grep "^.*|.*|.*|.*|.*|.*|F" $FICHERO_SOLICITUDES_FICHERO | wc -l )
		traza "Hay ${NUM_PETICIONES} peticiones de fichero pendientes de tratar"
		grep "^.*|.*|.*|.*|.*|.*|F" $FICHERO_SOLICITUDES_FICHERO | while read SOLICITUD_CSV
		do

			SOLICITUD_CSV=$(echo $SOLICITUD_CSV | sed 's/ /#.#/g')
			SOLICITUD_CSV=$(echo $SOLICITUD_CSV | sed 's/=/%3D/g')
			SOLICITUD_CSV=$(echo $SOLICITUD_CSV | sed 's/&/%26/g')
			
		
      traza "- Se procesa la siguiente petición_______________________"
			traza $SOLICITUD_CSV

			$SH_PROCESAR_FICHERO $SOLICITUD_CSV $DEBUG
			RESULTADO=$?
			if [[ $RESULTADO -ne 0 ]]; then
				traza "- ERROR: Petición no procesada con resultado "$RESULTADO
				doMail "- ERROR: Petición no procesada con resultado "$RESULTADO "\n${SOLICITUD_CSV}"
				SOL_INC=$(echo $SOLICITUD_CSV | awk -F'|' -v pos=${POS_CAMPO_ID} '{print $pos}')
				marcarIncidente $SOL_INC REC
			else 
				traza "- OK: Petición procesada " 
			fi

			traza "_________________________________________________________________________________"
			
		done
		
		traza 'Tratamiento de grandes ficheros históricos  (B): Se trantan todas las peticiones de ficheros de información histórica'
		# filtra las solicitudes para soporte Fichero, muestra solo los campos oficina, fecha inicial y fecha final, les aplica
		# un 'uniq' para eliminar duplicados, y resigue su contenido...
		NUM_PETICIONES=$(grep "^.*|.*|.*|.*|.*|.*|B" $FICHERO_SOLICITUDES_FICHERO | wc -l )
		traza "Hay ${NUM_PETICIONES} peticiones de ficheros históricos pendientes de tratar"
		
		PET_B=$TMP_DIR/pet_b.$CLAVEFICHERO.tmp
		cat $FICHERO_SOLICITUDES_FICHERO | grep "^.*|.*|.*|.*|.*|.*|B" > $PET_B
		
		cat $PET_B | while read SOLICITUD_CSV
		do
			
			SOLICITUD_CSV=$(echo $SOLICITUD_CSV | sed 's/ /#.#/g')
			SOLICITUD_CSV=$(echo $SOLICITUD_CSV | sed 's/=/%3D/g')
			SOLICITUD_CSV=$(echo $SOLICITUD_CSV | sed 's/&/%26/g')
			
			traza "- Se procesa la siguiente petición_______________________"
			traza $SOLICITUD_CSV
			
			$SH_PROCESAR_INMEDIATO $SOLICITUD_CSV $DEBUG
			RESULTADO=$?
			if [[ $RESULTADO -ne 0 ]]; then
				traza "- ERROR: Petición no procesada con resultado "$RESULTADO 
				doMail "- ERROR: Petición no procesada con resultado "$RESULTADO "\n${SOLICITUD_CSV}"
				SOL_INC=$(echo $SOLICITUD_CSV | awk -F'|' -v pos=${POS_CAMPO_ID} '{print $pos}')
				marcarIncidente $SOL_INC REC
			else 
				traza "- OK: Petición procesada " 
			fi

			traza "_________________________________________________________________________________"
			
		done
		
		traza 'Tratamiento de ficheros online diferidos (D): Se tratan las peticiones de ficheros que por su coste se procesan de forma diferida por la noche '
		# filtra las solicitudes para soporte Fichero, muestra solo los campos oficina, fecha inicial y fecha final, les aplica
		# un 'uniq' para eliminar duplicados, y resigue su contenido...
		NUM_PETICIONES=$(grep "^.*|.*|.*|.*|.*|.*|D" $FICHERO_SOLICITUDES_FICHERO | wc -l )
		traza "Hay ${NUM_PETICIONES} peticiones de ficheros online diferidos pendientes de tratar"
		PET_DIFERIDAS=$TMP_DIR/pet_diferidas.$CLAVEFICHERO.tmp
		cat $FICHERO_SOLICITUDES_FICHERO | grep "^.*|.*|.*|.*|.*|.*|D" > $PET_DIFERIDAS
		
		cat $PET_DIFERIDAS | while read SOLICITUD_DIF
		do
			
			SOLICITUD_DIF=$(echo $SOLICITUD_DIF | sed 's/ /#.#/g')
			SOLICITUD_DIF=$(echo $SOLICITUD_DIF | sed 's/=/%3D/g')
			SOLICITUD_DIF=$(echo $SOLICITUD_DIF | sed 's/&/%26/g')
			
			traza "- Se procesa la siguiente petición_______________________"
			traza $SOLICITUD_DIF

			#$SH_PROCESAR_NORMAL $SOLICITUD_DIF -n $DEBUG
			$SH_PROCESAR_NORMAL $SOLICITUD_DIF $DEBUG &
			RESULTADO=$?
			if [[ $RESULTADO -ne 0 ]]; then
				traza "- ERROR: Petición no procesada con resultado "$RESULTADO 
				doMail "- ERROR: Petición no procesada con resultado "$RESULTADO "\n${SOLICITUD_CSV}"
				SOL_INC=$(echo $SOLICITUD_CSV | awk -F'|' -v pos=${POS_CAMPO_ID} '{print $pos}')
				marcarIncidente $SOL_INC REC
			else 
				traza "- OK: Petición lanzada " 
			fi

			traza "_________________________________________________________________________________"
			
		done
		
	fi

fi	

#----------------------------------------------------------------------------------------------------------------------
# Eliminar ficheros temporales
#----------------------------------------------------------------------------------------------------------------------
traza 'paso 16: Eliminando ficheros temporales'
traza "doPaso16?: ${doPaso16}"

if [ $doPaso16 -eq 1 ]; then
# elimina ficheros temporales

	while read file
	do
		borraMaquinas_as $file
	done < $FICHERO_BORRADO_TXT
	rm $FICHERO_BORRADO_TXT
	
	
	F_TEMPORALES="${TMP_DIR}/*.tmp"
	F_A_BORRAR=''
	ls -l $F_TEMPORALES | awk '{print $9}' | while read F_A_BORRAR
	do
		traza "Borrando ficheros temporales creado '"$F_A_BORRAR"'"
		borra $F_A_BORRAR
		RESULTADO=$?
		if [[ $RESULTADO -ne 0 ]]; then
			traza "ERROR: Al eliminar los ficheros temporales creados '"$F_A_BORRAR"', el comando 'rm -f' devolvió la respuesta ${RESULTADO}."
			sendMailError "ERROR: Al eliminar los ficheros temporales particionados '"$F_A_BORRAR"', el comando 'rm -f' devolvió la respuesta ${RESULTADO}."
			exit 167
		fi #DEBUG
	done
	
	F_TEMP_PART="${TMP_DIR}/*.tmp.??"
	F_A_BORRAR=''
	ls -l $F_TEMP_PART | awk '{print $9}' | while read F_A_BORRAR
	do
	
		traza "Borrando ficheros temporales particionado creado '"$F_A_BORRAR"'"
		borra $F_A_BORRAR
		RESULTADO=$?
		if [[ $RESULTADO -ne 0 ]]; then
			traza "ERROR: Al eliminar los ficheros temporales particionados '"$F_A_BORRAR"', el comando 'rm -f' devolvió la respuesta ${RESULTADO}."
			sendMailError "ERROR: Al eliminar los ficheros temporales particionados '"$F_A_BORRAR"', el comando 'rm -f' devolvió la respuesta ${RESULTADO}."
			exit 167
		fi #DEBUG
	done
fi

#----------------------------------------------------------------------------------------------------------------------
# Pasa estadísticas de NIVEL_HIST
#----------------------------------------------------------------------------------------------------------------------
traza 'paso 17: Pasa estadísticas de NIVEL_HIST'
traza "doPaso17?: ${doPaso17}"

if [ $doPaso17 -eq 1 ]; then

	TABLE_OWNER=$(grep TMP_DIR ${TABLE_OWNER} | awk -F= '{print($2)}')

	sqlplus -s $CONEXION_INDICES_ORACLE > $TMP_LOG_FILE<< EOF
		EXEC DBMS_STATS.gather_table_stats('${TABLE_OWNER}', 'NIVEL_HIST', estimate_percent => dbms_stats.auto_sample_size, cascade=>TRUE)
		exit
EOF
#-	analyze table nivel_hist compute statistics for table for all indexes;

#el indice antes era idx_nivel_hist_nif . Falla, no se si es un problema de grants ya que el index es del Idecloi1
	#sed '/^$/d' $TMP_LOG_FILE # igual que un 'cat' pero sin líneas en blanco
	trazafile $TMP_LOG_FILE
	if [[ $(grep -c ORA- $TMP_LOG_FILE) -ne 0 || $(grep -c ERR $TMP_LOG_FILE) -ne 0 ]]; then
		traza "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
		cat $TMP_LOG_FILE > TMP_LOG_FILE_DOS
		sendMailError "ERROR: Se ha producido algún error durante la ejecución de sqlplus."
		exit 164
	fi
	rm $TMP_LOG_FILE
fi	


#----------------------------------------------------------------------------------------------------------------------
# Envio de correo con resumen
#----------------------------------------------------------------------------------------------------------------------
traza 'paso 18: Envio de correo con resumen'
traza "doPaso18?: ${doPaso18}"

if [ $doPaso18 -eq 1 ]; then

export SEPARADOR="***********************************************************************************************************************************************"
	# Reestablim el lang original perquè amb el que posem per al procés d'importació s'envia el correu amb 'xinos' i el cos arriba annexat
	#export LANG=${ORI_LANG}
	doPro=0
	FICHERO_MAIL=$TMP_DIR/mailProceso.$CLAVEFICHERO.tmp

	if [[ $TOTAL_TRATADAS -eq 0 && $NUM_SOLICITUDES_PENDIENTES -ne 0 ]]; then
		inc "ERROR: Hay ${NUM_SOLICITUDES_PENDIENTES} peticiones pendientes y no se ha activado ninguna. El proceso termirará con retcode 180."
		doMail "ERROR: Hay ${NUM_SOLICITUDES_PENDIENTES} peticiones pendientes y no se ha activado ninguna. El proceso termirará con retcode 180."
	fi

	APLICACION=$(grep APLICACION ${CONFIG_FILE} | awk -F= '{print($2)}')

	echo -e "${SEPARADOR}\n" >>  $FICHERO_MAIL
	echo -e "Resumen del Proceso de Importación de ${APLICACION} ${CLAVEFICHERO} \n" >> $FICHERO_MAIL
	echo -e "${SEPARADOR}\n"  >> $FICHERO_MAIL
	cat $RESUMEN_PROC  >> $FICHERO_MAIL
	echo -e "\n$SEPARADOR\n"  >> $FICHERO_MAIL
	cat $INCIDENCIAS_PROC  >> $FICHERO_MAIL
	echo -e "\n$SEPARADOR\n"  >> $FICHERO_MAIL
	cat $FICHERO_MAIL_INC >> $FICHERO_MAIL
	echo -e "\n$SEPARADOR\n"  >> $FICHERO_MAIL
	echo -e "CORREO ENVIADO AUTOMÁTICAMENTE - NO RESPONDER A ESTA DIRECCIÓN DE CORREO\n"  >> $FICHERO_MAIL

	DESTINATARIOS=$(grep DESTINATARIOS_MAIL ${CONFIG_FILE} | awk -F= '{print($2)}')
	ENTORNO=$(grep ENTORNO_MAIL ${CONFIG_FILE} | awk -F= '{print($2)}')
	CABECERA_MAIL=$(grep CABECERA_MAIL ${CONFIG_FILE} | awk -F= '{print($2)}')

	TITULO_MAIL="${CABECERA_MAIL} - Importación de Históricos (Resumen) ${CLAVEFICHERO} entorno ${ENTORNO}"
	
	export LANG=${ORI_LANG}
	cat $FICHERO_MAIL | mailx -v -s "${TITULO_MAIL}" $DESTINATARIOS
	traza "lanzando: cat ${FICHERO_MAIL} | mailx -v -s \"${TITULO_MAIL}\" ${DESTINATARIOS}"

	borra $FICHERO_MAIL
	borra $RESUMEN_PROC  
	borra $INCIDENCIAS_PROC
	borra $FICHERO_MAIL_INC

	traza " Fin de envío de correo OK  "
fi	

if [[ $TOTAL_TRATADAS -eq 0 && $NUM_SOLICITUDES_PENDIENTES -ne 0 ]]; then
	traza "ERROR: Hay ${NUM_SOLICITUDES_PENDIENTES} peticiones pendientes/incidentes y no se ha activado ninguna. El proceso termirará con retcode 180."
	#exit 180
fi

#----------------------------------------------------------------------------------------------------------------------
traza "Fin de importación de históricos OK a "$(date +"%d/%m/%Y %H:%M:%S")
exit 0
