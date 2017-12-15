/*
 ============================================================================
 Name        : DATANODE.c
 Author      : yo
 Version     :
 Copyright   : Grupo YAMAYA
 Description : Hello World in C, Ansi-style
 ============================================================================
 */


#include "DATANODE.h"

void cargarConfiguraciones() {

	configuration = config_create("/home/utnso/workspace/tp-2017-2c-Yamaya/branches/socketsSimples/CONFIG_NODO");
	log_info(logger, "Intentando levantar el archivo de configuraciones.");

	if(configuration==NULL){
				log_error(logger, "Error el archivo de configuraciones no existe.");
				exit(-1);
		}

		strcpy(ipFS,config_get_string_value(configuration, "IP_FILESYSTEM"));
		log_info(logger, "La IP del FileSystem es: %s", ipFS);

		if (config_has_property(configuration, "PUERTO_FILESYSTEM")) {

						puertoFS = config_get_int_value(configuration, "PUERTO_FILESYSTEM");

						log_info(logger, "El puerto del File System es: %i", puertoFS);

					} else {

						log_error(logger, "Error al obtener el puerto del File System");

						configOk = 0;
					}

	if (config_has_property(configuration, "NOMBRE_NODO")) {

			nombreNodo = config_get_string_value(configuration, "NOMBRE_NODO");

			log_info(logger, "El nombre del Nodo es: %s", nombreNodo);

		} else {

			log_error(logger, "Error al obtener el Nombre del Nodo");

			configOk = 0;
	}
	if (config_has_property(configuration, "PUERTO_WORKER")) {

							puertoWorker = config_get_int_value(configuration, "PUERTO_WORKER");

							log_info(logger, "El puerto del Worker es: %i", puertoWorker);

						} else {

							log_error(logger, "Error al obtener el puerto del DATANODE");

							configOk = 0;
						}

	if (config_has_property(configuration, "RUTA_DATABIN")) {

			rutaDataBin = config_get_string_value(configuration, "RUTA_DATABIN");

			log_info(logger, "La ruta del Data.Bin es: %s", rutaDataBin);

		} else {

			log_error(logger, "Error al obtener la ruta del Data.Bin");

			configOk = 0;
	}

	if (config_has_property(configuration, "TAMANIO_DATABIN_MB")) {

			tam_dataBin = config_get_int_value(configuration, "TAMANIO_DATABIN_MB");

			log_info(logger, "El tamaño del DataBin es: %i", tam_dataBin);


	} else {

		log_error(logger, "Error al obtener el tamaño del DataBin");

		configOk = 0;

	}
	strcpy(ip_nodo_actual,config_get_string_value(configuration, "IP_NODO"));
				log_info(logger, "La IP del nodo actual es: %s", ip_nodo_actual);

	if (!configOk) {
		log_error(logger, "Debido a errores en las configuraciones, se aborta la ejecución... (REVISE ARCH. CONFIGURACIONES)");
		exit(-1);
	}

}

void inicializar (){
	dir_temp = string_new();
	arch_bin = string_new();

	// archivo de logs
	logger = log_create("logDataNode", "DataNode LOG", true, LOG_LEVEL_DEBUG);
	log_info(logger, "<<Proceso DataNode inició>>");

	// archivo de configuracion
	cargarConfiguraciones();

	FILE* bin;
		if ((bin = fopen(rutaDataBin, "w+b")) == NULL) {
				log_error(logger, "Error al al crear archivo DataBin\n");
				abort();
		}else{
			log_info(logger, "Archivo Databin abierto correctamente");
		}

	long long int tamanio_arch_bin = 1024*1024*tam_dataBin;
	log_info(logger,"Tamanio archivo bin: %lld",tamanio_arch_bin);


	char* bufferBin;
	bufferBin = (char*) malloc (sizeof(char) * tamanio_arch_bin);
	memset(bufferBin,'\0',tamanio_arch_bin);
	fwrite(bufferBin,1,tamanio_arch_bin,bin);

	long long int bloque = 1024*1024;
	cant_max_bloques = (int)(tamanio_arch_bin/ bloque);
	log_info(logger,"Cantidad maxima de bloques que soporta el nodo: %d",cant_max_bloques);

	sem_init(&mutex_log,0,1);
}

int conectar_cliente (int puerto, char ip_destino[16],t_log* logger){
	log_info(logger, "Intentando levantar conexión con FS.");

	int sock= 0;
	struct sockaddr_in direccion_cliente;
	direccion_cliente.sin_family=AF_INET;
	direccion_cliente.sin_port=htons(puerto);
	direccion_cliente.sin_addr.s_addr=inet_addr(ip_destino);
	memset(&(direccion_cliente.sin_zero),0,8);


	log_info(logger, "[Cliente] Consiguiendo datos de red...");

	//creo el socket
	if((sock=socket(AF_INET,SOCK_STREAM,0))==-1)
	{

		log_error(logger, "Error al abrir el socket");
		close(sock);
		return -1;
	}

	//conecto por la ip dada
	if(connect(sock,(struct sockaddr *)&direccion_cliente,sizeof(struct sockaddr))==-1)
	{
		log_error(logger, "Problema al levantar conexión con FS en IP:%s PUERTO:%i (¿FS se encuentra levantado?)",ip_destino,puerto);
		close(sock);
		return -1;
	}

	log_info(logger, "DATANODE Conectado a destino");

	return sock;
}

void conectar_FS(void){
	sockFS = conectar_cliente(puertoFS,ipFS,logger);

	if(sockFS == -1)
	{
		log_error(logger,"Falló la conexión con FileSystem.");
		exit(1);
	}

	enviar_saludo(DATANODE,sockFS,logger,HANDSHAKE);
	log_info(logger,"Se establecio conexion con Filesystem");
	printf("Se establecio conexion con Filesystem \n");

	log_info(logger, "Envio todos mis datos al File System\n");
	enviarDatos_FS();
	log_info(logger,"Datos enviados correctamente");


	//creo un hilo para escuchar las operaciones del fs
	pthread_create(&thEscucharFS, NULL,crearListenerFS, NULL);
	log_info(logger, "Se ha creado el hilo para atender FILESYSTEM");
	pthread_join(thEscucharFS, NULL);

}

void* crearListenerFS(void* param){
	int8_t estado = 1;

		while (estado) {

			//validarDesconexionFS(status);
			realizarOperacionFS(sockFS, &estado);
		}

	return param;
}

void realizarOperacionFS(int socket, int8_t* estado) {
	uint8_t cod_operacion;
	uint16_t numero_bloque;
	log_info(logger,"Esperando codigo de operacion para trabajar.");
	*estado = recv(socket, &cod_operacion, sizeof(cod_operacion), 0);
	sem_wait(&mutex_log);
	log_debug(logger, "Recibi el siguiente codigo de operacion: %d\n", cod_operacion);
	sem_post(&mutex_log);

	switch (cod_operacion) {

		case GET_BLOQUE:
			*estado = recv(socket, &numero_bloque, sizeof(numero_bloque), 0);
			log_info(logger, "Solicitud para obtener el bloque: %d\n", numero_bloque);

			break;

		case SET_BLOQUE:
			*estado = recv(socket, &numero_bloque, sizeof(uint16_t), 0);
			log_info(logger, "Solicitud para setear el bloque: %d\n", numero_bloque);
			break;

	}
}


void enviarDatos_FS (){

	t_datanode nodoAenviar;

		nodoAenviar.ipNodo = malloc(sizeof(ip_nodo_actual));
		nodoAenviar.ipNodo = ip_nodo_actual;
		nodoAenviar.ipNodo_long = string_length(ip_nodo_actual);
		nodoAenviar.puertoNodo = puertoWorker;
		nodoAenviar.puertoNodo_long = sizeof(puertoWorker);
		nodoAenviar.nombreNodo = malloc(sizeof(nombreNodo));
		nodoAenviar.nombreNodo = nombreNodo;
		nodoAenviar.nombreNodo_long = string_length(nombreNodo);
		nodoAenviar.cantidad_bloques_long = sizeof(cant_max_bloques);
		nodoAenviar.cantidad_bloques = cant_max_bloques;


		char* paqueteSerializado;

		nodoAenviar.total_size = sizeof(nodoAenviar.ipNodo_long) +
				nodoAenviar.ipNodo_long +
				sizeof(nodoAenviar.puertoNodo_long) +
				nodoAenviar.puertoNodo_long +
				sizeof(nodoAenviar.nombreNodo_long) +
				nodoAenviar.nombreNodo_long +
				sizeof(nodoAenviar.cantidad_bloques_long) +
				nodoAenviar.cantidad_bloques;
		paqueteSerializado = serializarEstructura(&nodoAenviar);
		send(sockFS, paqueteSerializado, nodoAenviar.total_size,0);

		sem_wait(&mutex_log);
		log_info(logger, "Envie a FileSystem: ip=%s, puerto=%d, nombre=%s, cantidad bloques=%d\n",nodoAenviar.ipNodo,nodoAenviar.puertoNodo ,nodoAenviar.nombreNodo, nodoAenviar.cantidad_bloques);
		sem_post(&mutex_log);
		dispose_package(&paqueteSerializado);
}

char* serializarEstructura(t_datanode* estructura){

	char *paqueteSerializado = malloc(estructura->total_size);
	int offset = 0;
	int size;

	size =  sizeof(estructura->ipNodo_long);
	memcpy(paqueteSerializado + offset, &(estructura->ipNodo_long), size);
	offset += size;

	size =  estructura->ipNodo_long;
	memcpy(paqueteSerializado + offset, estructura->ipNodo, size);
	offset += size;

	size =  sizeof(estructura->puertoNodo_long);
	memcpy(paqueteSerializado + offset, &(estructura->puertoNodo_long), size);
	offset += size;

	size =  estructura->puertoNodo_long;
	memcpy(paqueteSerializado + offset, &(estructura->puertoNodo), size);
	offset += size;

	size =  sizeof(estructura->nombreNodo_long);
	memcpy(paqueteSerializado + offset, &(estructura->nombreNodo_long), size);
	offset += size;

	size =  estructura->nombreNodo_long;
	memcpy(paqueteSerializado + offset, estructura->nombreNodo, size);
	offset += size;

	size =  sizeof(estructura->cantidad_bloques_long);
	memcpy(paqueteSerializado + offset, &(estructura->cantidad_bloques_long), size);
	offset += size;

	size =  estructura->cantidad_bloques;
	memcpy(paqueteSerializado + offset, &(estructura->cantidad_bloques), size);

	offset += size;
	printf("OFFSET: %i\n",offset);
	printf("total_size: %i\n",estructura->total_size);
	return paqueteSerializado;
}

void dispose_package(char **package){
	free(*package);
}

int enviar_saludo(int id_origen, int fs_sock, t_log* logger,int tipo_mensaje){

	uint8_t codIdentificacion;
			codIdentificacion = 30; //DATANODE se identifica con el nro 0 ->TODO PASARLO ARRIBA CON DEFINE
	//ME PRESENTO Y LE DIGO QUIEN SOY
			if(send(fs_sock,&(codIdentificacion),sizeof(uint8_t),0)==-1){
				sem_wait(&mutex_log);
				log_error(logger, "Error al identificarme con mi número=%i. (¡Revise código!)",codIdentificacion);
				sem_post(&mutex_log);
				return -1;
			}else{
			sem_wait(&mutex_log);
			log_info(logger, "Me identifique al FS... (Soy=%i)",codIdentificacion);
			sem_post(&mutex_log);} //Envio primero el Codigo de operacion para que YAMA pueda usar un Switch.

	return 0;
}





int main(int argc, char **argv) {

	    inicializar(argv[1]);
		conectar_FS();
		log_info(logger, "<<Proceso DataNode finalizó>>");
		return EXIT_SUCCESS;
}
