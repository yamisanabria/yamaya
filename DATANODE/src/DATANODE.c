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


	long long int tamanio_arch_bin = 1024*1024*20;
	log_info(logger,"Tamanio archivo bin: %lld",tamanio_arch_bin);
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
	recibir_saludo(FILESYSTEM,sockFS,logger,HANDSHAKEOK);
	log_info(logger,"Se establecio conexion con Filesystem");
	printf("Se establecio conexion con Filesystem \n");

	log_info(logger, "Envio todos mis datos al File System.");

	//creo el hilo para atender FS
	pthread_t thFS;
	pthread_create(&thFS, NULL,(void*)enviarDatos_FS, NULL);
	log_info(logger, "Se ha creado el hilo para atender FILESYSTEM");

	pthread_join(thFS, NULL);

}

void enviarDatos_FS (){

	t_datanode nodoAenviar;

		nodoAenviar.ipNodo = malloc(sizeof(ip_nodo_actual));
		nodoAenviar.ipNodo = ip_nodo_actual;
		nodoAenviar.ipNodo_long = string_length(ip_nodo_actual);
		nodoAenviar.puertoNodo = puertoWorker;
		nodoAenviar.puertoNodo_long = sizeof(puertoWorker);
		nodoAenviar.nombreNodo = malloc(sizeof(ip_nodo_actual));
		nodoAenviar.nombreNodo = nombreNodo;
		nodoAenviar.nombreNodo_long = string_length(nombreNodo);
		nodoAenviar.cantidad_bloques_long = sizeof(cant_max_bloques);
		nodoAenviar.cantidad_bloques =cant_max_bloques;


		char* paqueteSerializado;
		nodoAenviar.total_size = sizeof(nodoAenviar.ipNodo) + nodoAenviar.ipNodo_long + sizeof(nodoAenviar.puertoNodo_long) + nodoAenviar.puertoNodo_long + sizeof(nodoAenviar.nombreNodo_long) + nodoAenviar.nombreNodo_long + sizeof(nodoAenviar.cantidad_bloques_long) + nodoAenviar.cantidad_bloques_long;
		paqueteSerializado = serializarEstructura(&nodoAenviar);
		send(sockFS, paqueteSerializado, nodoAenviar.total_size,0);

		sem_wait(&mutex_log);
		log_info(logger, "Envie a FileSystem: ip=%s, puerto=%i, nombre=%s, cantidad bloques=%i\n",nodoAenviar.ipNodo,nodoAenviar.puertoNodo ,nodoAenviar.nombreNodo, nodoAenviar.cantidad_bloques);
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

	return paqueteSerializado;
}

void dispose_package(char **package){
	free(*package);
}

int enviar_saludo(int id_origen, int sock, t_log* logger,int tipo_mensaje){

	// inicializo variables
	char* buffer;
	int numbytes;

	//creo el buffer
	if((buffer = (char*) malloc (sizeof(char) * MAXDATASIZE)) == NULL)
	{
		log_error(logger,"Error al reservar memoria para el buffer");
		return -1;
	}

	// creo mensaje y le asigno valores correspondientes

	t_mensaje mensaje;
	mensaje.tipo = tipo_mensaje;
	mensaje.id_proceso = id_origen;
	memcpy(buffer,&mensaje,SIZE_MSG);

	// mando el handshake a destino
	if((numbytes=send(sock,&mensaje,SIZE_MSG,0))<=0)
	{
		log_error(logger, "Error en el send de enviar_saludo");
		return -1;
	}

	free(buffer);
	log_info(logger, "Se envio saludo");
	return sock;
}

// Funcion recibe HANDSHAKE o HANDSHAKEOK dependiendo del tipo de mensaje especificado (#define)
// Retorna int con valor -1 en caso de error y 0 en caso de exito

int recibir_saludo(int id_destino, int sock, t_log* logger,int tipo_mensaje)
{
	//pongo en 0 el buffer para recibir
	char* buffer;

	//creo el buffer
		if((buffer = (char*) malloc (sizeof(char) * MAXDATASIZE)) == NULL)
		{
			log_error(logger,"Error al reservar memoria para el buffer");
			return -1;
		}

	memset(buffer,'\0',MAXDATASIZE);
	t_mensaje mensaje;
	int numbytes;

	//recibo en el buffer
	if((numbytes=recv(sock,buffer,SIZE_MSG,0))<=0 )
	{
		log_error(logger, "Error en el recv en el socket de recibir saludo");
		close(sock);
		return -1;
	}


	//copio la respuesta del buffer
	memcpy(&mensaje,buffer,SIZE_MSG);


	switch (mensaje.tipo)
		{
		case HANDSHAKEOK:
			log_info(logger, "Conexion Lograda con FS");
//	         free(buffer);  //Lo comento por que me esta generando *** Error in `./datanode': double free or corruption (top): 0x08846890 ***
	                        //[1]    10618 abort (core dumped)  ./datanode
		break;
		case HANDSHAKE:
			log_info(logger, "Recibi HANDSHAKE de FS");
			log_info(logger, "Conexion Lograda con FS");
			free(buffer);
		break;
		default:
			log_error(logger, "Error en el tipo de mensaje enviado");
			free(buffer);
		break;
		}

		free(buffer);

	return 0;
}


int main(int argc, char **argv) {

	    inicializar(argv[1]);
		conectar_FS();
		//log_info(logger, "<<Proceso DataNode finalizó>>");
		//return EXIT_SUCCESS;
}
