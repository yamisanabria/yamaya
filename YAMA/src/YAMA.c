/*
 ============================================================================
 Name        : YAMA.c
 Author      : yo
 Version     :
 Copyright   : Grupo YAMAYA
 Description : Ansi-style
 ============================================================================
 */

<<<<<<< HEAD
#include "yama.h"
=======
#include <stdio.h>
#include <stdlib.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/temporal.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>
#include <semaphore.h>
#include "YAMA.h"
#include <fcntl.h> // for open
#include <unistd.h> // for close
>>>>>>> refs/remotes/origin/socketsSimples

t_log* logger;
t_config* configuration;
int configOk=1;

char* ipFS;
int puertoFS;
int retardoPlanificacion;
char* algoritmoBalanceo;
<<<<<<< HEAD
char* ArchivoOriginalDeYAMAFS;
int sockFS;
=======
sem_t mutex_log;
pthread_t hiloMaster;
pthread_t hiloFileSystem;
uint8_t socket_fs;

/** Longitud del buffer  */
#define BUFFERSIZE 512

#define PUERTO_YAMA 9262
#define CONEXIONES_MASTER_MAX_SIMULTANEAS 6 //Definimos cuantas maximas conexiones vamos a tener pendientes al mismo tiempo (Listen)
#define CodOP_Master_Solic_Nuevo_Trab 0

>>>>>>> refs/remotes/origin/socketsSimples

void cargarConfiguraciones() {

	logger = log_create("logYAMA", "YAMA LOG", true, LOG_LEVEL_DEBUG);
	log_info(logger, "<<Proceso YAMA iniciando>> (CARGANDO CONFIGURACIONES)");
	configuration = config_create("CONFIG_YAMA");
	if(configuration==NULL){
		log_error(logger, "Verifique el archivo de configuracion de entrada. (¿Este existe?)");
		exit(-1);
	}
	if (config_has_property(configuration, "FS_IP")) {

			ipFS = config_get_string_value(configuration, "FS_IP");


			log_info(logger, "La IP del File System es: %s", ipFS);

		} else {

			log_error(logger, "Error al obtener la IP del File System");

			configOk = 0;
		}

		if (config_has_property(configuration, "FS_PUERTO")) {

			puertoFS = config_get_int_value(configuration, "FS_PUERTO");

			log_info(logger, "El puerto del File System es: %i", puertoFS);

		} else {

			log_error(logger, "Error al obtener el puerto del File System");

			configOk = 0;
		}

		if (config_has_property(configuration, "RETARDO_PLANIFICACION")) {

			retardoPlanificacion = config_get_int_value(configuration, "RETARDO_PLANIFICACION");

			log_info(logger, "El tiempo de retardo para la planificación definido es: %i ms.", retardoPlanificacion);

		} else {

			log_error(logger, "Error al obtener el tiempo de retardo para la planificación.");

			configOk = 0;
		}

		if (config_has_property(configuration, "ALGORITMO_BALANCEO")) {

			algoritmoBalanceo = config_get_string_value(configuration, "ALGORITMO_BALANCEO");

			log_info(logger, "Definido el algoritmo de balanceo como: %s", algoritmoBalanceo);

		} else {

			log_error(logger, "Error al obtener el algoritmo de balanceo.");

			configOk = 0;
		}


		if (!configOk) {
				log_error(logger, "Debido a errores en las configuraciones, se aborta la ejecución... (REVISE ARCH. CONFIGURACIONES)");
				exit(-1);

		}


}

void *hiloClienteSocketMaster(int client_sock){

	 //Get the socket descriptor
	uint8_t master_socket = *(uint8_t*)client_sock;
	int8_t status = 1;
	    while((status)){
	    	realizarOperacionMaster(master_socket);
	    	status=0;
	    	sem_wait(&mutex_log);
	    	log_info(logger, "Master %i -> Finalizamos el trabajo con Master. Nos despedimos de Master...", master_socket);
	    	sem_post(&mutex_log);

	    	close(master_socket);
	    }
return 0;
}

int realizarOperacionMaster(uint8_t master_socket){
	uint8_t codop;
	T_master_yama_start estructuraARecibir;

	int bytesRecibidos=recv(master_socket, &codop, sizeof(uint8_t),0);
	if (bytesRecibidos <= 0) {

		log_error(logger,"Master %i -> Se ha desconectado.", master_socket);
		return -1;
	}else{
		log_debug(logger,"Master %i -> Solicitada la operación Nro: %i.", master_socket , codop);
	}

	switch(codop){
	case CodOP_Master_Solic_Nuevo_Trab:

		sem_wait(&mutex_log);
		log_info(logger, "Master %i -> Recibiendo datos iniciales desde Master. (CODOP: CodOP_Master_Solic_Nuevo_Trab -> 0)", master_socket);
		sem_post(&mutex_log);

		recibirYDeserializar(&estructuraARecibir,master_socket); //TODO Manejar error de retorno, en caso de fallar que finalice la aplicacion o la conexion.

		log_debug(logger, "Recibimos desde Master los siguientes datos: ArchOriginal=%s ArchDestino=%s",estructuraARecibir.archivo_ruta_origen_YamaFs,estructuraARecibir.archivo_ruta_destino_YamaFs);

		solicitarInformacionAlFS(&estructuraARecibir,socket_fs);

		/*bytesRecibidos=recv(master_socket,&(nodoRecibido.long_archivo_ruta_origen_YamaFs), sizeof(nodoRecibido.long_archivo_ruta_origen_YamaFs),0);
		if (bytesRecibidos <= 0) {

			log_error(logger,"Master %i -> Se ha desconectado.", master_socket);
			return -1;
		}else{
			log_debug(logger,"Master %i -> BytesRecibidos: %i con: '%i' ", master_socket, bytesRecibidos, nodoRecibido.long_archivo_ruta_origen_YamaFs);
		}
*/

	break;
	 default:
		 log_error(logger,"Master %i -> Se recibió un Código de Operación Inexistente: '%i' (¡Revisá el Código en Master!)", master_socket, codop); //TODO Revisar el cierre al ingresar un CODOP Erroneo
		 return -1;
	}
	//free(buffer);
	return 0;
}


int recibirYDeserializar(T_master_yama_start *estructuraARecibir, int sockN) {

	int estado = 1;
	int tam_buffer;
	char* buffer = malloc(tam_buffer = sizeof(uint32_t));

	//Obtengo archivo_ruta_origen_YamaFs
	uint32_t long_file_original;
	estado = recv(sockN, buffer, sizeof(estructuraARecibir->long_archivo_ruta_origen_YamaFs), 0);
	if (!estado)return 0;
		memcpy(&(long_file_original), buffer, tam_buffer);

	estructuraARecibir->archivo_ruta_origen_YamaFs = malloc(long_file_original + 1);
	memset(estructuraARecibir->archivo_ruta_origen_YamaFs, '\0', long_file_original + 1);
	estado = recv(sockN, estructuraARecibir->archivo_ruta_origen_YamaFs, long_file_original, 0);
	if (!estado)return 0;


	//Obtengo el path destino del archivo resultante del trabajo
	uint32_t nombre_long;
	tam_buffer = sizeof(uint32_t);
	estado = recv(sockN, buffer, sizeof(estructuraARecibir->long_archivo_ruta_destino_YamaFs), 0);
	if (!estado)return 0;
	memcpy(&(nombre_long), buffer, tam_buffer);

	estructuraARecibir->archivo_ruta_destino_YamaFs = malloc(nombre_long + 1);
	memset(estructuraARecibir->archivo_ruta_destino_YamaFs, '\0', nombre_long + 1);
	estado = recv(sockN, estructuraARecibir->archivo_ruta_destino_YamaFs, nombre_long, 0);
    if (!estado)return 0;



	/*//Obtengo el puerto
	uint16_t puerto_long;
	tam_buffer = sizeof(uint16_t);
	estado = recv(sockN, buffer, sizeof(nodoRecibido->puertoNodo_long), 0);
	if (!estado)return 0;

	memcpy(&(puerto_long), buffer, tam_buffer);

	uint16_t bufferNuevo;
	estado = recv(sockN, &bufferNuevo, puerto_long,0);
	if (!estado)return 0;
    nodoRecibido->puertoNodo=bufferNuevo;
    */



	free(buffer);
	return estado;

}

int planificarNuevaTarea(T_master_yama_start *estructuraConDatos){

	/*if(conectarConFS()==-1){
		return -1;
	}
	return 0;*/ //TODO Planificacion
}


void* socketListenerMaster(void* param){
	struct sockaddr_in direccionServidor;
	direccionServidor.sin_family = AF_INET;
	direccionServidor.sin_addr.s_addr = INADDR_ANY;
	direccionServidor.sin_port = htons(PUERTO_YAMA);

	int listenningSocket = socket(AF_INET, SOCK_STREAM, 0);

	int activado = 1;
	setsockopt(listenningSocket, SOL_SOCKET, SO_REUSEADDR, &activado, sizeof(activado)); //Para decir al sistema de reusar el puerto

	if (bind(listenningSocket, (void*) &direccionServidor, sizeof(direccionServidor)) != 0) {
		log_error(logger, "Falló el bind. (¿Seguro que no hay dos instancias de YAMA corriendo?)");
		exit(-1);
	}
	sem_wait(&mutex_log);
	log_info(logger, "YAMA levanto servidor y se encuentra esperando conexiones de Masters por el puerto %i.",PUERTO_YAMA);
	sem_post(&mutex_log);
	listen(listenningSocket, CONEXIONES_MASTER_MAX_SIMULTANEAS);

	//------------------------------

	struct sockaddr_in direccionCliente;
	unsigned int tamanioDireccion = sizeof(struct sockaddr_in);
	int client_sock;
	pthread_t thread_id;
	sem_wait(&mutex_log);
	log_info(logger,"Esperando conexiones de clientes Master...");
	sem_post(&mutex_log);
	 while( (client_sock = accept(listenningSocket, (void*) &direccionCliente, &tamanioDireccion)) )
	    {
		 sem_wait(&mutex_log);
		 log_info(logger,"Master %i -> Conexión nueva aceptada.",(int)client_sock);
		 sem_post(&mutex_log);

	        if( pthread_create( &thread_id , NULL , (void *) hiloClienteSocketMaster, (void*) &client_sock) < 0)
	        {
	        	log_error(logger,"Falló al crear el hilo para atender a Master. (¡Revise código!");
	            exit(-1);
	        }

	        //Now join the thread , so that we dont terminate before the thread
	        pthread_join( thread_id , NULL);

	    }


return 0;
}

int conectar_cliente (int puerto, char ip_destino[16],t_log* logger)
{
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

	log_info(logger, "YAMA Conectado a destino");

	return sock;
}

void conectar_FS(void)
{
	sockFS = conectar_cliente(puertoFS,ipFS,logger);

	if(sockFS == -1)
	{
		log_error(logger,"Falló la conexión con FileSystem.");
		exit(1);
	}

	enviar_saludo(YAMA,sockFS,logger,HANDSHAKE);
	recibir_saludo(FILESYSTEM,sockFS,logger,HANDSHAKEOK);
	log_info(logger,"Se establecio conexion con Filesystem");
	printf("Se establecio conexion con Filesystem \n");

	//creo el hilo para atender FS
	pthread_t thFS;
	pthread_create(&thFS, NULL,(void*)atender_FS, NULL);
	log_info(logger, "Se ha creado el hilo para atender FILESYSTEM");

	pthread_join(thFS, NULL);

}

void atender_FS (void* param)
{
	while(1)
	{
		t_mensaje* mensaje = calloc(SIZE_MSG,1);
		if(recv(sockFS,mensaje,SIZE_MSG,0)<=0)
		{
			printf("Error intentando recibir mensaje\n");
			printf("FileSystem se ha desconectado\n");
			exit(1);
		}

	}
}

int enviar_saludo(int id_origen, int sock, t_log* logger,int tipo_mensaje)
{
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


int solicitarInformacionAlFS(T_master_yama_start *estructuraConDatos, int fs_sock){

	uint8_t codIdentificacion;
		codIdentificacion = 31; //YAMA se identifica con el nro 31 ->TODO PASARLO ARRIBA CON DEFINE
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

//LE DIGO EL CODIGO DE OPERACION
		uint8_t codigoOperacion=82; //TODO crear codigos de operacion
		if(send(fs_sock,&(codigoOperacion),sizeof(uint8_t),0)==-1){
			sem_wait(&mutex_log);
			log_error(logger, "Error al solicitar la operación=%i. (¡Revise código!)",codigoOperacion);
			sem_post(&mutex_log);
			return -1;
		}else{
			sem_wait(&mutex_log);
			log_info(logger, "Le solicite la operación (%i).",codigoOperacion);
			sem_post(&mutex_log);} //Envio primero el Codigo de operacion para que YAMA pueda usar un Switch.
//LE DIGO LA LONGITUD DEL PATH

		uint8_t long_path_origen=strlen(estructuraConDatos->archivo_ruta_origen_YamaFs)+1;
		if(send(fs_sock,&(long_path_origen),sizeof(uint8_t),0)==-1){
			sem_wait(&mutex_log);
			log_error(logger, "Error %i. (¡Revise código!)",long_path_origen);
			sem_post(&mutex_log);
			return -1;
		}else{
			sem_wait(&mutex_log);
			log_info(logger, "Long: (%i).",long_path_origen);
			sem_post(&mutex_log);}
//LE DIGO EL PATH
		if(send(fs_sock,estructuraConDatos->archivo_ruta_origen_YamaFs,long_path_origen,0)==-1){
			sem_wait(&mutex_log);
			log_error(logger, "Error %s. (¡Revise código!)",estructuraConDatos->archivo_ruta_origen_YamaFs);
			sem_post(&mutex_log);
			return -1;
		}else{
			sem_wait(&mutex_log);
			log_info(logger, "txt: (%s).",estructuraConDatos->archivo_ruta_origen_YamaFs);
			sem_post(&mutex_log);
		}

	return 0;
}

void* conectarConFS(void* param){
	log_info(logger, "Intentando levantar conexión con FS.");
	struct sockaddr_in direccionServidor;
		direccionServidor.sin_family = AF_INET;
		direccionServidor.sin_addr.s_addr = inet_addr(ipFS);
		direccionServidor.sin_port = htons(puertoFS);


	int cliente_sock = socket(AF_INET, SOCK_STREAM, 0);

	if (connect(cliente_sock, (void*) &direccionServidor, sizeof(direccionServidor)) != 0) {
		log_error(logger, "Problema al levantar conexión con FS en IP:%s PUERTO:%i (¿FS se encuentra levantado?)",ipFS,puertoFS);
		abort();
		//return -1;
	}
	log_info(logger,"Conexión establecida con FS.");

	socket_fs=cliente_sock;


return 0;

}


int main(void) {
<<<<<<< HEAD

=======
	sem_init(&mutex_log,0,1);
>>>>>>> refs/remotes/origin/socketsSimples
	cargarConfiguraciones();
<<<<<<< HEAD
	//printf("Valor recibido %s \n", argv[1]);
	conectar_FS();
	//cargarServidorParaEscucharAMaster();
=======

	if(pthread_create(&hiloMaster,NULL,(void*) socketListenerMaster,NULL)<0){
		log_error(logger, "Falló al crear el hilo para escuchar a Master. (¡Revise Código!)");
	}


	if(pthread_create(&hiloFileSystem,NULL,(void*) conectarConFS,NULL)<0){
		log_error(logger, "Falló al crear el hilo para hablar a FileSystem. (¡Revise Código!)");
	}
	pthread_join(hiloFileSystem,NULL);
	pthread_join(hiloMaster,NULL);



>>>>>>> refs/remotes/origin/socketsSimples


	log_info(logger, "<<Proceso YAMA Finalizado>>");


}
