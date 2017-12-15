/*
 ============================================================================
 Name        : YAMA.c
 Author      : yo
 Version     :
 Copyright   : Grupo YAMAYA
 Description : Ansi-style
 ============================================================================
 */

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

t_log* logger;
t_config* configuration;
int configOk=1;
char* ipFS;
int puertoFS;
int retardoPlanificacion;
char* algoritmoBalanceo;
pthread_t hiloListener;
sem_t mutex_log;
/** Longitud del buffer  */
#define BUFFERSIZE 512

#define PUERTO_YAMA 9262
#define CONEXIONES_MASTER_MAX_SIMULTANEAS 6 //Definimos cuantas maximas conexiones vamos a tener pendientes al mismo tiempo (Listen)
#define CodOP_Master_Solic_Nuevo_Trab 0


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
	 char* buffer=malloc(BUFFERSIZE);
	 memset(buffer, 0, BUFFERSIZE);
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
		bytesRecibidos=recv(master_socket,buffer, BUFFERSIZE,0);
		if (bytesRecibidos <= 0) {

			log_error(logger,"Master %i -> Se ha desconectado.", master_socket);
			return -1;
		}else{
			log_debug(logger,"Master %i -> BytesRecibidos: %i con: '%s' ", master_socket, bytesRecibidos, buffer);
			memset(buffer, 0, BUFFERSIZE);
		}

	break;
	 default:
		 log_error(logger,"Master %i -> Se recibió un Código de Operación Inexistente: '%i' (¡Revisá el Código en Master!)", master_socket, codop); //TODO Revisar el cierre al ingresar un CODOP Erroneo
		 return -1;
	}
	free(buffer);
	return 0;
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
		log_error(logger, "Falló el bind. (¿Seguro que no hay dos instancias de YAMA corriendo? ");
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


int conectarConFS(){
	log_info(logger, "Intentando levantar conexión con FS.");
	struct sockaddr_in direccionServidor;
		direccionServidor.sin_family = AF_INET;
		direccionServidor.sin_addr.s_addr = inet_addr(ipFS);
		direccionServidor.sin_port = htons(puertoFS);


	int cliente = socket(AF_INET, SOCK_STREAM, 0);

	if (connect(cliente, (void*) &direccionServidor, sizeof(direccionServidor)) != 0) {
		log_error(logger, "Problema al levantar conexión con FS en IP:%s PUERTO:%i (¿YAMA se encuentra levantado?)",ipFS,puertoFS);

		return -1;
	}
	log_info(logger,"Conexión establecida con FS.");


	while (1) {
		//sleep(3);

	char* buffer = malloc(20);
		/* memset(buffer, 0, 20);
		buffer = ("Hola, soy yama \n");*/
		//strcpy( mensaje.directorioArchivo, ArchivoOriginalDeYAMAFS);


		send(cliente, "sarasa", 6, 0);
		printf("enviado");

		//send(cliente, ArchivoOriginalDeYAMAFS, 2000, 0);

	//	printf("FS me respondió: %s\n", buffer);
		free(buffer);

	}

	return 0;

}


int main(void) {
	sem_init(&mutex_log,0,1);
	cargarConfiguraciones();
	/*if(conectarConFS()==-1){
		return -1;
	}
*/
	if(pthread_create(&hiloListener,NULL,(void*) socketListenerMaster,NULL)<0){
		log_error(logger, "Falló al crear el hilo para escuchar a Master. (¡Revise Código!)");
	}
	pthread_join(hiloListener,NULL);




	log_info(logger, "<<Proceso YAMA Finalizado>>");


}


