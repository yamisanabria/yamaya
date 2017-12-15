/*
 ============================================================================
 Name        : MASTER.c
 Author      : yo
 Version     :
 Copyright   : Grupo YAMAYA
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/temporal.h>
#include <stdint.h>
#include <pthread.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h> //SLEEP FUNCTION
#include <semaphore.h>
#include "MASTER.h"
#define CodOP_Master_Solic_Nuevo_Trab 0
#define CANTIDADARGUMENTOS 5 //El primer argumento es el programa... (PARAMETROS RECIBIDOS AL EJECUTAR)
#define BUFFERSIZE 512 /** Longitud del buffer  */

t_log* logger;
t_config* configuration;
char* ipYAMA;
char* ipWorker="127.0.0.1";
uint16_t puertoWorker = 7000;
uint16_t puertoYAMA;
char* puertoMaster;
int configOk=1;
uint8_t FSSocket;

char* ScriptTransformador;
char* ScriptReductor;
char* ArchivoOriginalDeYAMAFS;
char* ArchivoDestinoEnYAMAFS;
sem_t mutex_log;
uint8_t yamaSocket;

void cargarConfiguraciones() {
	logger = log_create("logMaster", "MASTER LOG", true, LOG_LEVEL_DEBUG);
	log_debug(logger, "<<Proceso Master iniciando>> (CARGANDO CONFIGURACIONES)");
	configuration = config_create("CONFIG_MASTER");
	if(configuration==NULL){
			log_error(logger, "Verifique el archivo de configuracion de entrada. (¿Este existe?)");
			exit(-1);
		}

	if (config_has_property(configuration, "YAMA_IP")) {

			ipYAMA = config_get_string_value(configuration, "YAMA_IP");


		} else {
			log_error(logger, "Error al obtener la IP de YAMA");

			configOk = 0;
		}

		if (config_has_property(configuration, "YAMA_PUERTO")) {

			puertoYAMA = config_get_int_value(configuration, "YAMA_PUERTO");


		} else {
			log_error(logger, "Error al obtener el puerto de YAMA");

			configOk = 0;
		}



		if (!configOk) {
				log_error(logger, "Debido a errores en las configuraciones, se aborta la ejecución... (REVISE ARCH. CONFIGURACIONES)");
				exit(-1);

			}
		log_debug(logger, "La IP de YAMA es: %s, su puerto: %i", ipYAMA, puertoYAMA);

}

uint8_t conectarConYama(){
	uint8_t yamaSocket;
	sem_wait(&mutex_log);
	log_debug(logger, "Intentando conexión con YAMA.");
	sem_post(&mutex_log);

	struct sockaddr_in direccionServidor;
		direccionServidor.sin_family = AF_INET;
		direccionServidor.sin_addr.s_addr = inet_addr(ipYAMA);
		direccionServidor.sin_port = htons(puertoYAMA);


	yamaSocket = socket(AF_INET, SOCK_STREAM, 0);

	if (connect(yamaSocket, (void*) &direccionServidor, sizeof(direccionServidor)) != 0) {
		sem_wait(&mutex_log);
		log_error(logger, "Problema al levantar conexión con YAMA en IP:%s PUERTO:%i (¿YAMA se encuentra levantado?)",ipYAMA,puertoYAMA);
		sem_post(&mutex_log);
		exit(-1);
	}
	sem_wait(&mutex_log);
	log_info(logger,"Conexión establecida con YAMA.");
	sem_post(&mutex_log);

	return yamaSocket;

}

char* serializarEnvioArchivo(T_master_yama_start* estructuraAMandar){

	 char *serializedPackage = malloc(estructuraAMandar->total_size);
	 //memset(serializedPackage, 0, sizeof(&serializedPackage));
	 int offset = 0;
	 int size_to_send;

	//Empaquetamos el tamaño del path del archivo
 	 size_to_send =  sizeof(estructuraAMandar->long_archivo_ruta_origen_YamaFs);
 	 memcpy(serializedPackage + offset, &(estructuraAMandar->long_archivo_ruta_origen_YamaFs), size_to_send);
 	 offset += size_to_send;
 	printf("Enviado long_archivo_ruta_origen_YamaFs: %u ", estructuraAMandar->long_archivo_ruta_origen_YamaFs);

 	 //Ahora empaquetamos el path del archivo origen YAMA FS
 	 size_to_send=estructuraAMandar->long_archivo_ruta_origen_YamaFs;
 	 memcpy(serializedPackage + offset, estructuraAMandar->archivo_ruta_origen_YamaFs, size_to_send);
 	 offset += size_to_send;
 	printf("Enviado archivo_ruta_origen_YamaFs: %s ", estructuraAMandar->archivo_ruta_origen_YamaFs);

 	 //Enviamos tamaño del path destino
 	 size_to_send=sizeof(estructuraAMandar->long_archivo_ruta_destino_YamaFs);
 	 memcpy(serializedPackage + offset, &(estructuraAMandar->long_archivo_ruta_destino_YamaFs), size_to_send);
 	 offset += size_to_send;
 	printf("Enviado long_archivo_ruta_destino_YamaFs: %u ", estructuraAMandar->long_archivo_ruta_destino_YamaFs);

 	 //Ahora empaquetamos el path del archivo destino YAMA FS
 	 size_to_send=estructuraAMandar->long_archivo_ruta_destino_YamaFs;
 	 memcpy(serializedPackage + offset, estructuraAMandar->archivo_ruta_destino_YamaFs, size_to_send);
 	 offset += size_to_send;
 	printf("Enviado archivo_ruta_destino_YamaFs: %s \n", estructuraAMandar->archivo_ruta_destino_YamaFs);



 return serializedPackage;
}

void enviarArchivoAYama(){
	uint8_t codop;
	T_master_yama_start estructuraAMandar;
	yamaSocket=conectarConYama();
	codop = CodOP_Master_Solic_Nuevo_Trab;

	if(send(yamaSocket,&(codop),sizeof(uint8_t),0)==-1){
		sem_wait(&mutex_log);
		log_error(logger, "Error envio de CODOP=%i. (¡Revise código!)",codop);
		sem_post(&mutex_log);
	} //Envio primero el Codigo de operacion para que YAMA pueda usar un Switch.
	sem_wait(&mutex_log);
	log_info(logger, "Enviado CODOP=%i",codop);
	sem_post(&mutex_log);

	 estructuraAMandar.archivo_ruta_origen_YamaFs=malloc(sizeof(ArchivoOriginalDeYAMAFS));
	 estructuraAMandar.archivo_ruta_origen_YamaFs=ArchivoOriginalDeYAMAFS;

	 estructuraAMandar.long_archivo_ruta_origen_YamaFs=strlen(ArchivoOriginalDeYAMAFS) + 1; //Se le suma +1 debido a que strlen no cuenta el '\0'


	 estructuraAMandar.long_archivo_ruta_destino_YamaFs=strlen(ArchivoDestinoEnYAMAFS) + 1; //Se le suma +1 debido a que strlen no cuenta el '\0'

	 estructuraAMandar.archivo_ruta_destino_YamaFs=malloc(sizeof(ArchivoDestinoEnYAMAFS));
	 estructuraAMandar.archivo_ruta_destino_YamaFs=ArchivoDestinoEnYAMAFS;

	 estructuraAMandar.total_size=sizeof(estructuraAMandar.long_archivo_ruta_origen_YamaFs)+estructuraAMandar.long_archivo_ruta_origen_YamaFs + sizeof(estructuraAMandar.long_archivo_ruta_destino_YamaFs)+ estructuraAMandar.long_archivo_ruta_destino_YamaFs;
	 //TODO Revisar el conteo de este sizeof

	char *buffer=serializarEnvioArchivo(&estructuraAMandar); //Recibo el paquete all armado y preparado para el envio

	if(send(yamaSocket,buffer, estructuraAMandar.total_size,0)==-1){
		sem_wait(&mutex_log);
		log_error(logger, "Error envio de BUFFER");
		sem_post(&mutex_log);
	} //Le envio la longitud de la ruta de origen del archivo en YAMA FS




	sem_wait(&mutex_log);
	log_info(logger, "Finalizado enviarArchivoAYama",ArchivoOriginalDeYAMAFS);
	sem_post(&mutex_log);



	int esperoRespuesta=1;
	int loop=0;
	while(esperoRespuesta){

		reloj(loop);
		int bytesRecibidos = recv(yamaSocket,buffer,BUFFERSIZE,0);
		if(bytesRecibidos==0){
			log_error(logger,"Perdimos conexión con YAMA");
			perror("PError-> ");
			exit(-1);
		}else if(bytesRecibidos>0){
			esperoRespuesta=0;

			//TODO Luego de la repuesta por parte de YAMA realizar la conexión al Worker para trabajar...
		}
		loop++;
	}
}

int conectarConWorker(){


	log_debug(logger, "Intentando levantar conexión con el WORKER.");
		struct sockaddr_in direccionServidorWorker;
			direccionServidorWorker.sin_family = AF_INET;
			direccionServidorWorker.sin_addr.s_addr = inet_addr(ipWorker);
			direccionServidorWorker.sin_port = htons(puertoWorker);


		int cliente = socket(AF_INET, SOCK_STREAM, 0);

		if (connect(cliente, (void*) &direccionServidorWorker, sizeof(direccionServidorWorker)) != 0) {
			log_error(logger, "Problema al levantar conexión con WORKER en IP:%s PUERTO:%i (¿WORKER se encuentra levantado?)",ipWorker,puertoWorker);

			return -1;
		}

		log_debug(logger,"Conexión establecida con el WORKER.");
		//while (1) {
			//sleep(3);

			char* buffer = malloc(1000);
			//strcpy( mensaje.directorioArchivo, ArchivoOriginalDeYAMAFS);
			//send(cliente, mensaje, sizeof(mensaje), 0);
			send(cliente, "EXIT", 1000, 0);

			memset(buffer, 0, 1000);
			recv(cliente, buffer, 1000, 0);

			printf("Respuesta del Worker: %s\n", buffer);
			free(buffer);


		//}
		return 0;
}

int main(int argc, char *argv[]) {
	/* 	//Para calcular tiempo de ejecucion (METRICAS).
		#include <sys/time.h>
	clock_t start2 = clock();
	struct timeval stop, start;
	gettimeofday(&start, NULL);
	sleep(5);//Mi trabajo
	gettimeofday(&stop, NULL);
	printf("took %lu microseconds\n", stop.tv_usec - start.tv_usec);
    printf("Tiempo transcurrido: %f segundos", ((double)clock() - start2) / CLOCKS_PER_SEC);
		 */

	cargarConfiguraciones();
	sem_init(&mutex_log,0,1);
	if(argc!=CANTIDADARGUMENTOS) {
	log_error(logger, "Cantidad de argumentos recibidos incorrectos, se aborta la ejecución. (Se esperan %i se recibieron %i)",CANTIDADARGUMENTOS-1,argc-1);
	return EXIT_FAILURE;
	}else{
		ScriptTransformador = argv[1];
		ScriptReductor = argv[2];
		ArchivoOriginalDeYAMAFS = argv[3];
		ArchivoDestinoEnYAMAFS = argv[4];
	log_debug(logger, "Datos ingresados por parametros son: ScriptTransformador= \"%s\" ScriptReductor= \"%s\" ArchivoOriginalDeYAMAFS= \"%s\" ArchivoDestinoEnYAMAFS= \"%s\" ",ScriptTransformador,ScriptReductor,ArchivoOriginalDeYAMAFS,ArchivoDestinoEnYAMAFS);

	}
	enviarArchivoAYama();

	if(conectarConWorker()==-1){
		log_error(logger, "Falló al intentar conectar al WORKER, aborta ejecución.");
		return EXIT_FAILURE;
  }
	return EXIT_SUCCESS;
}


void reloj(int loop)
{
	sleep(1);
  if (loop==0)
    log_info(logger,"[MASTER] Esperando respuesta \n ");

 printf("\033[1D");        /* Introducimos código ANSI para retroceder 2 caracteres */
  switch (loop%4)
    {
    case 0: printf("|"); break;
    case 1: printf("/"); break;
    case 2: printf("-"); break;
    case 3: printf("\\"); break;
    default:            /* No debemos estar aquí */
      break;
    }

  fflush(stdout);       /* Actualizamos la pantalla */
}
