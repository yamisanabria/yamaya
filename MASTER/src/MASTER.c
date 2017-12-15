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

t_log* logger;
t_config* configuration;
char* ipYAMA;
char* ipWorker="127.0.0.1";
uint16_t puertoWorker = 7000;
uint16_t puertoYAMA;
char* puertoMaster;
int configOk=1;
uint8_t FSSocket;
#define CANTIDADARGUMENTOS 5 //El primer argumento es el programa... (PARAMETROS RECIBIDOS AL EJECUTAR)
char* ScriptTransformador;
char* ScriptReductor;
char* ArchivoOriginalDeYAMAFS;
char* ArchivoDestinoEnYAMAFS;


void cargarConfiguraciones() {
	logger = log_create("logMaster", "MASTER LOG", true, LOG_LEVEL_DEBUG);
	log_info(logger, "<<Proceso Master iniciando>> (CARGANDO CONFIGURACIONES)");
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
		log_info(logger, "La IP de YAMA es: %s, su puerto: %i", ipYAMA, puertoYAMA);

}

int conectarAYama(){
	log_info(logger, "Intentando levantar conexión con YAMA.");
	struct sockaddr_in direccionServidor;
		direccionServidor.sin_family = AF_INET;
		direccionServidor.sin_addr.s_addr = inet_addr(ipYAMA);
		direccionServidor.sin_port = htons(puertoYAMA);


	int cliente = socket(AF_INET, SOCK_STREAM, 0);

	if (connect(cliente, (void*) &direccionServidor, sizeof(direccionServidor)) != 0) {
		log_error(logger, "Problema al levantar conexión con YAMA en IP:%s PUERTO:%i (¿YAMA se encuentra levantado?)",ipYAMA,puertoYAMA);

		return -1;
	}

	log_info(logger,"Conexión establecida con YAMA.");
	//while (1) {
		//sleep(3);

		char* buffer = malloc(1000);
		//strcpy( mensaje.directorioArchivo, ArchivoOriginalDeYAMAFS);
		//send(cliente, mensaje, sizeof(mensaje), 0);
		send(cliente, ArchivoOriginalDeYAMAFS, 1000, 0);
		recv(cliente, buffer, 1000, 0);
		printf("YAMA me respondió: %s\n", buffer);
		free(buffer);

	//}

	return 0;

}

int conectarConElWorker(){


	log_info(logger, "Intentando levantar conexión con el WORKER.");
		struct sockaddr_in direccionServidorWorker;
			direccionServidorWorker.sin_family = AF_INET;
			direccionServidorWorker.sin_addr.s_addr = inet_addr(ipWorker);
			direccionServidorWorker.sin_port = htons(puertoWorker);


		int cliente = socket(AF_INET, SOCK_STREAM, 0);

		if (connect(cliente, (void*) &direccionServidorWorker, sizeof(direccionServidorWorker)) != 0) {
			log_error(logger, "Problema al levantar conexión con WORKER en IP:%s PUERTO:%i (¿WORKER se encuentra levantado?)",ipWorker,puertoWorker);

			return -1;
		}

		log_info(logger,"Conexión establecida con el WORKER.");
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
	//printf("Valor recibido %s \n", argv[1]);
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
	if(conectarAYama()==-1){
		log_error(logger, "Fallo al intentar conectar a YAMA, aborta ejecución.");
		return EXIT_FAILURE;
	}
	/*if(conectarConElWorker()==-1){
		log_error(logger, "Fallo al intentar conectar al WORKER, aborta ejecución.");
		return EXIT_FAILURE;
  }*/
}
