/*
 ============================================================================
 Name        : DATANODE.c
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
#include "funcionesMensajes.h"
char* ArchivoOriginalDeData="HOLA";

t_log* logger;
t_config* configuration;
char* puertoFS;
//iuint16_t puertoFS;
char* ipFS;
char* nombreNodo;
char* rutaDataBin;
int configOk=1;


void cargarConfiguraciones() {
	logger = log_create("logDataNode", "DataNode LOG", true, LOG_LEVEL_DEBUG);
	log_info(logger, "<<Proceso DataNode inició>>");
	configuration = config_create("/home/utnso/workspace/tp-2017-2c-Yamaya/CONFIG_NODO");
	log_info(logger, "Intentando levantar el archivo de configuraciones.");
		if(configuration==NULL){
				log_error(logger, "Error el archivo de configuraciones no existe.");
				exit(-1);
		}


	if (config_has_property(configuration, "IP_FILESYSTEM")) {

			ipFS = config_get_string_value(configuration, "IP_FILESYSTEM");

			log_info(logger, "La IP del FileSystem es: %s", ipFS);

	} else {

			log_error(logger, "Error al obtener la IP del FileSystem");

			configOk = 0;
	}

	if (config_has_property(configuration, "PUERTO_FILESYSTEM")) {

			puertoFS = config_get_string_value(configuration, "PUERTO_FILESYSTEM");

			log_info(logger, "El puerto del FileSystem es: %s", puertoFS);

	} else {



			log_error(logger, "Error al obtener el puerto del FileSystem");

			configOk = 0;
	}

	if (config_has_property(configuration, "NOMBRE_NODO")) {

			nombreNodo = config_get_string_value(configuration, "NOMBRE_NODO");

			log_info(logger, "El nombre del Nodo es: %s", nombreNodo);

		} else {

			log_error(logger, "Error al obtener el Nombre del Nodo");

			configOk = 0;
	}
	if (config_has_property(configuration, "RUTA_DATABIN")) {

			rutaDataBin = config_get_string_value(configuration, "RUTA_DATABIN");

			log_info(logger, "La ruta del Data.Bin es: %s", rutaDataBin);

		} else {

			log_error(logger, "Error al obtener la ruta del Data.Bin");

			configOk = 0;
	}
	if (!configOk) {
		log_error(logger, "Debido a errores en las configuraciones, se aborta la ejecución... (REVISE ARCH. CONFIGURACIONES)");
		exit(-1);
	}


}

int conectarAFS(){
	log_info(logger, "Intentando levantar conexión con YAMA.");
	struct sockaddr_in direccionServidor;
		direccionServidor.sin_family = AF_INET;
		direccionServidor.sin_addr.s_addr = inet_addr(ipFS);
		direccionServidor.sin_port = htons(1234);


	int cliente = socket(AF_INET, SOCK_STREAM, 0);

	if (connect(cliente, (void*) &direccionServidor, sizeof(direccionServidor)) != 0) {
		log_error(logger, "Problema al levantar conexión con FS en IP:%s PUERTO:%i (¿FS se encuentra levantado?)",ipFS,1234);

		return -1;
	}

	log_info(logger,"Conexión establecida con FS.");
	//while (1) {
		//sleep(3);

		char* buffer = malloc(1000);
		//strcpy( mensaje.directorioArchivo, ArchivoOriginalDeYAMAFS);
		//send(cliente, mensaje, sizeof(mensaje), 0);
		send(cliente, ArchivoOriginalDeData, 1000, 0);
		recv(cliente, buffer, 1000, 0);
		printf("DataNode me respondió: %s\n", buffer);
		free(buffer);

	//}

	return 0;

}


int main(void) {

		cargarConfiguraciones();
		conectarAFS();
		//log_info(logger, "<<Proceso DataNode finalizó>>");
		//return EXIT_SUCCESS;
}


