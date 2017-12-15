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

t_log* logger;
t_config* configuration;
int configOk=1;

char* ipFS;
int puertoFS;
int retardoPlanificacion;
char* algoritmoBalanceo;
char* ArchivoOriginalDeYAMAFS;

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

int cargarServidorParaEscucharAMaster(){

	struct sockaddr_in direccionServidor;
	direccionServidor.sin_family = AF_INET;
	direccionServidor.sin_addr.s_addr = INADDR_ANY;
	direccionServidor.sin_port = htons(9262);

	int servidor = socket(AF_INET, SOCK_STREAM, 0);

	int activado = 1;
	setsockopt(servidor, SOL_SOCKET, SO_REUSEADDR, &activado, sizeof(activado)); //Para decir al sistema de reusar el puerto

	if (bind(servidor, (void*) &direccionServidor, sizeof(direccionServidor)) != 0) {
		log_error(logger, "Falló el bind. (¿Seguro que no hay dos instancias de YAMA corriendo? ");
		return 1;
	}

	log_info(logger, "YAMA levanto servidor y se encuentra escuchando por puerto %i.",9262);
	listen(servidor, 100);

	//------------------------------

	struct sockaddr_in direccionCliente;
	unsigned int tamanioDireccion = sizeof(struct sockaddr_in);
	int cliente = accept(servidor, (void*) &direccionCliente, &tamanioDireccion);

	log_info(logger, "Master conectado, asignado como el cliente Nro.%i.",cliente);



	//------------------------------

	char* buffer = malloc(1000);

	//while (1) {
		int bytesRecibidos = recv(cliente, buffer, 1000, 0); //En vez de 0 puedo poner MSG_WAITALL que espera que el buffer se llene
		if (bytesRecibidos <= 0) {

			perror("\nMaster se ha desconectado.");
			return 1;
		}

		buffer[bytesRecibidos] = '\0';
		printf("Recibido de Master %s\n", buffer);
		send(cliente, "Hola Master!, conectate al nodo 1 en ip 127.0.0.1\n", 1000, 0);
//}

	free(buffer);
 return 0;
}

int conectarAFiliSystem(){
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
	//while (1) {
		//sleep(3);

		char* buffer = malloc(1000);
		//strcpy( mensaje.directorioArchivo, ArchivoOriginalDeYAMAFS);
		//send(cliente, mensaje, sizeof(mensaje), 0);
		send(cliente, ArchivoOriginalDeYAMAFS, 1000, 0);
		recv(cliente, buffer, 1000, 0);
		printf("FS me respondió: %s\n", buffer);
		free(buffer);

	//}

	return 0;

}


int main(void) {
	cargarConfiguraciones();
	//printf("Valor recibido %s \n", argv[1]);
	conectarAFiliSystem();
	cargarServidorParaEscucharAMaster();


	log_info(logger, "<<Proceso YAMA Finalizado>>");


}
