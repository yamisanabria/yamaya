/*
 * funcionesMensajes.h
 *
 *  Created on: 15/9/2017
 *      Author: utnso
 */

#ifndef FUNCIONESMENSAJES_H_
#define FUNCIONESMENSAJES_H_

#include <netinet/in.h>
#include <commons/string.h>
#include <string.h>
#include <sys/socket.h>//socket
#include <arpa/inet.h> //socket
#include <commons/log.h> // log_create, log_info, log_error
#include <commons/collections/list.h>
#include <stdio.h>
#include <stdlib.h>
#include <commons/config.h>
#include <commons/temporal.h>
#include <stdint.h>
#include <pthread.h>
#include <unistd.h> //SLEEP FUNCTION
#include <sys/stat.h> //tamaño archivo


#define FILESYSTEM 201
#define DATANODE 203

#define HANDSHAKE 100
#define HANDSHAKEOK 101
#define MAXDATASIZE 1024
#define SIZE_MSG sizeof(t_mensaje)

t_log* logger;
t_config* configuration;
//int puertoFS;
int puertoFS;
char ipFS[16];
char* nombreNodo;
char* rutaDataBin;
int configOk=1;
int sockFS;
int puerto_nodo_actual;
char ip_nodo_actual[16];
int nodo_estado;
int id_nodo;
int puertoWorker;
char* ArchivoOriginalDeData="HOLA";
char* dir_temp;
char* arch_bin;


int enviar_saludo(int id_origen, int sock, t_log* logger,int tipo_mensaje);
int recibir_saludo(int id_destino, int sock, t_log* logger,int tipo_mensaje);
void atender_FS (void* param);

typedef struct {
	int tipo;
	int id_proceso;
	int datosNumericos;
	char mensaje[16];
} t_mensaje;

typedef struct {
	char id[16];
	int puerto;
	char ip[16];
	int cantidad_maxima_bloques;
} t_datanode;

#endif /* FUNCIONESMENSAJES_H_ */
