/*
 * yama.h
 *
 *  Created on: 29/9/2017
 *      Author: utnso
 */

#ifndef YAMA_H_
#define YAMA_H_

#include <stdio.h>
#include <stdlib.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/temporal.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <commons/collections/list.h>

#define YAMA 200
#define FILESYSTEM 201
#define WORKER 202
#define DATANODE 203
#define MASTER 204

#define HANDSHAKE 100
#define HANDSHAKEOK 101
#define MAXDATASIZE 1024
#define SIZE_MSG sizeof(t_mensaje)

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
	int indice;
	int puerto;
	char ip[16];
	int estado; //1 estuvo en el sistema, 0 no estuvo en el sistema
	int cantidad_maxima_bloques;
} t_datanode;

#endif /* YAMA_H_ */
