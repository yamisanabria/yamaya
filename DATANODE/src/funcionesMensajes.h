/*
 * funcionesMensajes.h
 *
 *  Created on: 15/9/2017
 *      Author: utnso
 */

#ifndef FUNCIONESMENSAJES_H_
#define FUNCIONESMENSAJES_H_

#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>//socket
#include <arpa/inet.h> //socket
#include <commons/log.h> // log_create, log_info, log_error
#include <commons/collections/list.h>


#define FILESYSTEM 201
#define NODO 203

#define HANDSHAKE 100
#define HANDSHAKEOK 101
#define MAXDATASIZE 1024
#define SIZE_MSG sizeof(t_mensaje)

int enviar_saludo(int id_origen, int sock, t_log* logger,int tipo_mensaje);
int recibir_saludo(int id_destino, int sock, t_log* logger,int tipo_mensaje);

typedef struct {
	int tipo;
	int id_proceso;
	int datosNumericos;
	char mensaje[16];
} t_mensaje;


#endif /* FUNCIONESMENSAJES_H_ */
