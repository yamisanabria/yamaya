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
#include <netdb.h>
#include <commons/collections/dictionary.h>
#include <semaphore.h>
#include <sys/mman.h>


#define FILESYSTEM 201
#define DATANODE 203

#define HANDSHAKE 100
#define HANDSHAKEOK 101
#define MAXDATASIZE 1024
#define SIZE_MSG sizeof(t_mensaje)
#define GET_BLOQUE 1
#define SET_BLOQUE 2

#define TAMANIO_BLOQUE 1048576 //1mb

t_log* logger;
t_config* configuration;
//int puertoFS;
int puertoFS;
char ipFS[16];
char* nombreNodo;
char* rutaDataBin;
int configOk=1;
int sockFS;
int puertoWorker;
char ip_nodo_actual[16];
int nodo_estado;
int id_nodo;
int cant_max_bloques;
char* ArchivoOriginalDeData="HOLA";
char* dir_temp;
char* arch_bin;
int tam_dataBin;
char* mapeo;
uint32_t tamanio_espacio_datos;
pthread_t thEscucharFS;

sem_t mutex_log;

typedef struct {
	int tipo;
	int id_proceso;
	int datosNumericos;
	char mensaje[16];
} t_mensaje;

typedef struct {
	uint16_t nombreNodo_long;
	char* nombreNodo;
	uint16_t puertoNodo_long;
	uint16_t puertoNodo;
	uint8_t ipNodo_long;
	char*ipNodo;
	uint16_t cantidad_bloques_long;
	uint16_t cantidad_bloques;
	uint32_t total_size;
}__attribute__((packed)) t_datanode;

typedef struct {
	uint8_t codigo_long;
	uint8_t codigo_operacion;
	uint16_t numero_bloque_long;
	uint16_t numero_bloque;
	uint32_t tam_datos;
	char* datos;
	uint32_t tamanio_datos;
}__attribute__((packed)) t_datos;

void inicializar ();
void enviarDatos_FS ();
char* serializarEstructura(t_datanode* estructuraAMandar);
void dispose_package(char **paquete);
void* crearListenerFS(void* param);
void realizarOperacionFS(int socket, int8_t* estado);
int enviar_saludo(int id_origen, int sock, t_log* logger,int tipo_mensaje);
int recibir_saludo(int id_destino, int sock, t_log* logger,int tipo_mensaje);
void validarDesconexionFS(int8_t* status);
uint8_t setBloque(uint16_t numero_bloque, char* datos);
char* mapearAMemoria(char* RutaDelArchivo);
int tamanioArchivo(FILE* bin);
int recibirYDeserializar(t_datos *datosRecibidos, int sockFS);

#endif /* FUNCIONESMENSAJES_H_ */
