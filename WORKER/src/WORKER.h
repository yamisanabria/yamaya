/*
 * WORKER.h
 *
 *  Created on: 20/11/2017
 *      Author: utnso
 */

#ifndef WORKER_H_
#define WORKER_H_

#include <stdio.h>
#include <stdlib.h>
#include <commons/log.h>
#include <commons/config.h>
#include <sys/stat.h>
#include <pthread.h>
#include <commons/string.h>
#include <commons/collections/list.h>
#include <fcntl.h>
#include <errno.h>
#include <netinet/in.h>
#include <resolv.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <sys/mman.h>
#include <semaphore.h>
#include <netdb.h>

//Informacion propia del nodo
typedef struct {
	char* ip;
	uint8_t ip_long;
	uint16_t puerto;
	uint16_t puerto_long;
	uint8_t nuevo; //0 es nuevo. 1 es existente
	uint8_t nuevo_long;
	uint16_t cant_bloques;
	uint16_t cant_bloques_long;
	uint32_t total_size;
}__attribute__((packed)) t_nodo_fs;

//Informacion de los workers a reducir
typedef struct {
	char* ip_worker;
	uint8_t ip_long;
	uint16_t puerto;
	char* nombre_archivo;
	uint8_t nombre_archivo_long;
	char* dir_temp;
}__attribute__ ((packed)) t_worker_archivo;


void cargarConfiguracion();
void inicializarVariables();
uint32_t tamanioArchivo(FILE* bin);
uint16_t calcularCantidadBloques();
void llenarEspacioDeDatos();
char* getBloque(uint16_t numero_bloque);
char* serializarOperandos(t_nodo_fs* estructuraAMandar);
int8_t sendAll (int8_t socket, char* datos, uint32_t tamanio_datos);
char* mapearAMemoria(char* ruta);
void realizarOperacionMaster(int8_t socket_job);
int8_t recibirYDeserializarMaster(t_worker_archivo* estructura, int8_t socket);
char* concatenarArchivos(t_list* lista_archivos);
int8_t conectarConWorker(char* ip, uint16_t puerto);
uint8_t aplicarRutinaTransformacion(char* script, uint32_t numero_bloque, char* nombre_archivo_tmp,int8_t socket);
uint8_t aplicarRutinaReduccion(char* script, char* concat_archivo, char* nombre_archivo_tmp, int8_t socket);
void permitirEjecucion(char* ruta);
char* pasarAArchivoTemporal(char* script, char* nombre, char* extension, uint32_t tam);
void sortearArchivo(char* ruta_archivo_tmp, char* nombre_archivo);
void liberarRecursos();
char* ContenidoOtroWorker(int8_t socket, char* nombre_archivo);
int AtiendeAMaster(int socket, struct sockaddr_in addr);
int DemasiadosClientes(int socket, struct sockaddr_in addr);
void error(int code, char *err);
void reloj(int loop);
void* hiloClienteSocket(void* socket);
void* crearListenerSocket(void* param);
void ObtenerDatosArchivo(int8_t socket, int8_t* status);
char* getFileContent(char* nombre_archivo);
char* mapearAMemoria(char* ruta);
int8_t sendAll (int8_t socket, char* datos, uint32_t tamanio_datos);
t_list* buscarArchivosWorkers(t_list* lista_archivo_workers,uint8_t codop);
int levantarEscuchasParaMaster();

//void EncontrarArchivoWorker(t_worker_archivo* worker);
#endif /* WORKER_H_ */



