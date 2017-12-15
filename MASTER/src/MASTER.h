/*
 * MASTER.h
 *
 *  Created on: 28/9/2017
 *      Author: utnso
 */

#ifndef MASTER_H_
#define MASTER_H_

#include <stdio.h>
#include <stdint.h>
#include <commons/config.h>
#include <commons/collections/list.h>

typedef struct{
	uint32_t long_archivo_ruta_origen_YamaFs;
	char archivo_ruta_origen_YamaFs[255];
	uint32_t long_archivo_ruta_destino_YamaFs;
	char archivo_ruta_destino_YamaFs[255];
	uint32_t total_size;
}  __attribute__((packed)) T_master_yama_start;

typedef struct{
	uint16_t nro_bloque_long;
	uint16_t nro_bloque;
	uint8_t ip_long;
	char* ip;
	uint16_t puerto_long;
	uint16_t puerto;
	uint8_t nombreArchivo_long; //Podría ser más grande
	char* nombreArchivo;
	uint8_t total_size;
}  __attribute__((packed)) t_worker_para_transformacion;

typedef struct{
	char* ip_nodo;
	uint8_t ip_long;
	uint16_t puerto;
	uint16_t puerto_long;
	char* archivoTemporalFinal;
	uint8_t archivoTemporalFinal_long;
	uint8_t cantElementos;
	t_list* nodosAReducir;
	} __attribute__((packed)) t_worker_para_reduccion_local;


typedef struct{
	char* ip_nodo;
	uint8_t ip_long;
	uint16_t puerto;
	uint16_t puerto_long;
	char* nombreArchivo;
	uint8_t nombreArchivo_long;
	uint8_t total_size;
	//agregar estado TODO
} __attribute__((packed)) t_nodos_a_reducir;

void* hiloMasterNodoTransformacion(void* workerTransformacion);
void* hiloMasternodoReduccion(void* nodoReduccion);
uint32_t tamanioArchivoFD(FILE* bin);
int8_t sendAll (int8_t socket, char* datos, uint32_t tamanio_datos);
int8_t sendAll8 (int8_t socket, char* datos, uint8_t tamanio_datos);
void reloj(int loop);
int conectarConWorker(char* ipWorker,uint16_t puertoWorker);
void realizarOperacionYAMA();
pthread_t thr_worker_transformacion;
pthread_t thr_worker_reduccion;
#endif /* MASTER_H_ */
