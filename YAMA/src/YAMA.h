/*
 * YAMA.h
 *
 *  Created on: 28/9/2017
 *      Author: utnso
 */

#include <commons/collections/list.h>

#ifndef YAMA_H_
#define YAMA_H_
#define MAX_FILE_SIZE 300


typedef struct{
	char* archivo;
	uint32_t archivos_long;
}  __attribute__((packed)) archivo;

typedef struct{
	uint32_t long_archivo_ruta_origen_YamaFs;
	char* archivo_ruta_origen_YamaFs;
	uint32_t long_archivo_ruta_destino_YamaFs;
	char* archivo_ruta_destino_YamaFs;
	uint32_t total_size;
}  __attribute__((packed)) T_master_yama_start;

typedef struct t_nodo_bloque {
	char* nombre_archivo;
	uint8_t nombre_archivo_long;
	uint16_t nro_bloque_archi;
	uint16_t nro_bloque_archi_long;
	uint8_t id_nodo;
	uint8_t id_nodo_long;
	uint16_t nro_bloque_nodo;
	uint16_t nro_bloque_nodo_long;
	char* ip;
	uint8_t ip_long;
	uint16_t puerto;
	uint16_t puerto_long;
}__attribute__((packed)) t_nodo_bloque;

typedef struct{
	uint8_t id_job;
	t_list* archivosAsociados;
	uint8_t combiner;
	t_list* asociadosDeReduce;
	uint8_t elegidoParaReduce;
	t_list* asociadosDeReduceSinCombiner;
	char* rutaFinal;
	uint8_t rutaFinal_long;
} __attribute__((packed)) t_job;

typedef struct {
	uint8_t id_nodo;
	uint8_t id_nodo_long;
	uint16_t nro_bloque;
	uint16_t nro_bloque_long;
	uint16_t nro_parte_archivo;
	uint16_t nro_parte_archivo_long;
	char* ip;
	uint8_t ip_long;
	uint16_t puerto;
	uint16_t puerto_long;
	char* nombreArchivo;
	uint8_t nombreArchivo_long; //Podría ser más grande
	uint32_t total_size;
	uint8_t desconectado;
 }  __attribute__((packed)) t_nodo;

int recibirYDeserializar(T_master_yama_start *nodoRecibido, int sockN);
void *hiloClienteSocketMaster(int client_sock);
int realizarOperacionMaster(uint8_t master_socket);

#endif /* YAMA_H_ */
