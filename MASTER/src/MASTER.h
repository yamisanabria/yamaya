/*
 * MASTER.h
 *
 *  Created on: 28/9/2017
 *      Author: utnso
 */

#ifndef MASTER_H_
#define MASTER_H_

typedef struct{
	uint32_t long_archivo_ruta_origen_YamaFs;
	char* archivo_ruta_origen_YamaFs;
	uint32_t long_archivo_ruta_destino_YamaFs;
	char* archivo_ruta_destino_YamaFs;
	uint32_t total_size;
}  __attribute__((packed)) T_master_yama_start;




void reloj(int loop);

#endif /* MASTER_H_ */
