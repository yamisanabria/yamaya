/*
 * YAMA.h
 *
 *  Created on: 28/9/2017
 *      Author: utnso
 */

#ifndef YAMA_H_
#define YAMA_H_
#define MAX_FILE_SIZE 300


typedef struct{
	char* archivo;
	uint32_t archivos_long;
}  __attribute__((packed)) archivo;


void *hiloClienteSocketMaster(int client_sock);
int realizarOperacionMaster(uint8_t master_socket);

#endif /* YAMA_H_ */
