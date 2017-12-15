/*
 ============================================================================
 Name        : MASTER.c
 Author      : yo
 Version     :
 Copyright   : Grupo YAMAYA
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/temporal.h>
#include <stdint.h>
#include <pthread.h>
#include <string.h>
#include <commons/string.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h> //SLEEP FUNCTION
#include <semaphore.h>
#include "MASTER.h"
#include <sys/stat.h>
#define CodOP_Master_Solic_Nuevo_Trab 0
#define CANTIDADARGUMENTOS 5 //El primer argumento es el programa... (PARAMETROS RECIBIDOS AL EJECUTAR)
#define BUFFERSIZE 512 /** Longitud del buffer  */

t_log* logger;
t_config* configuration;
char* ipYAMA;
char* ipWorker="127.0.0.1";
uint16_t puertoWorker = 7000;
uint16_t puertoYAMA;
char* puertoMaster;
int configOk=1;
uint8_t FSSocket;
char* transformador;
char* reductor;
char* ScriptTransformador;
char* ScriptReductor;
char* ArchivoOriginalDeYAMAFS;
char* ArchivoDestinoEnYAMAFS;
sem_t mutex_log;
uint8_t yamaSocket;
sem_t mutex_yamaSocket;

#define ID_PROC_YAMA 1
#define ID_PROC_WORKER 3
#define ID_PROC_MASTER 4

#define CODOP_YAMA_RECIBIRWORKERTRANSFORMACION 3
#define CODOP_YAMA_RECIBIRWORKERREDUCCION_LOCAL 4
#define CODOP_YAMA_RECIBIRWORKERREDUCCION_GLOBAL 5

#define CODOP_MASTER_TRANSFORMACIONTERMINADO 2
#define CODOP_MASTER_REDUCCIONTERMINADO 5

#define CODOP_WORKER_TRANSFORMACION 4
#define CODOP_WORKER_REDUCCION_LOCAL 5
#define CODOP_WORKER_REDUCCION_GLOBAL 6

void cargarConfiguraciones() {
	logger = log_create("logMaster", "MASTER LOG", true, LOG_LEVEL_DEBUG);
	log_debug(logger, "<<Proceso Master iniciando>> (CARGANDO CONFIGURACIONES)");
	configuration = config_create("CONFIG_MASTER");
	if(configuration==NULL){
			log_error(logger, "Verifique el archivo de configuracion de entrada. (¿Este existe?)");
			exit(-1);
		}

	if (config_has_property(configuration, "YAMA_IP")) {

			ipYAMA = config_get_string_value(configuration, "YAMA_IP");


		} else {
			log_error(logger, "Error al obtener la IP de YAMA");

			configOk = 0;
		}

		if (config_has_property(configuration, "YAMA_PUERTO")) {

			puertoYAMA = config_get_int_value(configuration, "YAMA_PUERTO");


		} else {
			log_error(logger, "Error al obtener el puerto de YAMA");

			configOk = 0;
		}



		if (!configOk) {
				log_error(logger, "Debido a errores en las configuraciones, se aborta la ejecución... (REVISE ARCH. CONFIGURACIONES)");
				exit(-1);

			}
		log_debug(logger, "La IP de YAMA es: %s, su puerto: %i", ipYAMA, puertoYAMA);

}

uint8_t conectarConYama(){
	uint8_t yamaSocket;
	sem_wait(&mutex_log);
	log_debug(logger, "Intentando conexión con YAMA.");
	sem_post(&mutex_log);

	struct sockaddr_in direccionServidor;
		direccionServidor.sin_family = AF_INET;
		direccionServidor.sin_addr.s_addr = inet_addr(ipYAMA);
		direccionServidor.sin_port = htons(puertoYAMA);


	yamaSocket = socket(AF_INET, SOCK_STREAM, 0);

	if (connect(yamaSocket, (void*) &direccionServidor, sizeof(direccionServidor)) != 0) {
		sem_wait(&mutex_log);
		log_error(logger, "Problema al levantar conexión con YAMA en IP:%s PUERTO:%i (¿YAMA se encuentra levantado?)",ipYAMA,puertoYAMA);
		sem_post(&mutex_log);
		exit(-1);
	}
	sem_wait(&mutex_log);
	log_info(logger,"Conexión establecida con YAMA.");
	sem_post(&mutex_log);

	return yamaSocket;

}


char* serializarEnvioArchivo(T_master_yama_start* estructuraAMandar){

	 char *serializedPackage = malloc(estructuraAMandar->total_size);
	 //memset(serializedPackage, 0, sizeof(&serializedPackage));
	 int offset = 0;
	 int size_to_send;

	//Empaquetamos el tamaño del path del archivo
 	 size_to_send =  sizeof(estructuraAMandar->long_archivo_ruta_origen_YamaFs);
 	 memcpy(serializedPackage + offset, &(estructuraAMandar->long_archivo_ruta_origen_YamaFs), size_to_send);
 	 offset += size_to_send;
 	 printf("IMprimo: %s", serializedPackage);

 	 //Ahora empaquetamos el path del archivo origen YAMA FS
 	 size_to_send=estructuraAMandar->long_archivo_ruta_origen_YamaFs;
 	 memcpy(serializedPackage + offset, &(estructuraAMandar->archivo_ruta_origen_YamaFs), size_to_send);
 	 offset += size_to_send;
 	printf("IMprimo2: %s", serializedPackage);

 	 //Enviamos tamaño del path destino
 	 size_to_send=sizeof(estructuraAMandar->long_archivo_ruta_destino_YamaFs);
 	 memcpy(serializedPackage + offset, &(estructuraAMandar->long_archivo_ruta_destino_YamaFs), size_to_send);
 	 offset += size_to_send;
 	printf("IMprimo3: %s", serializedPackage);

 	 //Ahora empaquetamos el path del archivo destino YAMA FS
 	 size_to_send=estructuraAMandar->long_archivo_ruta_destino_YamaFs;
 	 memcpy(serializedPackage + offset, &(estructuraAMandar->archivo_ruta_destino_YamaFs), size_to_send);
 	 offset += size_to_send;


printf("Estamos devolviendo: '%s'...",serializedPackage);
 return serializedPackage;
}

void enviarArchivoAYama(){
	uint8_t codop;
	T_master_yama_start estructuraAMandar;
	yamaSocket=conectarConYama();
	codop = CodOP_Master_Solic_Nuevo_Trab;

	if(send(yamaSocket,&(codop),sizeof(uint8_t),0)==-1){
		sem_wait(&mutex_log);
		log_error(logger, "Error envio de CODOP=%i. (¡Revise código!)",codop);
		sem_post(&mutex_log);
	} //Envio primero el Codigo de operacion para que YAMA pueda usar un Switch.
	sem_wait(&mutex_log);
	log_info(logger, "Enviado CODOP=%i",codop);
	sem_post(&mutex_log);


	 estructuraAMandar.long_archivo_ruta_origen_YamaFs=strlen(ArchivoOriginalDeYAMAFS) + 1; //Se le suma +1 debido a que strlen no cuenta el '\0'
	 strcpy(estructuraAMandar.archivo_ruta_origen_YamaFs, ArchivoOriginalDeYAMAFS);
	 estructuraAMandar.long_archivo_ruta_destino_YamaFs=strlen(ArchivoDestinoEnYAMAFS) + 1; //Se le suma +1 debido a que strlen no cuenta el '\0'
	 strcpy(estructuraAMandar.archivo_ruta_destino_YamaFs, ArchivoDestinoEnYAMAFS);

	 estructuraAMandar.total_size=sizeof(estructuraAMandar.long_archivo_ruta_origen_YamaFs)+estructuraAMandar.long_archivo_ruta_origen_YamaFs + sizeof(estructuraAMandar.long_archivo_ruta_destino_YamaFs)+ estructuraAMandar.long_archivo_ruta_destino_YamaFs;
	 //TODO Revisar el conteo de este sizeof

	char *buffer=serializarEnvioArchivo(&estructuraAMandar); //Recibo el paquete all armado y preparado para el envio

	if(send(yamaSocket,buffer, 255,0)==-1){
		sem_wait(&mutex_log);
		log_error(logger, "Error envio de BUFFER");
		sem_post(&mutex_log);
	} //Le envio la longitud de la ruta de origen del archivo en YAMA FS

	sem_wait(&mutex_log);
	log_info(logger, "Finalizado enviarArchivoAYama",ArchivoOriginalDeYAMAFS);
	sem_post(&mutex_log);

	int esperoRespuesta=1;
	int loop=0;
	while(esperoRespuesta){

		reloj(loop);
		int bytesRecibidos = recv(yamaSocket,buffer,BUFFERSIZE,0);
		if(bytesRecibidos==0){
			log_error(logger,"Perdimos conexión con YAMA");
			perror("PError-> ");
			exit(-1);
		}else if(bytesRecibidos>0){
			esperoRespuesta=0;

			//TODO Luego de la repuesta por parte de YAMA realizar la conexión al Worker para trabajar...
		}
		loop++;
	}
}

int conectarConWorker(char* ipWorker,uint16_t puertoWorker){


	log_debug(logger, "Intentando levantar conexión con el WORKER.");
		struct sockaddr_in direccionServidorWorker;
			direccionServidorWorker.sin_family = AF_INET;
			direccionServidorWorker.sin_addr.s_addr = inet_addr(ipWorker);
			direccionServidorWorker.sin_port = htons(puertoWorker);

		int cliente = socket(AF_INET, SOCK_STREAM, 0);

		if (connect(cliente, (void*) &direccionServidorWorker, sizeof(direccionServidorWorker)) != 0) {
			log_error(logger, "Problema al levantar conexión con WORKER en IP:%s PUERTO:%i (¿WORKER se encuentra levantado?)",ipWorker,puertoWorker);

			return -1;
		}

		log_debug(logger,"Conexión establecida con el WORKER.");
		//while (1) {
			//sleep(3);

			char* buffer = malloc(1000);
			//strcpy( mensaje.directorioArchivo, ArchivoOriginalDeYAMAFS);
			//send(cliente, mensaje, sizeof(mensaje), 0);
			send(cliente, "EXIT", 1000, 0);

			memset(buffer, 0, 1000);
			recv(cliente, buffer, 1000, 0);

			printf("Respuesta del Worker: %s\n", buffer);
			free(buffer);

			//}
		return 0;
}

int main(int argc, char *argv[]) {
	/* 	//Para calcular tiempo de ejecucion (METRICAS).
		#include <sys/time.h>
	clock_t start2 = clock();
	struct timeval stop, start;
	gettimeofday(&start, NULL);
	sleep(5);//Mi trabajo
	gettimeofday(&stop, NULL);
	printf("took %lu microseconds\n", stop.tv_usec - start.tv_usec);
    printf("Tiempo transcurrido: %f segundos", ((double)clock() - start2) / CLOCKS_PER_SEC);
	*/

	cargarConfiguraciones();
	sem_init(&mutex_log,0,1);
	if(argc!=CANTIDADARGUMENTOS) {
	log_error(logger, "Cantidad de argumentos recibidos incorrectos, se aborta la ejecución. (Se esperan %i se recibieron %i)",CANTIDADARGUMENTOS-1,argc-1);
	return EXIT_FAILURE;
	}else{
		ScriptTransformador = argv[1];
		ScriptReductor = argv[2];
		ArchivoOriginalDeYAMAFS = argv[3];
		ArchivoDestinoEnYAMAFS = argv[4];
	log_debug(logger, "Datos ingresados por parametros son: ScriptTransformador= \"%s\" ScriptReductor= \"%s\" ArchivoOriginalDeYAMAFS= \"%s\" ArchivoDestinoEnYAMAFS= \"%s\" ",ScriptTransformador,ScriptReductor,ArchivoOriginalDeYAMAFS,ArchivoDestinoEnYAMAFS);

	}
	 enviarArchivoAYama();

	if(conectarConWorker(ipWorker,puertoWorker)==-1){
	log_error(logger, "Falló al intentar conectar al WORKER, aborta ejecución.");
		return EXIT_FAILURE;
	}
	 pthread_join(thr_worker_transformacion,NULL);
	 pthread_join(thr_worker_reduccion,NULL);
	return EXIT_SUCCESS;
}

uint32_t tamanioArchivoFD(FILE* bin) {
	struct stat st;

	int descriptor_archivo = fileno(bin);
	fstat(descriptor_archivo, &st);

	return st.st_size;
}

int8_t sendAll (int8_t socket, char* datos, uint32_t tamanio_datos){
	uint32_t total = 0;
	uint32_t bytes_left = tamanio_datos;
	int i;

	while(total<tamanio_datos){
		i=send(socket, datos+total,bytes_left,0);
		if (i==-1) {
			break;
		}
		total += i;
		bytes_left -= i;
	}
	return i==-1?-1:1;
}

int8_t sendAll8 (int8_t socket, char* datos, uint8_t tamanio_datos){
	uint8_t total = 0;
	uint8_t bytes_left = tamanio_datos;
	int i;

	while(total<tamanio_datos){
		i=send(socket, datos+total,bytes_left,0);
		if (i==-1) {
			break;
		}
		total += i;
		bytes_left -= i;
	}
	return i==-1?-1:1;
}

void reloj(int loop)
{
	sleep(1);
  if (loop==0)
    log_info(logger,"[MASTER] Esperando respuesta \n ");

 printf("\033[1D");         //Introducimos código ANSI para retroceder 2 caracteres
  switch (loop%4)
    {
    case 0: printf("|"); break;
    case 1: printf("/"); break;
    case 2: printf("-"); break;
    case 3: printf("\\"); break;
    default:           //No debemos estar aquí
      break;
    }

  fflush(stdout);      //Actualizamos la pantalla
}


void realizarOperacionYAMA(){
	uint8_t codop,h;
	int8_t status;
	t_worker_para_transformacion* workerTransformacion = malloc(sizeof(t_worker_para_transformacion*));
	t_worker_para_reduccion_local* workerReducing = malloc(sizeof(t_worker_para_reduccion_local));
	char* datos_tmp;
	uint16_t comparacion = 0;

	status = recv(yamaSocket, &codop, sizeof(uint8_t), 0);
	if ((status==0) || (status==-1)){
		sem_wait(&mutex_log);
		log_error(logger,"YAMA Desconectado\n");
		log_error(logger,"Abortando...\n");
		sem_post(&mutex_log);
		abort();
	}
	else printf ("sigue");


	switch(codop){
		case CODOP_YAMA_RECIBIRWORKERTRANSFORMACION:


			recv(yamaSocket,&workerTransformacion->nro_bloque,sizeof(uint16_t), 0);

			recv(yamaSocket,&workerTransformacion->ip_long,sizeof(uint8_t), 0);
			workerTransformacion->ip = string_new();
			datos_tmp = malloc(workerTransformacion->ip_long+1);
			memset(datos_tmp,'\0',workerTransformacion->ip_long+1);
			comparacion = 0;

			while(comparacion != workerTransformacion->ip_long){
				comparacion += recv(yamaSocket,datos_tmp,workerTransformacion->ip_long-comparacion, 0);
				string_append(&workerTransformacion->ip,datos_tmp);
				memset(datos_tmp,'\0',workerTransformacion->ip_long+1);
			}
			free(datos_tmp);

			recv(yamaSocket,&workerTransformacion->puerto,sizeof(uint16_t), 0);

			recv(yamaSocket,&workerTransformacion->nombreArchivo_long,sizeof(uint8_t), 0);
			workerTransformacion->nombreArchivo = string_new();
			datos_tmp = malloc(workerTransformacion->nombreArchivo_long+1);
			memset(datos_tmp,'\0',workerTransformacion->nombreArchivo_long+1);
			comparacion = 0;

			while(comparacion != workerTransformacion->nombreArchivo_long){
				comparacion += recv(yamaSocket,datos_tmp,workerTransformacion->nombreArchivo_long-comparacion, 0);
				string_append(&workerTransformacion->nombreArchivo,datos_tmp);
				memset(datos_tmp,'\0',workerTransformacion->nombreArchivo_long+1);
			}
			free(datos_tmp);

			pthread_create(&thr_worker_transformacion,NULL,hiloMasterNodoTransformacion,(void*) workerTransformacion);

			  pthread_join(thr_worker_transformacion,NULL);
		break;

		case (CODOP_YAMA_RECIBIRWORKERREDUCCION_LOCAL||CODOP_YAMA_RECIBIRWORKERREDUCCION_GLOBAL):
			status = recv(yamaSocket,NULL,sizeof(uint8_t),0);


			recv(yamaSocket,&workerReducing->ip_long,sizeof(uint8_t),0);

			workerReducing->ip_nodo = string_new();
			datos_tmp = malloc(workerReducing->ip_long+1);
			memset(datos_tmp,'\0',workerReducing->ip_long+1);

			while(comparacion != workerReducing->ip_long){
				comparacion += recv(yamaSocket,datos_tmp,workerReducing->ip_long-comparacion, 0);
				string_append(&workerReducing->ip_nodo,datos_tmp);
				memset(datos_tmp,'\0',workerReducing->ip_long+1);
			}
			free(datos_tmp);

			recv(yamaSocket,&workerReducing->puerto_long,sizeof(uint16_t),0);
			recv(yamaSocket,&workerReducing->puerto,sizeof(uint16_t),0);
			recv(yamaSocket,&workerReducing->archivoTemporalFinal_long,sizeof(uint8_t),0);

			workerReducing->archivoTemporalFinal = string_new();
			datos_tmp = malloc(workerReducing->archivoTemporalFinal_long+1);
			memset(datos_tmp,'\0',workerReducing->archivoTemporalFinal_long+1);
			comparacion = 0;

			while(comparacion != workerReducing->archivoTemporalFinal_long){
				comparacion += recv(yamaSocket,datos_tmp,workerReducing->archivoTemporalFinal_long-comparacion, 0);
				string_append(&workerReducing->archivoTemporalFinal,datos_tmp);
				memset(datos_tmp,'\0',workerReducing->archivoTemporalFinal_long+1);
			}
			free(datos_tmp);

			recv(yamaSocket,&workerReducing->cantElementos,sizeof(uint8_t),0);
			t_list* listaNodosAReducir = list_create();
			for(h=0;h<workerReducing->cantElementos;h++){
				t_nodos_a_reducir* nodito = malloc(sizeof(t_nodos_a_reducir));
				recv(yamaSocket,&(nodito->ip_long),sizeof(uint8_t),0);

				nodito->ip_nodo = string_new();
				datos_tmp = malloc(nodito->ip_long+1);
				memset(datos_tmp,'\0',nodito->ip_long+1);
				comparacion = 0;

				while(comparacion != nodito->ip_long){
					comparacion += recv(yamaSocket,datos_tmp,nodito->ip_long-comparacion, 0);
					string_append(&nodito->ip_nodo,datos_tmp);
					memset(datos_tmp,'\0',nodito->ip_long+1);
				}
				free(datos_tmp);

				recv(yamaSocket,&nodito->puerto_long,sizeof(uint16_t),0);
				recv(yamaSocket,&nodito->puerto,sizeof(uint16_t),0);
				recv(yamaSocket,&nodito->nombreArchivo_long,sizeof(uint8_t),0);

				nodito->nombreArchivo = string_new();
				datos_tmp = malloc(nodito->nombreArchivo_long+1);
				memset(datos_tmp,'\0',nodito->nombreArchivo_long+1);
				comparacion = 0;

				while(comparacion != nodito->nombreArchivo_long){
					comparacion += recv(yamaSocket,datos_tmp,nodito->nombreArchivo_long-comparacion, 0);
					string_append(&nodito->nombreArchivo,datos_tmp);
					memset(datos_tmp,'\0',nodito->nombreArchivo_long+1);
				}
				free(datos_tmp);

				list_add(listaNodosAReducir,nodito);
			}
			workerReducing->nodosAReducir = list_create();
			list_add_all(workerReducing->nodosAReducir,listaNodosAReducir);
			pthread_create(&thr_worker_reduccion,NULL,hiloMasternodoReduccion,(void*) workerReducing);
		break;
	}
}


void* hiloMasterNodoTransformacion(void* workerTransformacion){

	uint8_t identificacion,codop,terminado;
	int8_t status;


	t_worker_para_transformacion* worker;

	worker = (t_worker_para_transformacion*) workerTransformacion;



	identificacion = ID_PROC_MASTER;
	codop = CODOP_WORKER_TRANSFORMACION;
	sem_wait(&mutex_log);


	log_info(logger,"Creacion de hilo transformacion para archivo %s\n",worker->nombreArchivo);
	sem_post(&mutex_log);

	int32_t workerSocket = conectarConWorker(worker->ip,worker->puerto);

	if(workerSocket!=-1){

		send(workerSocket,&identificacion,sizeof(uint8_t),0);
		send(workerSocket,&codop,sizeof(uint8_t),0);

		FILE* archivo = fopen(transformador,"rb");
		uint32_t tam = tamanioArchivoFD(archivo);
		char* script = malloc(sizeof(char)*tam);
		fread(script,tam,1,archivo);
		fclose(archivo);

		send(workerSocket,&tam,sizeof(uint32_t),0);
		sendAll(workerSocket,script,tam);

		free(script);

		send(workerSocket,&worker->nro_bloque,sizeof(uint16_t),0);
		send(workerSocket,&worker->nombreArchivo_long,sizeof(uint8_t),0);
		sendAll8(workerSocket,worker->nombreArchivo,worker->nombreArchivo_long);

		sem_wait(&mutex_log);
		log_info(logger,"Envie etapa transformacion a Nodo. Esperando resultados\n");
		sem_post(&mutex_log);

		status = recv(workerSocket,&terminado,sizeof(uint8_t),0);

		close(workerSocket);

		if((status==0)||(status==-1) || (terminado==0)){
			sem_wait(&mutex_log);
			log_error(logger,"SALIO MAL EL TRANSFORMACION");
			sem_post(&mutex_log);
			codop = CODOP_MASTER_TRANSFORMACIONTERMINADO;
			sem_wait(&mutex_yamaSocket);
			terminado=0;
			send(yamaSocket,&codop,sizeof(uint8_t),0);
			send(yamaSocket,&worker->nombreArchivo_long,sizeof(uint8_t),0);
			sendAll8(yamaSocket,worker->nombreArchivo,worker->nombreArchivo_long);
			send(yamaSocket,&terminado,sizeof(uint8_t),0);
			sem_post(&mutex_yamaSocket);
		}else{
			sem_wait(&mutex_log);
			log_info(logger,"TRANSFORMACION OK");
			sem_post(&mutex_log);
			codop = CODOP_MASTER_TRANSFORMACIONTERMINADO;
			sem_wait(&mutex_yamaSocket);
			send(yamaSocket,&codop,sizeof(uint8_t),0);
			send(yamaSocket,&worker->nombreArchivo_long,sizeof(uint8_t),0);
			sendAll8(yamaSocket,worker->nombreArchivo,worker->nombreArchivo_long);
			send(yamaSocket,&terminado,sizeof(uint8_t),0);
			sem_post(&mutex_yamaSocket);
		}
	}else{
		sem_wait(&mutex_log);
		log_error(logger,"SALIO MAL EL TRANSFORMACION");
		sem_post(&mutex_log);
		codop = CODOP_MASTER_TRANSFORMACIONTERMINADO;
		sem_wait(&mutex_yamaSocket);
		terminado=0;
		send(yamaSocket,&codop,sizeof(uint8_t),0);
		send(yamaSocket,&worker->nombreArchivo_long,sizeof(uint8_t),0);
		sendAll8(yamaSocket,worker->nombreArchivo,worker->nombreArchivo_long);
		send(yamaSocket,&terminado,sizeof(uint8_t),0);
		sem_post(&mutex_yamaSocket);
	}
	return worker;
}

void* hiloMasternodoReduccion(void* nodoReduccion){
	uint8_t identificacion,codop,terminado,i;
	int8_t status;
	t_worker_para_reduccion_local* nodo;
	nodo = (t_worker_para_reduccion_local*)nodoReduccion;

	sem_wait(&mutex_log);
	log_info(logger,"Creacion de hilo REDUCCION para archivo %s\n",nodo->archivoTemporalFinal);
	sem_post(&mutex_log);

	int32_t nodoSocket = conectarConWorker(nodo->ip_nodo,nodo->puerto);

	if(nodoSocket!=-1){

		identificacion = ID_PROC_MASTER;
		codop = CODOP_WORKER_REDUCCION_LOCAL;

		send(nodoSocket,&identificacion,sizeof(uint8_t),0);
		send(nodoSocket,&codop,sizeof(uint8_t),0);

		FILE* archivo = fopen(reductor,"rb");
		uint32_t tam = tamanioArchivoFD(archivo);
		char* script = malloc(sizeof(char)*tam);
		fread(script,tam,1,archivo);
		fclose(archivo);

		send(nodoSocket,&tam,sizeof(uint32_t),0);
		sendAll(nodoSocket,script,tam);

		free(script);

		send(nodoSocket,&(nodo->cantElementos),sizeof(nodo->cantElementos),0);
		for(i=0;i<nodo->cantElementos;i++){
			t_nodos_a_reducir* nodito = malloc(sizeof(t_nodos_a_reducir));
			nodito = list_get(nodo->nodosAReducir,i);
			send(nodoSocket,&(nodito->ip_long),sizeof(nodito->ip_long),0);
			sendAll8(nodoSocket,nodito->ip_nodo,nodito->ip_long);
			send(nodoSocket,&(nodito->puerto),nodito->puerto_long,0);
			send(nodoSocket,&(nodito->nombreArchivo_long),sizeof(nodito->nombreArchivo_long),0);
			sendAll8(nodoSocket,nodito->nombreArchivo,nodito->nombreArchivo_long);
		}
		send(nodoSocket,&(nodo->archivoTemporalFinal_long),sizeof(nodo->archivoTemporalFinal_long),0);
		sendAll8(nodoSocket,nodo->archivoTemporalFinal,nodo->archivoTemporalFinal_long);

		sem_wait(&mutex_log);
		log_info(logger,"Envie etapa REDUCCION a Nodo. Esperando resultados\n");
		sem_post(&mutex_log);

		status = recv(nodoSocket,&terminado,sizeof(uint8_t),0);

		if((status==0)||(status==-1) || (terminado == 0)){
			sem_wait(&mutex_log);
			log_error(logger,"SALIO MAL LA REDUCCION");
			sem_post(&mutex_log);
			codop = CODOP_MASTER_REDUCCIONTERMINADO;
			sem_wait(&mutex_yamaSocket);
			terminado=0;
			send(yamaSocket,&codop,sizeof(uint8_t),0);
			send(yamaSocket,&nodo->archivoTemporalFinal_long,sizeof(uint8_t),0);
			sendAll8(yamaSocket,nodo->archivoTemporalFinal,nodo->archivoTemporalFinal_long);
			send(yamaSocket,&terminado,sizeof(uint8_t),0);
			sem_post(&mutex_yamaSocket);
			sem_wait(&mutex_log);
			log_error(logger,"Abortando...");
			sem_post(&mutex_log);
			abort();
		}else{
			sem_wait(&mutex_log);
			log_info(logger,"REDUCCION OK. Guardado en %s",nodo->archivoTemporalFinal);
			sem_post(&mutex_log);
			codop = CODOP_MASTER_REDUCCIONTERMINADO;
			sem_wait(&mutex_yamaSocket);
			send(yamaSocket,&codop,sizeof(uint8_t),0);
			send(yamaSocket,&nodo->archivoTemporalFinal_long,sizeof(uint8_t),0);
			sendAll8(yamaSocket,nodo->archivoTemporalFinal,nodo->archivoTemporalFinal_long);
			send(yamaSocket,&terminado,sizeof(uint8_t),0);
			sem_post(&mutex_yamaSocket);

				log_info(logger,"El MASTER termino correctamente. Se aborta.\n");
				log_info(logger,"Abortando...\n");
				abort();
			}

}else{
		sem_wait(&mutex_log);
		log_error(logger,"SALIO MAL EL REDUCE");
		sem_post(&mutex_log);
		codop = CODOP_MASTER_REDUCCIONTERMINADO;
		sem_wait(&mutex_yamaSocket);
		terminado=0;
		send(yamaSocket,&codop,sizeof(uint8_t),0);
		send(yamaSocket,&nodo->archivoTemporalFinal_long,sizeof(uint8_t),0);
		sendAll8(yamaSocket,nodo->archivoTemporalFinal,nodo->archivoTemporalFinal_long);
		send(yamaSocket,&terminado,sizeof(uint8_t),0);
		sem_post(&mutex_yamaSocket);
		sem_wait(&mutex_log);
		log_error(logger,"Abortando...");
		sem_post(&mutex_log);
		abort();
		}
	return nodo;
}


