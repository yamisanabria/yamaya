/*
 ============================================================================
 Name        : YAMA.c
 Author      : yo
 Version     :
 Copyright   : Grupo YAMAYA
 Description : Ansi-style
 ============================================================================
 */

#include <stdio.h>
#include <stdlib.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/temporal.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <pthread.h>
#include <semaphore.h>
#include "YAMA.h"
#include <fcntl.h> // for open
#include <unistd.h> // for close

t_log* logger;
t_config* configuration;
int configOk=1;
char* ipFS;
int puertoFS;
int retardoPlanificacion;
char* algoritmoBalanceo;
sem_t mutex_log;
pthread_t hiloMaster;
pthread_t hiloFileSystem;
uint8_t socket_fs;
t_list* tablaEstados;

/** Longitud del buffer  */
#define BUFFERSIZE 512

#define PUERTO_YAMA 9262
#define CONEXIONES_MASTER_MAX_SIMULTANEAS 6 //Definimos cuantas maximas conexiones vamos a tener pendientes al mismo tiempo (Listen)
#define CodOP_Master_Solic_Nuevo_Trab 0


void cargarConfiguraciones() {

	logger = log_create("logYAMA", "YAMA LOG", true, LOG_LEVEL_DEBUG);
	log_info(logger, "<<Proceso YAMA iniciando>> (CARGANDO CONFIGURACIONES)");
	configuration = config_create("CONFIG_YAMA");
	if(configuration==NULL){
		log_error(logger, "Verifique el archivo de configuracion de entrada. (¿Este existe?)");
		exit(-1);
	}
	if (config_has_property(configuration, "FS_IP")) {

			ipFS = config_get_string_value(configuration, "FS_IP");



		} else {

			log_error(logger, "Error al obtener la IP del File System");

			configOk = 0;
		}

		if (config_has_property(configuration, "FS_PUERTO")) {

			puertoFS = config_get_int_value(configuration, "FS_PUERTO");

			log_info(logger, "El puerto del File System es: %i", puertoFS);

		} else {

			log_error(logger, "Error al obtener el puerto del File System");

			configOk = 0;
		}

		if (config_has_property(configuration, "RETARDO_PLANIFICACION")) {

			retardoPlanificacion = config_get_int_value(configuration, "RETARDO_PLANIFICACION");

			log_info(logger, "El tiempo de retardo para la planificación definido es: %i ms.", retardoPlanificacion);

		} else {

			log_error(logger, "Error al obtener el tiempo de retardo para la planificación.");

			configOk = 0;
		}

		if (config_has_property(configuration, "ALGORITMO_BALANCEO")) {

			algoritmoBalanceo = config_get_string_value(configuration, "ALGORITMO_BALANCEO");

			log_info(logger, "Definido el algoritmo de balanceo como: %s", algoritmoBalanceo);

		} else {

			log_error(logger, "Error al obtener el algoritmo de balanceo.");

			configOk = 0;
		}


		if (!configOk) {
				log_error(logger, "Debido a errores en las configuraciones, se aborta la ejecución... (REVISE ARCH. CONFIGURACIONES)");
				exit(-1);

		}


}

void *hiloClienteSocketMaster(int client_sock){

	 //Get the socket descriptor
	uint8_t master_socket = *(uint8_t*)client_sock;
	int8_t status = 1;
	    while((status)){
	    	realizarOperacionMaster(master_socket);
	    	status=0;
	    	sem_wait(&mutex_log);
	    	log_info(logger, "Master %i -> Finalizamos el trabajo con Master. Nos despedimos de Master...", master_socket);
	    	sem_post(&mutex_log);

	    	close(master_socket);
	    }
return 0;
}

int realizarOperacionMaster(uint8_t master_socket){
	uint8_t codop;
	T_master_yama_start estructuraARecibir;

	int bytesRecibidos=recv(master_socket, &codop, sizeof(uint8_t),0);
	if (bytesRecibidos <= 0) {

		log_error(logger,"Master %i -> Se ha desconectado.", master_socket);
		return -1;
	}else{
		log_debug(logger,"Master %i -> Solicitada la operación Nro: %i.", master_socket , codop);
	}

	switch(codop){
	case CodOP_Master_Solic_Nuevo_Trab:

		sem_wait(&mutex_log);
		log_info(logger, "Master %i -> Recibiendo datos iniciales desde Master. (CODOP: CodOP_Master_Solic_Nuevo_Trab -> 0)", master_socket);
		sem_post(&mutex_log);

		recibirYDeserializar(&estructuraARecibir,master_socket); //TODO Manejar error de retorno, en caso de fallar que finalice la aplicacion o la conexion.

		log_debug(logger, "Recibimos desde Master los siguientes datos: ArchOriginal=%s ArchDestino=%s",estructuraARecibir.archivo_ruta_origen_YamaFs,estructuraARecibir.archivo_ruta_destino_YamaFs);

		solicitarInformacionAlFS(&estructuraARecibir,socket_fs);

		/*bytesRecibidos=recv(master_socket,&(nodoRecibido.long_archivo_ruta_origen_YamaFs), sizeof(nodoRecibido.long_archivo_ruta_origen_YamaFs),0);
		if (bytesRecibidos <= 0) {

			log_error(logger,"Master %i -> Se ha desconectado.", master_socket);
			return -1;
		}else{
			log_debug(logger,"Master %i -> BytesRecibidos: %i con: '%i' ", master_socket, bytesRecibidos, nodoRecibido.long_archivo_ruta_origen_YamaFs);
		}
*/

	break;
	 default:
		 log_error(logger,"Master %i -> Se recibió un Código de Operación Inexistente: '%i' (¡Revisá el Código en Master!)", master_socket, codop); //TODO Revisar el cierre al ingresar un CODOP Erroneo
		 return -1;
	}
	//free(buffer);
	return 0;
}


int recibirYDeserializar(T_master_yama_start *estructuraARecibir, int sockN) {

	int estado = 1;
	int tam_buffer;
	char* buffer = malloc(tam_buffer = sizeof(uint32_t));

	//Obtengo archivo_ruta_origen_YamaFs
	uint32_t long_file_original;
	estado = recv(sockN, buffer, sizeof(estructuraARecibir->long_archivo_ruta_origen_YamaFs), 0);
	if (!estado)return 0;
		memcpy(&(long_file_original), buffer, tam_buffer);

	estructuraARecibir->archivo_ruta_origen_YamaFs = malloc(long_file_original + 1);
	memset(estructuraARecibir->archivo_ruta_origen_YamaFs, '\0', long_file_original + 1);
	estado = recv(sockN, estructuraARecibir->archivo_ruta_origen_YamaFs, long_file_original, 0);
	if (!estado)return 0;


	//Obtengo el path destino del archivo resultante del trabajo
	uint32_t nombre_long;
	tam_buffer = sizeof(uint32_t);
	estado = recv(sockN, buffer, sizeof(estructuraARecibir->long_archivo_ruta_destino_YamaFs), 0);
	if (!estado)return 0;
	memcpy(&(nombre_long), buffer, tam_buffer);

	estructuraARecibir->archivo_ruta_destino_YamaFs = malloc(nombre_long + 1);
	memset(estructuraARecibir->archivo_ruta_destino_YamaFs, '\0', nombre_long + 1);
	estado = recv(sockN, estructuraARecibir->archivo_ruta_destino_YamaFs, nombre_long, SOCK_STREAM); //TODO Revisar etiqueta para borrar excedentes en recv SOCK_STREAM
    if (!estado)return 0;


	/*//Obtengo el puerto
	uint16_t puerto_long;
	tam_buffer = sizeof(uint16_t);
	estado = recv(sockN, buffer, sizeof(nodoRecibido->puertoNodo_long), 0);
	if (!estado)return 0;

	memcpy(&(puerto_long), buffer, tam_buffer);

	uint16_t bufferNuevo;
	estado = recv(sockN, &bufferNuevo, puerto_long,0);
	if (!estado)return 0;
    nodoRecibido->puertoNodo=bufferNuevo;
    */



	free(buffer);
	return estado;

}

int planificarNuevaTarea(T_master_yama_start *estructuraConDatos){

	/*if(conectarConFS()==-1){
		return -1;
	}
	return 0;*/ //TODO Planificacion
}


void* socketListenerMaster(void* param){
	struct sockaddr_in direccionServidor;
	direccionServidor.sin_family = AF_INET;
	direccionServidor.sin_addr.s_addr = INADDR_ANY;
	direccionServidor.sin_port = htons(PUERTO_YAMA);

	int listenningSocket = socket(AF_INET, SOCK_STREAM, 0);

	int activado = 1;
	setsockopt(listenningSocket, SOL_SOCKET, SO_REUSEADDR, &activado, sizeof(activado)); //Para decir al sistema de reusar el puerto

	if (bind(listenningSocket, (void*) &direccionServidor, sizeof(direccionServidor)) != 0) {
		log_error(logger, "Falló el bind. (¿Seguro que no hay dos instancias de YAMA corriendo?)");
		exit(-1);
	}
	sem_wait(&mutex_log);
	log_info(logger, "YAMA levanto servidor y se encuentra esperando conexiones de Masters por el puerto %i.",PUERTO_YAMA);
	sem_post(&mutex_log);
	listen(listenningSocket, CONEXIONES_MASTER_MAX_SIMULTANEAS);

	//------------------------------

	struct sockaddr_in direccionCliente;
	unsigned int tamanioDireccion = sizeof(struct sockaddr_in);
	int client_sock;
	pthread_t thread_id;
	sem_wait(&mutex_log);
	log_info(logger,"Esperando conexiones de clientes Master...");
	sem_post(&mutex_log);
	 while( (client_sock = accept(listenningSocket, (void*) &direccionCliente, &tamanioDireccion)) )
	    {
		 sem_wait(&mutex_log);
		 log_info(logger,"Master %i -> Conexión nueva aceptada.",(int)client_sock);
		 sem_post(&mutex_log);

	        if( pthread_create( &thread_id , NULL , (void *) hiloClienteSocketMaster, (void*) &client_sock) < 0)
	        {
	        	log_error(logger,"Falló al crear el hilo para atender a Master. (¡Revise código!");
	            exit(-1);
	        }

	        //Now join the thread , so that we dont terminate before the thread
	        pthread_join( thread_id , NULL);

	    }


return 0;
}



int solicitarInformacionAlFS(T_master_yama_start *estructuraConDatos, int fs_sock){

	uint8_t codIdentificacion;
		codIdentificacion = 31; //YAMA se identifica con el nro 31 ->TODO PASARLO ARRIBA CON DEFINE
//ME PRESENTO Y LE DIGO QUIEN SOY
		if(send(fs_sock,&(codIdentificacion),sizeof(uint8_t),0)==-1){
			sem_wait(&mutex_log);
			log_error(logger, "Error al identificarme con mi número=%i. (¡Revise código!)",codIdentificacion);
			sem_post(&mutex_log);
			return -1;
		}else{
		sem_wait(&mutex_log);
		log_info(logger, "Me identifique al FS... (Soy=%i)",codIdentificacion);
		sem_post(&mutex_log);} //Envio primero el Codigo de operacion para que YAMA pueda usar un Switch.

//LE DIGO EL CODIGO DE OPERACION
		uint8_t codigoOperacion=82; //TODO crear codigos de operacion
		if(send(fs_sock,&(codigoOperacion),sizeof(uint8_t),0)==-1){
			sem_wait(&mutex_log);
			log_error(logger, "Error al solicitar la operación=%i. (¡Revise código!)",codigoOperacion);
			sem_post(&mutex_log);
			return -1;
		}else{
			sem_wait(&mutex_log);
			log_info(logger, "Le solicite la operación (%i).",codigoOperacion);
			sem_post(&mutex_log);} //Envio primero el Codigo de operacion para que YAMA pueda usar un Switch.
//LE DIGO LA LONGITUD DEL PATH

		uint8_t long_path_origen=strlen(estructuraConDatos->archivo_ruta_origen_YamaFs)+1;
		if(send(fs_sock,&(long_path_origen),sizeof(uint8_t),0)==-1){
			sem_wait(&mutex_log);
			log_error(logger, "Error %i. (¡Revise código!)",long_path_origen);
			sem_post(&mutex_log);
			return -1;
		}else{
			sem_wait(&mutex_log);
			log_info(logger, "Long: (%i).",long_path_origen);
			sem_post(&mutex_log);}
//LE DIGO EL PATH
		if(send(fs_sock,estructuraConDatos->archivo_ruta_origen_YamaFs,long_path_origen,0)==-1){
			sem_wait(&mutex_log);
			log_error(logger, "Error %s. (¡Revise código!)",estructuraConDatos->archivo_ruta_origen_YamaFs);
			sem_post(&mutex_log);
			return -1;
		}else{
			sem_wait(&mutex_log);
			log_info(logger, "txt: (%s).",estructuraConDatos->archivo_ruta_origen_YamaFs);
			sem_post(&mutex_log);
		}

		sem_wait(&mutex_log);
		log_info(logger, "Esperando recibir lista de nodos de FileSystem para el archivo %s\n",estructuraConDatos->archivo_ruta_origen_YamaFs);
		sem_post(&mutex_log);
//RECIBO INFORMACION DE BLOQUES QUE CONTIENEN EL PATH
		int cantidad=0;
		t_nodo_bloque* nodoARecibir=malloc(sizeof(t_nodo_bloque));
		uint8_t cantidadBloquesQueComponenArchivo=0;

		if((cantidad=recv(fs_sock,&cantidadBloquesQueComponenArchivo,sizeof(cantidadBloquesQueComponenArchivo),0))<0){
			sem_wait(&mutex_log);
			log_error(logger, "Error %s. (¡Revise código!)",cantidadBloquesQueComponenArchivo);
			sem_post(&mutex_log);
			return -1;
		}else{
			sem_wait(&mutex_log);
			log_info(logger, "Valor recibido: %i.(CONTADOR=%i)",cantidadBloquesQueComponenArchivo,cantidad);
			sem_post(&mutex_log);
		}



		int i=0;
		t_list* listaParaAlgoritmo = list_create();
		for (i = 0; i < cantidadBloquesQueComponenArchivo; ++i) {
			//ID NODO
			int contador=recv(fs_sock,&nodoARecibir->id_nodo,sizeof(nodoARecibir->id_nodo),0);
			sem_wait(&mutex_log);
			log_info(logger, "Valor id_nodo : %i. (CONTADOR=%i) (SIZEOF=%i)",nodoARecibir->id_nodo,contador,sizeof(nodoARecibir->id_nodo));
			sem_post(&mutex_log);

			//NRO DE BLOQUE NODO
			contador=recv(fs_sock,&nodoARecibir->nro_bloque_nodo,sizeof(nodoARecibir->nro_bloque_nodo),0);
			sem_wait(&mutex_log);
			log_info(logger, "Valor nro_bloque_nodo : %i.(CONTADOR=%i)(SIZEOF=%i)",nodoARecibir->nro_bloque_nodo,contador,sizeof(nodoARecibir->nro_bloque_nodo));
			sem_post(&mutex_log);

			//NRO DE BLOQUE ARCHIVO
			contador=recv(fs_sock,&nodoARecibir->nro_bloque_archi,sizeof(nodoARecibir->nro_bloque_archi),0);
			sem_wait(&mutex_log);
			log_info(logger, "Valor nro_bloque_archi : %i.(CONTADOR=%i)(SIZEOF=%i)",nodoARecibir->nro_bloque_archi,contador,sizeof(nodoARecibir->nro_bloque_archi));
			sem_post(&mutex_log);

			//IP
			contador=recv(fs_sock,&nodoARecibir->ip_long,sizeof(nodoARecibir->ip_long),0);
			sem_wait(&mutex_log);
			log_info(logger, "Valor iplong : %i.(CONTADOR=%i)(SIZEOF=%i)",nodoARecibir->ip_long,contador,sizeof(nodoARecibir->nro_bloque_archi));
			sem_post(&mutex_log);

			nodoARecibir->ip=malloc(nodoARecibir->ip_long);
			contador=recv(fs_sock,nodoARecibir->ip,nodoARecibir->ip_long,0);
			sem_wait(&mutex_log);
			log_info(logger, "Valor ip : %s.(CONTADOR=%i)(SIZEOF=%i)",nodoARecibir->ip,contador,nodoARecibir->ip_long);
			sem_post(&mutex_log);

			//NRO DE PUERTO
			contador=recv(fs_sock,&nodoARecibir->puerto,sizeof(nodoARecibir->puerto),0);
			sem_wait(&mutex_log);
			log_info(logger, "Valor puerto : %i.(CONTADOR=%i)(SIZEOF=%i)",nodoARecibir->puerto,contador,sizeof(nodoARecibir->puerto));
			sem_post(&mutex_log);

			//NOMBRE ARCHIVO
			contador=recv(fs_sock,&nodoARecibir->nombre_archivo_long,sizeof(nodoARecibir->nombre_archivo_long),0);
			sem_wait(&mutex_log);
			log_info(logger, "Valor nombre_archivo_long : %i.(CONTADOR=%i)(SIZEOF=%i)",nodoARecibir->nombre_archivo_long,contador,sizeof(nodoARecibir->nombre_archivo_long));
			sem_post(&mutex_log);

			nodoARecibir->nombre_archivo=malloc(nodoARecibir->nombre_archivo_long);
			contador=recv(fs_sock,nodoARecibir->nombre_archivo,nodoARecibir->nombre_archivo_long,0);
			sem_wait(&mutex_log);
			log_info(logger, "Valor nombre_archivo : %s.(CONTADOR=%i)(SIZEOF=%i)",nodoARecibir->nombre_archivo,contador,nodoARecibir->nombre_archivo_long);
			sem_post(&mutex_log);

			list_add(listaParaAlgoritmo,nodoARecibir);

		}
		//YA TENGO TODOS LOS BLOQUES PLANIFICO
		if(strcmp(algoritmoBalanceo,"CLOCK") == 0){
		//algoritmoDePlanificacionClock(listaParaAlgoritmo,p,id_job,nombreArchivoOriginal);
		}
		else if(strcmp(algoritmoBalanceo,"WCLOCK") == 0){
		//algoritmoDePlanificacionWClock(listaParaAlgoritmo,p,id_job,nombreArchivoOriginal);
		}
		else{
			log_error(logger, "Se ha recibido un algoritmo de balanceo incorrecto. (¡Verifica el Archivo de Configuraciones!) [CORRECTO= CLOCK o WCLOCK]");
		}

	return 0;
}


//ALGORITMO DE PLANIFICACION CLOCK
void algoritmoDePlanificacionClock(listaParaAlgoritmo,p,id_job,nombreArchivoOriginal){

}


void* conectarConFS(void* param){
	log_info(logger, "Intentando levantar conexión con FS.");
	struct sockaddr_in direccionServidor;
		direccionServidor.sin_family = AF_INET;
		direccionServidor.sin_addr.s_addr = inet_addr(ipFS);
		direccionServidor.sin_port = htons(puertoFS);


	int cliente_sock = socket(AF_INET, SOCK_STREAM, 0);

	if (connect(cliente_sock, (void*) &direccionServidor, sizeof(direccionServidor)) != 0) {
		log_error(logger, "Problema al levantar conexión con FS en IP:%s PUERTO:%i (¿FS se encuentra levantado?)",ipFS,puertoFS);
		abort();
		//return -1;
	}
	log_info(logger,"Conexión establecida con FS.");

	socket_fs=cliente_sock;


return 0;

}


int main(void) {
	sem_init(&mutex_log,0,1);
	cargarConfiguraciones();

	if(pthread_create(&hiloMaster,NULL,(void*) socketListenerMaster,NULL)<0){
		log_error(logger, "Falló al crear el hilo para escuchar a Master. (¡Revise Código!)");
	}


	if(pthread_create(&hiloFileSystem,NULL,(void*) conectarConFS,NULL)<0){
		log_error(logger, "Falló al crear el hilo para hablar a FileSystem. (¡Revise Código!)");
	}
	pthread_join(hiloFileSystem,NULL);
	pthread_join(hiloMaster,NULL);





	log_info(logger, "<<Proceso YAMA Finalizado>>");


}


