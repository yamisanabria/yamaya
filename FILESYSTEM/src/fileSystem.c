/*
 ============================================================================
 Name        : FILESYSTEM.c
 Author      : yo
 Version     :
 Copyright   : Grupo YAMAYA
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "fileSystem.h"

#define BACKLOG 5
#define DATANODE 30
#define YAMA 31
#define SOLICITO_INFORMACION_BLOQUES_ARCHIVO 82

t_log* logger;
t_config* configuration;
char* ipYAMA;
char* ipFS;
int puertoFS;
int socketDN;
int socketYama;
uint16_t puertoYAMA;
t_directorio* directorio_actual;
char* ruta_fs;
sem_t mutex_log;
uint8_t socket_yama;
int configOk=1;

t_list* listas_bloques_de_archivos;

int crearServidorFS (int puerto, t_log* logger){

	//inicializo parametros

	int sock = 0;

	struct sockaddr_in direccionServidor;
		direccionServidor.sin_family = AF_INET;
		direccionServidor.sin_addr.s_addr = INADDR_ANY;
		direccionServidor.sin_port = htons(puerto);
	memset(&(direccionServidor.sin_zero), 0, 8);

	log_info(logger, "[FS] Consiguiendo datos de red...");

	//creo el socket
	if ((sock = socket(AF_INET, SOCK_STREAM, 0)) == -1)
	{
		log_error(logger, "Error al abrir el socket");
		close(sock);
		return -1;
	}
	int activado = 1;
	setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &activado, sizeof(activado)); //Para decir al sistema de reusar el puerto

	if (bind(sock, (void*) &direccionServidor, sizeof(direccionServidor)) != 0) {
			log_error(logger, "Falló el bind. (¿Seguro que no hay dos instancias de FileSystem corriendo? ");
			return 1;
		}

	log_info(logger, "FileSystem levanto servidor y se encuentra escuchando por puerto %i.",puerto);

	listen(sock, 100);

	return sock;
}


int recibirYDeserializar(t_datanode *nodoRecibido, int sockN) {


	int estado = 1;
	int tam_buffer;
	char* buffer = malloc(tam_buffer = sizeof(uint8_t));

	//Obtengo la ip
	uint8_t ip_long;
//	tam_buffer = sizeof(uint8_t);
	estado = recv(sockN, buffer, sizeof(nodoRecibido->ipNodo_long), 0);
	if (!estado)return 0;
		memcpy(&(ip_long), buffer, tam_buffer);

	nodoRecibido->ipNodo = malloc(ip_long + 1);
	memset(nodoRecibido->ipNodo, '\0', ip_long + 1);
	estado = recv(sockN, nodoRecibido->ipNodo, ip_long, 0);
	if (!estado)return 0;

	//Obtengo el puerto
	uint16_t puerto_long;
	tam_buffer = sizeof(uint16_t);
	estado = recv(sockN, buffer, sizeof(nodoRecibido->puertoNodo_long), 0);
	if (!estado)return 0;

	memcpy(&(puerto_long), buffer, tam_buffer);

	uint16_t bufferNuevo;
	estado = recv(sockN, &bufferNuevo, puerto_long,0);
	if (!estado)return 0;
    nodoRecibido->puertoNodo=bufferNuevo;

	//Obtengo el nombre del Data Node
	uint16_t nombre_long;
	tam_buffer = sizeof(uint16_t);
	estado = recv(sockN, buffer, sizeof(nodoRecibido->nombreNodo_long), 0);
	if (!estado)return 0;
	memcpy(&(nombre_long), buffer, tam_buffer);

	nodoRecibido->nombreNodo = malloc(nombre_long + 1);
	memset(nodoRecibido->nombreNodo, '\0', nombre_long + 1);
	estado = recv(sockN, nodoRecibido->nombreNodo, nombre_long, 0);
    if (!estado)return 0;

	//Obtengo cantidad de bloques
    uint16_t cantBloques_long;
	tam_buffer = sizeof(uint16_t);
	estado = recv(sockN, buffer, sizeof(nodoRecibido->cantidad_bloques_long),0);
	if (!estado)return 0;


	memcpy(&(cantBloques_long), buffer, tam_buffer);
	estado = recv(sockN, &(nodoRecibido->cantidad_bloques), cantBloques_long,0);; //TODO Revisar elemento restante en RECV(3)
	if (!estado)return 0;

	free(buffer);
	return estado;

}

void agregarNodoAestructura(t_datanode* nodoRecibido, int sockN){

	log_info(logger,"Recibí de DATANODE: ip=%s, puerto=%d, nombre=%s, cantidad bloques=%d\n",nodoRecibido->ipNodo,nodoRecibido->puertoNodo ,nodoRecibido->nombreNodo, nodoRecibido->cantidad_bloques);

	uint16_t cantBloques = nodoRecibido->cantidad_bloques;
	char* ipNodo;
	ipNodo = nodoRecibido->ipNodo;
    uint16_t puerto = nodoRecibido->puertoNodo;
	char* nombre = nodoRecibido->nombreNodo;

	uint16_t i;
	t_nodoInterno *nodo = malloc(sizeof(t_nodoInterno));
	log_info(logger,"nodo_VIejo 2");
	t_nodoInterno* nodo_viejo = nodoBuscadoEnListaNodosPorIPYPuerto(ipNodo, puerto);




	if (nodo_viejo != NULL) {
	log_error(logger,"El Nodo conectado contiene mismo ip y puerto que nodo %s por lo tanto nodo %s es eliminado", nodo_viejo->nombre_nodo, nodo_viejo->nombre_nodo);

	//	desconexionDeNodo(nodo_viejo->socket_nodo);
	//	eliminarNodoDePartes(nodo_viejo);
	//	eliminarNodoDeListaNodos(nodo_viejo->socket_nodo);
	//	quitarNodoDeLista(nodo_viejo->socket_nodo);

	}else{
		log_info(logger, "El nodo conectado es unico, asi que procedemos a registrarlo.");
	}
	uint16_t cantBytes;
	if (cantBloques % 8 > 0)
		cantBytes = cantBloques / 8 + 1;
	else
		cantBytes = cantBloques / 8;

	char* bloques = malloc(cantBytes);
	t_bitarray *bloquesNodo = bitarray_create(bloques, sizeof(bloques));

	for (i = 0; i < cantBloques; i++) {
		bitarray_clean_bit(bloquesNodo, i);
	}

	nodo->bloques = bloquesNodo;
	nodo->nombre_nodo = nombre;
	nodo->cant_max_bloques = cantBloques;
	nodo->sockn = sockN;
	nodo->ip_nodo = ipNodo;
	nodo->puerto_nodo = puerto;


	list_add(listaNodosActivos, nodo);
	/*
	t_nodoInterno* variable=list_get(listaNodosActivos,0);
	printf("%s",variable->nombre_nodo);*/ //todo obtener de lista

	log_info(logger,"Agregué: ip=%s, puerto=%d, nombre=%s, cantidad bloques=%d, sock=%d\n",nodo->ip_nodo,nodo->puerto_nodo ,nodo->nombre_nodo, nodo->cant_max_bloques, nodo->sockn);

	/* struct stat st = {0};
		   if (stat("/metadata", &st) == -1) {
		       int resultado=mkdir("bitmaps", 0770);
		       printf("Resultado creacion: %i",resultado);
		   }*/ //TODO Revisar creación de directorio. Ahroa lo hacemos manualmente

	FILE* bin;

	char* ruta_bin = "/home/utnso/metadata/nodos.bin";
	if ((bin = fopen(ruta_bin, "w+b")) == NULL) {
					log_error(logger, "Error al al crear archivo nodos.bin");
					abort();
			}else{
				log_info(logger, "Archivo nodos.bin creado correctamente");
			}
	tamanioTotalNodos = tamanioTotalNodos + nodo->cant_max_bloques;
		fprintf(bin,"TAMANIO=%i\n",tamanioTotalNodos);
		fprintf(bin,"LIBRE=%i\n",171); //TODO valores de prueba
		fprintf(bin,"NODOS=[%s]\n","Nodo1, Nodo2, Nodo3");
		fprintf(bin,"%sTotal=%i\n","Nodo1",50);
		fprintf(bin,"%sLibre=%i\n","Nodo1",16);

	fclose(bin);

    persistirBloquesNodo(nodo);
    obtenerCantidadBloquesLibresNodo(nodo);

}

int obtenerCantidadBloquesLibresNodo(t_nodoInterno* nodo){

	char* ruta_bitmap;
	char* extArch;
	char* rutaAux = malloc(sizeof(1));
	char* ruta = malloc (sizeof(1));

	extArch = ".dat";
	ruta = "/home/utnso/metadata/bitmaps/";

	rutaAux = concat(nodo->nombre_nodo,extArch);
	ruta_bitmap = concat(ruta,rutaAux);

	FILE* bitmap;

	if ((bitmap = fopen(ruta_bitmap, "rb")) == NULL) {
		log_error(logger, "Error al abrir archivo bitmao de %s\n", nodo->nombre_nodo);
		abort();
	}else{
		log_info(logger, "Archivo bitmap abierto correctamente de %s", nodo->nombre_nodo);
	   	}



	    char *code;
	    size_t n = 0;
	    int c;

	    if (bitmap == NULL)
	        return 0; //could not open file

	    code = malloc(1000);

	    while ((c = fgetc(bitmap)) != EOF)
	    {
	        code[n++] = (char) c;
	    }

//TODO BITMAP
return 0;

}

char* concat(char* s1, char* s2)
	{
	    char *result = malloc(strlen(s1)+strlen(s2)+1);//+1 for the null-terminator
	    //in real code you would check for errors in malloc here
	    strcpy(result, s1);
	    strcat(result, s2);
	    return result;
	}

void persistirBloquesNodo(t_nodoInterno *nodoApersistir) {

	char* ruta_bitmap;
	char* extArch;
	char* rutaAux = malloc(sizeof(1));
	char* ruta = malloc (sizeof(1));

	/* struct stat st = {0};
	   if (stat("metadata/bitmaps/", &st) == -1) {
	       int resultado=mkdir("bitmaps", 0770);
	       printf("Resultado creacion: %i",resultado);
	   }*/ //TODO Revisar creación de directorio. Ahroa lo hacemos manualmente


	extArch = ".dat";
	ruta = "/home/utnso/metadata/bitmaps/";

 	rutaAux = concat(nodoApersistir->nombre_nodo,extArch);
	ruta_bitmap = concat(ruta,rutaAux);

	FILE* bitmap;

	if ((bitmap = fopen(ruta_bitmap, "w+b")) == NULL) {
					log_error(logger, "Error al al crear archivo bitmap\n");
					abort();
			}else{
				log_info(logger, "Archivo bitmap creado correctamente");
			}

  //TODO creacion del bitmap

  int i;

  for(i=0; i < nodoApersistir->cant_max_bloques; i++ ){

	  fprintf(bitmap,"0");

  }

	fclose(bitmap);
}

t_nodoInterno* nodoBuscadoEnListaNodosPorIPYPuerto(char* ip, uint16_t puerto) {
	bool cumpleConIPPUERTO(t_nodoInterno* nodo) {
			return (nodo->puerto_nodo == puerto) && string_equals_ignore_case(nodo->ip_nodo,ip);
		}

	t_nodoInterno* nodo = list_find(listaNodosActivos, (void*) cumpleConIPPUERTO);
	return nodo;
}

int conexion_yama(void* param) {

	int sockM = *((int *) param);
	//enviar handshake ok

	t_mensaje mensaje;
	memset(&mensaje, 0, sizeof(t_mensaje));
	int size_mensaje = sizeof(t_mensaje);
	char* buffer;
	int numberBytes = 0;

	if ((buffer = (char*) malloc(sizeof(char) * size_mensaje)) == NULL) {
		log_error(logger, "error al reservar memoria para el buffer de YAMA");
		return -1;
	}

	mensaje.tipo = HANDSHAKEOK;
	mensaje.id_proceso = FILESYSTEM;
	memset(buffer, '\0', size_mensaje);
	memcpy(buffer, &mensaje, size_mensaje);
	if ((numberBytes = send(sockM, buffer, size_mensaje, 0)) <= 0) {
		log_error(logger, "error al enviar el mensaje al YAMA");
		return -1;
	}
	log_info(logger, "logro conexion correcta con YAMA");
	log_info(logger, "socket de YAMA: %d", sockM);
	sleep(1 / 100);

	memset(&mensaje, 0, sizeof(t_mensaje));

	free(buffer);
	return 0;

}

void cargarConfiguraciones() {
	logger = log_create("logFileSystem", "FILESYSTEM LOG", true, LOG_LEVEL_DEBUG);
	configuration = config_create("CONFIG_FS");
		if(configuration==NULL){
			log_error(logger, "Verifique el archivo de configuracion de entrada. (¿Este existe?)");
			exit(-1);
		}
		if (config_has_property(configuration, "IP_FS")) {

				ipFS = config_get_string_value(configuration, "IP_FS");


				log_debug(logger, "La IP del File System es: %s", ipFS);

			} else {

				log_error(logger, "Error al obtener la IP del File System");

				configOk = 0;
			}

			if (config_has_property(configuration, "PUERTO_FS")) {

				puertoFS = config_get_int_value(configuration, "PUERTO_FS");

				log_debug(logger, "El puerto del File System es: %i", puertoFS);

			} else {

				log_error(logger, "Error al obtener el puerto del File System");

				configOk = 0;
			}

}

void consola_imprimir_encabezado(){
	printf("*********** BIENVENIDO A LA CONSOLA DEL FILESYSTEM ***********\n");
	printf("En caso de no conocer los comandos escriba la palabra help\n");
	printf("\n");

	leer_palabra();

}

void consola_imprimir_menu(){

	printf("COMANDOS ADMITIDOS:\n");
	printf("format\n");
	printf("rm\n");
	printf("rename\n");
	printf("mv\n");
	printf("cat\n");
	printf("mkdir\n");
	printf("cpfrom\n");
	printf("cpto\n");
	printf("cpblock\n");
	printf("md5\n");
	printf("ls\n");
	printf("info\n");
	printf("help");
}


void formatear_filesystem(){
	printf("probando eliminar archivo\n");
}

void eliminar_arch_etc(){
	printf("probando eliminar archivo\n");
}

void renombrar_arch_dir(){
	printf("probando renombrar\n");
}

void mostrar_cont_arch(){
	printf("mostrar contenido\n");
}

void crear_directorio(){
	printf("crear directorio\n");
}

void copia_arch_local(){
	printf("copiar un archivo local al yamafs\n");
}

void crear_copia_bloque(){
	printf("copia de un bloque de un archivo\n");
}

void solicitar_md5(){
	printf("solicitar md5\n");
}

void listar_archivos(){
	printf("listar los archivos de un directorio\n");
}

void mostrar_info(){
	printf("mostrar información\n");
}

void reconocer_comando(char * linea){ //reconoce comando

	int flag_comandoOK =0;
	char dirarch1[250];
	int dirarch2;
	fflush(stdin);

	if(flag_comandoOK==0 && strcmp(linea, "format")==0){
        formatear_filesystem();
	}else if(flag_comandoOK==0 && strcmp(linea, "rm")==0){
	    eliminar_arch_etc();
	   }else if(flag_comandoOK==0 && strcmp(linea, "rname")==0){
		   renombrar_arch_dir();
	   }else if(flag_comandoOK==0 && strcmp(linea, "mv")==0){
		   eliminar_arch_etc();
	   }else if(flag_comandoOK==0 && strcmp(linea, "cat")==0){
		   mostrar_cont_arch();
	    }else if(flag_comandoOK==0 && strcmp(linea, "mkdir")==0){
	    	crear_directorio();
	    }else if(flag_comandoOK==0 && strcmp(linea, "cpfrom")==0){

	    	printf("\nIngrese la dirección del archivo:\n\n>");
	    	fgets(dirarch1, sizeof(dirarch1), stdin);
	    	printf("\nIngrese el directorio del FS:\n\n>");
	    	scanf("%i", &dirarch2);
	    //	limpiar(dirarch1);

	    dividirArchivoUsuario(dirarch1,dirarch2);
	    	//devolverRedundanciaArchivo(redundanciaArchivo);


	    }else if(flag_comandoOK==0 && strcmp(linea, "cpto")==0){
	    	copia_arch_local();
	    }else if(flag_comandoOK==0 && strcmp(linea, "cpblock")==0){
	    	crear_copia_bloque();
	    }else if(flag_comandoOK==0 && strcmp(linea, "md5")==0){
	    	solicitar_md5();
	    }else if(flag_comandoOK==0 && strcmp(linea, "ls")==0){
	    	listar_archivos();
	    }else if(flag_comandoOK==0 && strcmp(linea, "info")==0){
	    	mostrar_info();
	    }else if (flag_comandoOK==0 && strcmp(linea, "help")==0){
		   consola_imprimir_menu();
		   leer_palabra();
		   }else if (flag_comandoOK==0 && strcmp(linea, "help")!=0){
			   printf("%s no es un comando válido\n", linea);
			   leer_palabra();
			   free(linea);
		   }
}

void leer_palabra(){
	char * linea;

	while(1) {
		    linea = readline(">");

		     reconocer_comando(linea);

		  }
	}

void crearEstructuraFS(void)
{
	sem_init(&mutex_log, 0, 1);

	listaNodosActivos=list_create();
	listaNodosEsperando=list_create();
	listaNodosDesconectados=list_create();
	tablaDeArchivos=list_create();
	punteroNodoAllenar=0;
	nodoParaYama=list_create();
	listaNombreNodos=list_create();

	tamanioLibreNodos = 0;
	tamanioTotalNodos = 0;

}
int dividirArchivoUsuario(char* pathArchLocal, int directorio) {

	int tamBloque = 1024 * 1024;
	char* buffer = calloc(tamBloque, 1);
	memset(buffer, '\0', tamBloque);

	t_file* archivoNuevo = malloc(sizeof(t_file));
	char nombreArchivo[255];

	strcpy(archivoNuevo->nombre, nombreArchivo);

	archivoNuevo->directorio = directorio;
	archivoNuevo->bloques = list_create();

	printf("path: %s\n", pathArchLocal);

	//Abro archivo
	FILE* file;
	struct stat stat_file;
	stat(pathArchLocal, &stat_file);
	file = fopen(pathArchLocal, "r");
	printf("%s",pathArchLocal);

	if (file == NULL) {
		log_error(logger, "Error de apertura de archivo");
		puts("-1");

		return -1;
	}

	log_info(logger, "puntero a archivo: %p", file);

	//calculo la cantidad de bloques de un archivo
	int cantBloques = (stat_file.st_size) / tamBloque;
	int tam_archivo = stat_file.st_size;

	printf("cantidad de bloques: %d y tamaño: %d",cantBloques,(tam_archivo % tamBloque));

	log_info(logger, "tam archivo: %d", tam_archivo);
	log_info(logger, "cantBloques: %d", cantBloques);

	if ((tam_archivo % (1024 * 1024 * 20)) >= 1) {
				cantBloques = cantBloques + 1;
			}

	//Leo del archivo y lo pongo en el buffer

	archivoNuevo->tamanio = cantBloques * tamBloque;

	int resto = archivoNuevo->tamanio - tam_archivo;
	int resto1 = tam_archivo % tamBloque;

	printf("resto= %d --  resto1: %d\n", resto, resto1);

	int j;
	int desplazamiento = 0;
	int cantidadBloquesIniciales = cantBloques;
	for (j = 0; j < cantBloques; j++) {

		memset(buffer, '\0', tamBloque);
		if (cantidadBloquesIniciales - 1 > j) {
			fseek(file, desplazamiento, SEEK_SET); //buscar desde el ppio del archivo
			char* bufferAux = calloc(tamBloque, 1);
			memset(bufferAux, '\0', tamBloque);
			fread(bufferAux, tamBloque, 1, file);
			int tamanio_bloque_actual = tamBloque;
			int barraN = 0;
			while ((!barraN) && (tamanio_bloque_actual >= 0)) {
				if (bufferAux[tamanio_bloque_actual] == '\n') {
					barraN = 1;
					tamanio_bloque_actual++;
				} else {
					tamanio_bloque_actual--;
				}
			}
			memcpy(buffer, bufferAux, tamanio_bloque_actual);
			free(bufferAux);
			printf("\njtamaño: %d", strnlen(buffer, tamanio_bloque_actual));
			desplazamiento += tamanio_bloque_actual;
			resto1 = tam_archivo - desplazamiento;
		} else {
			int tamanio_bloque_actual = tam_archivo - desplazamiento;
			if (resto1 > tamBloque) {
				tamanio_bloque_actual = tamBloque;
			}
			fseek(file, desplazamiento, SEEK_SET);
			char* bufferAux = calloc(tamBloque, 1);
			memset(bufferAux, '\0', tamBloque);
			fread(bufferAux, tamBloque, 1, file);
			int barraN = 0;
			while ((!barraN) && (tamanio_bloque_actual >= 0)) {
				if (bufferAux[tamanio_bloque_actual] == '\n'
						|| tamanio_bloque_actual == resto1) {
					barraN = 1;
					tamanio_bloque_actual++;
				} else {
					tamanio_bloque_actual--;
				}
			}
			memcpy(buffer, bufferAux, tamanio_bloque_actual);
			free(bufferAux);
			printf("\njtamaño: %d", strnlen(buffer, tamanio_bloque_actual));
			desplazamiento += tamanio_bloque_actual;
			resto1 = tam_archivo - desplazamiento;
			if (resto1 > 0)
				cantBloques++;
			printf("\ntamaño: %d resto: %d", strnlen(buffer, tamBloque),
					resto1);	//no seria el resto sino lo que falta.
		}
	}
 return 0;
}



void* clienteSocketThread(void* socket) {
		uint8_t cliente_socket;
		cliente_socket = *((int*) socket);
		uint8_t identificador;
		sem_wait(&mutex_log);
		log_debug(logger, "Cliente conectado %i. Aguardamos identificacion", cliente_socket);
		sem_post(&mutex_log);
		//handshake
		int8_t status = recv(cliente_socket, &identificador, sizeof(uint8_t), 0);  //DATANODE=30 YAMA=31
		if (status != -1) {
			if (identificador == DATANODE) {
				sem_wait(&mutex_log);
				log_debug(logger, "Conexion por socket con DataNode");
				sem_post(&mutex_log);
				sem_wait(&mutex_log);
				log_info(logger, "Conectado a DATANODE");
				sem_post(&mutex_log);
				conexion_datanode(cliente_socket);
			} else if (identificador == YAMA) {
				sem_wait(&mutex_log);
				log_debug(logger, "Conexion por socket con YAMA");
				sem_post(&mutex_log);
				sem_wait(&mutex_log);
				log_info(logger, "Conectado a YAMA");
				sem_post(&mutex_log);
				printf("Acitvos al momento: %i",list_size(listaNodosActivos));

				conexion_YAMA(cliente_socket);
			}else{
				sem_wait(&mutex_log);
				log_error(logger, "He recibo una conexión de un cliente desconocido y no se como tratarlo. (¡Verifique código en FS!)");
				sem_post(&mutex_log);
			}
		}

		return socket; //retorno solo para evitar el warning
	}

int conexion_datanode(int sockM) {

	t_mensaje mensaje;
	t_datanode nodoRecibido;

	memset(&mensaje, 0, sizeof(t_mensaje));
	int size_mensaje = sizeof(t_mensaje);
	char* buffer;

	if ((buffer = (char*) malloc(sizeof(char) * size_mensaje)) == NULL) {
		log_error(logger, "Error al reservar memoria para el buffer de DATANODE");
		return -1;
	}

	log_info(logger, "logro conexion correcta con DATANODE");
	log_info(logger, "socket de DATANODE: %d", sockM);
	int8_t estado = recibirYDeserializar(&nodoRecibido, sockM);

	 if (estado != -1) {
		 sem_wait(&mutex_log);
	 	 log_debug(logger, "Deserialice un nodo\n");
	 	 sem_post(&mutex_log);
	 	 agregarNodoAestructura(&nodoRecibido, sockM);
	}else {
		printf("Fallo el deserializado...(¡Verifica codigo!)");
	}


	free(buffer);
	return 0;
}

int conexion_YAMA(int socket) {
	uint8_t cod_op;
	socket_yama = socket;
	int8_t status = 1;
	int8_t status2;

while(status){

	status2= recv(socket_yama, &cod_op, sizeof(uint8_t), 0);
	if (status2 == 0 || status2 == -1) {
		sem_wait(&mutex_log);
		log_error(logger, "YAMA se ha desconectado.");
		sem_post(&mutex_log);
		status=0;
	}else{
		switch (cod_op) {
			case SOLICITO_INFORMACION_BLOQUES_ARCHIVO:
				sem_wait(&mutex_log);
				log_debug(logger, "YAMA SOLICITO_INFORMACION_BLOQUES_ARCHIVO.");
				sem_post(&mutex_log);
			informacionDeArchivoParaYama(&status);
			break;

		default:
			 log_error(logger,"YAMA %i -> Se recibió un Código de Operación Inexistente: '%i' (¡Ignoramos y continuamos!) COD=FSCODECASE", socket_yama, cod_op); //TODO Revisar el cierre al ingresar un CODOP Erroneo y verificar ejecutar con dos master al mismo tiempo sobre yama
			 continue;
		}
		}
	}
return 0;
}

int informacionDeArchivoParaYama(int8_t* status){
	uint8_t long_path_archivo_original;
	uint16_t i;

	//RECIBO LARGO DEL PATH
		*status = recv(socket_yama, &long_path_archivo_original, sizeof(uint8_t), 0);
		sem_wait(&mutex_log);
		log_debug(logger, "Recibido long %i", long_path_archivo_original);
		sem_post(&mutex_log);

	//RECIBO EL PATH
		char* path_archivo_original=malloc(long_path_archivo_original);
		memset(path_archivo_original, '\0', long_path_archivo_original + 1);
		*status = recv(socket_yama, path_archivo_original, long_path_archivo_original, 0);
		sem_wait(&mutex_log);
		log_debug(logger, "Recibido path %s", path_archivo_original);
		sem_post(&mutex_log);

	//ENVIO DATOS DEL ARCHIVO SOLICITADO
		log_info(logger, "Comienzo el envio de datos a YAMA.");
		/*t_list* listaBloques = list_create();

		t_archivo_bloques_partes* archivo = malloc(sizeof(t_archivo_bloques_partes));


		list_add_all(listaBloques, archivo->bloques);*/

		//Todo Por ahora es un envio fijo a yama de un archivo inexistente, acoplarlo con lo que haga yami.



		uint8_t cantidadBloquesQueComponenArchivo=1;

		int numberBytes;

		if ((numberBytes = send(socket_yama, &cantidadBloquesQueComponenArchivo, sizeof(cantidadBloquesQueComponenArchivo), 0)) <= 0) {
				log_error(logger, "Hubo problema al enviar cantidadBloquesQueComponenArchivo a YAMA");
				return -1;
		}else{
			log_info(logger,"Enviado la cantidad de bloques: %i",cantidadBloquesQueComponenArchivo);
		}

		for (i = 0; i < cantidadBloquesQueComponenArchivo; i++) {

				// t_nodo_bloque* nodo_bloque = list_get(listaBloques, i); TODO obtener de la lista de bloques
			t_nodo_bloque* nodo_bloque=malloc(sizeof(t_nodo_bloque));

			nodo_bloque->nombre_archivo=malloc(strlen("file.csv") + 1);
			strcpy(nodo_bloque->nombre_archivo, "file.csv");

			nodo_bloque->nombre_archivo_long=86;

			nodo_bloque->nro_bloque_archi=89;
			nodo_bloque->nro_bloque_archi_long=sizeof(uint16_t);
			nodo_bloque->id_nodo=81;
			nodo_bloque->id_nodo_long=sizeof(uint8_t);
			nodo_bloque->nro_bloque_nodo=85;
			nodo_bloque->nro_bloque_archi_long=sizeof(uint16_t);

			nodo_bloque->ip="127.0.0.874";
			nodo_bloque->ip_long=string_length(nodo_bloque->ip);

			nodo_bloque->puerto=1234;
			nodo_bloque->nombre_archivo="nombreArchivo.csv";
			nodo_bloque->nombre_archivo_long=string_length(nodo_bloque->nombre_archivo);
			enviarUnBloque(nodo_bloque);
			}



		if (*status == 0 || *status == -1) {
				sem_wait(&mutex_log);
				log_error(logger, "YAMA se ha desconectado.");
				sem_wait(&mutex_log);
			}
return 0;
}

void enviarUnBloque(t_nodo_bloque* nodo_bloque) {
	//ID NODO

	send(socket_yama, &nodo_bloque->id_nodo,sizeof(uint8_t),0);
	log_info(logger,"enviado %i id nodo", nodo_bloque->id_nodo);

	//NRO DE BLOQUE NODO
	send(socket_yama, &nodo_bloque->nro_bloque_nodo,sizeof(uint16_t),0);
	log_info(logger,"enviado %i nro_bloque_nodo", nodo_bloque->nro_bloque_nodo);

	//NRO DE BLOQUE ARCHIVO
	send(socket_yama, &nodo_bloque->nro_bloque_archi,sizeof(uint16_t),0);
	log_info(logger,"enviado %i nro_bloque_archi", nodo_bloque->nro_bloque_archi);

	//IP
	send(socket_yama, &nodo_bloque->ip_long,sizeof(uint8_t),0);
	log_info(logger,"enviado %i ip_long", nodo_bloque->ip_long);

	sendAll(socket_yama, nodo_bloque->ip,nodo_bloque->ip_long);

	//NRO DE PUERTO
	send(socket_yama, &nodo_bloque->puerto,sizeof(uint16_t),0);

	//NOMBRE ARCHIVO
	send(socket_yama, &nodo_bloque->nombre_archivo_long,sizeof(uint8_t),0);
	sendAll(socket_yama, nodo_bloque->nombre_archivo,nodo_bloque->nombre_archivo_long);
}

int8_t sendAll (int8_t socket, char* datos, uint8_t tamanio_datos){
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

char* serializarEnvioArchivoDatosAYama(t_bloquesArchivoYamaFS* estructuraAMandar){
	/*
		char *serializedPackage = malloc(estructuraAMandar->tamanioTotalEstructura);
		 //memset(serializedPackage, 0, sizeof(&serializedPackage));
		 int offset = 0;
		 int size_to_send;

		 //Empaquetamos el numero del bloque del archivo deseado
		  size_to_send =  sizeof(estructuraAMandar->numeroBloque);
		  memcpy(serializedPackage + offset, &(estructuraAMandar->numeroBloque), size_to_send);
		  offset += size_to_send;
		 printf("Enviado NumeroDeBloqueDelArchivoSolicitado: %u ", estructuraAMandar->numeroBloque);


		 //Empaquetamos el largo del string que contiene el nombre del nodo que guarda el bloque con la copia 0
		 size_to_send =  sizeof(estructuraAMandar->largoNombreNodoCopia0);
		 memcpy(serializedPackage + offset, &(estructuraAMandar->largoNombreNodoCopia0), size_to_send);
		 offset += size_to_send;
		printf("Enviado long_nombre_nodo_copia0: %u ", estructuraAMandar->largoNombreNodoCopia0);

		//Ahora empaquetamos el nombre del nodo que contiene la copia 0 de este bloque
		size_to_send=estructuraAMandar->nombreNodoCopia0;
		memcpy(serializedPackage + offset, estructuraAMandar->nombreNodoCopia0, size_to_send);
		offset += size_to_send;
		printf("Enviado nombre_nodo_copia0: %s ", estructuraAMandar->nombreNodoCopia0);

		//Empaquetamos el nro de bloque de archivo en nodo con la copia 0
		size_to_send =  sizeof(estructuraAMandar->nroDeBloqueDeArchivoEnNodoCopia0);
		memcpy(serializedPackage + offset, &(estructuraAMandar->nroDeBloqueDeArchivoEnNodoCopia0), size_to_send);
		offset += size_to_send;
		printf("Enviado nroDeBloqueDeArchivoEnNodoCopia0: %u ", estructuraAMandar->nroDeBloqueDeArchivoEnNodoCopia0);



		//Empaquetamos el tamaño del path del archivo //TODO DEMO
	 	 size_to_send =  sizeof(estructuraAMandar->long_archivo_ruta_origen_YamaFs);
	 	 memcpy(serializedPackage + offset, &(estructuraAMandar->long_archivo_ruta_origen_YamaFs), size_to_send);
	 	 offset += size_to_send;
	 	printf("Enviado long_archivo_ruta_origen_YamaFs: %u ", estructuraAMandar->long_archivo_ruta_origen_YamaFs);

	 	 //Ahora empaquetamos el path del archivo origen YAMA FS //TODO DEMO
	 	 size_to_send=estructuraAMandar->long_archivo_ruta_origen_YamaFs;
	 	 memcpy(serializedPackage + offset, estructuraAMandar->archivo_ruta_origen_YamaFs, size_to_send);
	 	 offset += size_to_send;
	 	printf("Enviado archivo_ruta_origen_YamaFs: %s ", estructuraAMandar->archivo_ruta_origen_YamaFs);

	 	return serializedPackage;*/
}



void* socketListener(void* param)
{
	    sem_wait(&mutex_log);
		log_debug(logger, "Iniciando Listener para escuchar clientes YAMA/DATANODES");
		sem_post(&mutex_log);
		struct addrinfo hints;
		struct addrinfo *serverInfo;

		memset(&hints, 0, sizeof(hints));
		hints.ai_family = AF_UNSPEC; // No importa si uso IPv4 o IPv6
		hints.ai_flags = AI_PASSIVE; // Asigna el address del localhost: 127.0.0.1 //TODO VERIFICAR SI ES NECESARIO IP
		hints.ai_socktype = SOCK_STREAM; // Indica que usaremos el protocolo TCP

		sem_wait(&mutex_log);
		log_debug(logger, "Hints completo");
		sem_post(&mutex_log);

		// Le pasamos NULL como IP, ya que le indicamos que use localhost en AI_PASSIVE
		// Carga en serverInfo los datos de la conexion
		getaddrinfo(NULL, string_itoa(puertoFS), &hints, &serverInfo);

		//Creo socket que escuche las conexiones entrantes
		int listeningSocket;
		listeningSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype,
				serverInfo->ai_protocol);
		sem_wait(&mutex_log);
		log_debug(logger, "Socket para escuchar generado: %d", listeningSocket);
		sem_post(&mutex_log);
		//Me pongo a escuchar conexiones entrantes
		if(bind(listeningSocket, serverInfo->ai_addr, serverInfo->ai_addrlen)==-1){
			log_error(logger,"El puerto de listening esta ocupado.");
			log_error(logger,"Interrumpiendo ejecución...");
			abort();
		}
		freeaddrinfo(serverInfo);
		sem_wait(&mutex_log);
		log_debug(logger, "Escuchando por el puerto");
		sem_post(&mutex_log);

		//El sistema esperara hasta que reciba una conexion entrante
		struct sockaddr_in addr; // Esta estructura contendra los datos de la conexion del cliente. IP, puerto, etc.
		socklen_t addrlen = sizeof(addr);

		while (true) {
			sem_wait(&mutex_log);
			log_debug(logger, "Estoy escuchando");
			sem_post(&mutex_log);
			listen(listeningSocket, BACKLOG); // IMPORTANTE: listen() es una syscall BLOQUEANTE.

			// Aceptamos la conexion entrante y creamos un nuevo socket para comunicarnos (el actual se queda escuchando más conexiones)
			int clienteSocket = accept(listeningSocket, (struct sockaddr *) &addr,
					&addrlen);
			pthread_t thr_cliente;
			sem_wait(&mutex_log);
			log_debug(logger, "Llego alguien");
			sem_post(&mutex_log);
			int cliente = pthread_create(&thr_cliente, NULL, clienteSocketThread,(void*) &clienteSocket);
			if(cliente){
						sem_wait(&mutex_log);
						log_error(logger,"No se pudo crear el hilo Cliente");
						sem_post(&mutex_log);
						abort();
				}
		}
		sem_wait(&mutex_log);
		log_debug(logger, "Termino el hilo listener");
		sem_post(&mutex_log);
		close(listeningSocket);
		return param;
}



int main(int argc, char *argv[]) {

	sem_init(&mutex_log,0,1);
	pthread_t th_consola;
	pthread_t th_listener;

	cargarConfiguraciones();
	int listener= pthread_create(&th_listener, NULL, socketListener, NULL); //TODO Descomentar luego de prueba

	if(listener){
		sem_wait(&mutex_log);
		log_error(logger,"No se pudo crear el hilo Listener");
	    sem_post(&mutex_log);
		abort();
		}




	crearEstructuraFS();
	/*	int sock_listen;
	sock_listen = crearServidorFS(puertoFS, logger);



	int hiloConsola =pthread_create(&th_consola, NULL, (void *) consola_imprimir_encabezado, NULL);

	if(hiloConsola){
					sem_wait(&mutex_logger);
					log_error(logger,"No se pudo crear el hilo Consola\n");
					sem_post(&mutex_logger);
					abort();
			}

 //TODO Descomentar luego de prueba
*/
	//No quiero que termine hasta que se cierre la consola
	pthread_join(th_consola, NULL);
	pthread_join(th_listener, NULL);



    log_info(logger, "<<Proceso FileSystem Finalizado>>");
    log_destroy(logger);
    return 1;

}
