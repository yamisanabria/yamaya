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

#define NODO_CAIDO 1
t_log* logger;
t_config* configuration;
char* ipYAMA;
char* ipFS;
int puertoFS;
int socketDN;
int socketYama;
uint16_t puertoYAMA;
t_directorio* directorio_actual;
uint64_t espacioDisponible;
uint8_t id_nodo;
char* ruta_fs;
char* nombDirectorio;
char* nuevaCadena;
t_list* listas_bloques_de_archivos;
t_list* lista_nodos_consola;
t_list* lista_nodos;

int configOk=1;

int conectar_servidor (int puerto, t_log* logger){

	//inicializo parametros

	int sock = 0;

	struct sockaddr_in direccionServidor;
		direccionServidor.sin_family = AF_INET;
		direccionServidor.sin_addr.s_addr = INADDR_ANY;
		direccionServidor.sin_port = htons(puerto);
	memset(&(direccionServidor.sin_zero), 0, 8);

	log_info(logger, "[Servidor] Consiguiendo datos de red...");

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

	log_info(logger, "[Servidor] Socket creado correctamente.");
	return sock;
}

int conexion_nueva(int new_socket) {

	t_mensaje msg;
	memset(&msg, '\0', sizeof(t_mensaje));
	char* buffer;
	int cantBytes;
	if ((buffer = (char*) malloc(sizeof(char) * SIZE_MSG)) == NULL) {
		log_error(logger, "error al reservar memoria para el buffer");
		return -1;
	}

	memset(buffer, '\0', SIZE_MSG);
	if ((cantBytes = recv(new_socket, buffer, SIZE_MSG, 0)) <= 0) {
		log_error(logger, "error en el recv en el socket");
		close(new_socket);
		return -1;
	}
	log_info(logger, "Información Recibida desde el DataNode.");

	memcpy(&msg, buffer, SIZE_MSG);
	if (msg.tipo == HANDSHAKE && msg.id_proceso == DATANODE) {
		log_info(logger, "creando conexion con DATANODE");
		socketDN = new_socket;
		conexion_datanode(socketDN);

	} else if (msg.tipo == HANDSHAKE && msg.id_proceso == YAMA) {
		log_info(logger, "creando conexion con YAMA");
		pthread_t th_yama;
		socketYama = new_socket;
		pthread_create(&th_yama, NULL, (void *) conexion_yama,
				(void *) &socketYama);

	}
	free(buffer);
	return 0;
}

int conexion_datanode(int sockM) {

	//enviar handshake ok

	t_mensaje mensaje;
	t_datanode nodoRecibido;

	memset(&mensaje, 0, sizeof(t_mensaje));
	int size_mensaje = sizeof(t_mensaje);
	char* buffer;
	int numberBytes = 0;

	if ((buffer = (char*) malloc(sizeof(char) * size_mensaje)) == NULL) {
		log_error(logger, "error al reservar memoria para el buffer de DATANODE");
		return -1;
	}

	mensaje.tipo = HANDSHAKEOK;
	mensaje.id_proceso = FILESYSTEM;
	memset(buffer, '\0', size_mensaje);
	memcpy(buffer, &mensaje, size_mensaje);
	if ((numberBytes = send(sockM, buffer, size_mensaje, 0)) <= 0) {
		log_error(logger, "error al enviar el mensaje al DATANODE");
		return -1;
	}

	log_info(logger, "logro conexion correcta con DATANODE");
	log_info(logger, "socket de DATANODE: %d", sockM);
	int8_t estado = recibirYDeserializar(&nodoRecibido, sockM);

	 if (estado != -1) {
		 sem_wait(&mutex_logger);
	 	 log_debug(logger, "Deserialice un nodo\n");
	 	 sem_post(&mutex_logger);
	 	 agregarNodoAestructura(&nodoRecibido, sockM);
	}else {
		printf("Fallo el deserializado...(¡Verifica codigo!)");
	}


	free(buffer);
	return 0;

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

	t_nodoInterno* nodo_viejo = nodoBuscadoEnListaNodosPorIPYPuerto(ipNodo, puerto);

	if (nodo_viejo != NULL) {
	log_error(logger,"El Nodo conectado contiene mismo ip y puerto que nodo %s por lo tanto nodo %s es eliminado", nodo_viejo->nombre_nodo, nodo_viejo->nombre_nodo);

	//	desconexionDeNodo(nodo_viejo->socket_nodo);
	//	eliminarNodoDePartes(nodo_viejo);
	//	eliminarNodoDeListaNodos(nodo_viejo->socket_nodo);
	//	quitarNodoDeLista(nodo_viejo->socket_nodo);

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
		fprintf(bin,"%sTotal=%i\n",nodo->nombre_nodo,cantBloques);
		fprintf(bin,"%sLibre=%i\n",nodo->nombre_nodo,0);

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
		log_error(logger, "Error al abrir archivo bitmap de %s\n", nodo->nombre_nodo);
		abort();
	}else{
		log_info(logger, "Archivo bitmap abierto correctamente de %s", nodo->nombre_nodo);
	   	}



	  /*  char *code;
	    size_t n = 0;
	    int c;
	    if (bitmap == NULL)
	        return 0; //could not open file
	    code = malloc(1000);
	    while ((c = fgetc(bitmap)) != EOF)
	    {
	        code[n++] = (char) c;
	    }
	    // don't forget to terminate with the null character
	    code[n] = '\0';
	    int posicion=0;
	    int cantidadDe=0;
	    while(strcmp(code[posicion],"\0")!=0){
	    	if(strcmp(code[posicion],"0")==0){
	    		cantidadDe++;
	    	}
	    }
	    	posicion++;
	    }
	    printf("Devolvió = %s",code);*/

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


				log_info(logger, "La IP del File System es: %s", ipFS);

			} else {

				log_error(logger, "Error al obtener la IP del File System");

				configOk = 0;
			}

			if (config_has_property(configuration, "PUERTO_FS")) {

				puertoFS = config_get_int_value(configuration, "PUERTO_FS");

				log_info(logger, "El puerto del File System es: %i", puertoFS);

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

	uint8_t i;
	uint16_t j;


		for(i=0;i<list_size(lista_nodos);i++){
			t_nodo_fs* nodo= list_get(lista_nodos,i);

			for(j=0;j<nodo->cantBloques;j++){
				bitarray_clean_bit(nodo->bloquesNodo, j);
			}

		}

		list_clean(lista_directorios);
		list_clean(listas_bloques_de_archivos);

		remove("directorios");
		remove("bloques_partes");
		remove("var_global");


		ruta_fs = "mdfs";
		id_directorio = 0;

		t_directorio* directorio = malloc(sizeof(t_directorio));
		directorio->index = 0;
		directorio->index = 1024;
		//directorio->nombre = "fs";
		list_add(lista_directorios, directorio);

		directorio_actual = directorio;



		persistirBloquesPartes();
		persistirDirectorios();
		persistirNodos();
		persistirVarGlobales();
		sem_wait(&mutex_logger);
		log_info(logger, "FS formateado");
		sem_post(&mutex_logger);

		mostrarEspacioDispo();
	}

void freearArchivo(t_file* archivo){
	free(archivo->nombre);
	free(archivo);
}


void eliminar_arch_etc(char* nomArchivo){
	bool archivoTengaMismoNombre(t_file* archivo) {
		return (string_equals_ignore_case(archivo->nombre, nomArchivo))
				&& (directorio_actual->id_directorio == archivo->id_directorio)
				&& (archivo->disponible == 0);
	}

	if (list_any_satisfy(listas_bloques_de_archivos,(void*) archivoTengaMismoNombre)) {
		t_file* archivo=list_find(listas_bloques_de_archivos,(void*) archivoTengaMismoNombre);
		borrarBloquesDePartes(archivo->bloques);
		list_remove_and_destroy_by_condition(listas_bloques_de_archivos,(void*) archivoTengaMismoNombre, (void*) freearArchivo);
		sem_wait(&mutex_logger);
		log_info(logger, "Archivo %s eliminado", nomArchivo);
		sem_post(&mutex_logger);
		persistirBloquesPartes();
		persistirNodos();
		mostrarEspacioDispo();


	} else {
		sem_wait(&mutex_logger);
		log_error(logger, "No se puede eliminar el archivo");
		sem_post(&mutex_logger);
	}
}

void renombrar_arch_dir(char* Archivo, char* nomNuevo){


		char* ruta_fs;
		printf("ingrese la ruta del archivo: \n");
		ruta_fs = readline(">");
		//strcpy(&ruta,ruta);

		 FILE *archivo;
		 	    archivo = fopen(ruta_fs,"w");
		   // fprintf(archivo,"Hola Mundo");
		    //fclose(archivo);

		 	    char* nuevoNombre;

		    	printf("ingrese el nuevo nombre: \n");

		    	nuevoNombre = readline(">");

		    	char* v_nueva_ruta = retrocedoHastaLaBarra(ruta_fs);
		    	printf ("A;%s",v_nueva_ruta);

				char* finalNombre;

		    	finalNombre=strcat(v_nueva_ruta, nuevoNombre);

		    	printf ("A;%s",ruta_fs);
		    	printf ("B;%s",finalNombre);

		    if(rename(ruta_fs,finalNombre)==0)// Renombramos el archivo
		        printf("El archivo se renombro satisfactoriamente\n");

		    else
		        printf("No se pudo renombrar el archivo\n");

		    fclose(archivo);
		    leer_palabra();
	}


char* retrocedoHastaLaBarra(p_ruta_fs) {



	    		char** arrayCadena= string_split(p_ruta_fs, "/");
	    		int i = 0;
	    		while (arrayCadena[i] != NULL) {
	    			i++;
	    		}
	    		char* nuevaCadena = string_new();
	    		int j;
	    	 		for (j = 0; j < i -1; j++) {
	    	 			if(j==0){
	    	 				string_append(&nuevaCadena,"/");
	    	 			}
	    			string_append(&nuevaCadena, strcat(arrayCadena[j], "/"));
	    		}
	    	 			  		return nuevaCadena;
	    	}

void mostrar_cont_arch(char* Archivo){


	char* ruta_fs;
	printf("ingrese la ruta del archivo: \n");
	ruta_fs = readline(">");

					FILE *archivo;
			 	    archivo = fopen(ruta_fs,"r");



		if( archivo==NULL )
			printf("Error al abrir el Archivo\n");

		else
		{
			int ch;

			while ((ch=getc(archivo))!=EOF)
				putc(ch, stdout);

		}
}

void crear_directorio(char* nomDirectorio){



	log_info(logger,"Entré a crear directorio");


		t_directorio* directorioNuevo = malloc(sizeof(t_directorio));

		bool nombreRepetidoConMismoPadre(t_directorio* directorio) {
			log_info(logger,"verificando Nombre repetido");
			return string_equals_ignore_case(directorio->nombre, nomDirectorio)&& (directorio_actual->index == directorio->padre);
		}

		if (list_any_satisfy(lista_directorios,
				(void*) nombreRepetidoConMismoPadre)) {
			sem_wait(&mutex_logger);
			log_error(logger, "Intente con otro nombre");
			sem_post(&mutex_logger);
		} else {
			id_directorio++;
			strcpy(directorioNuevo->nombre,nomDirectorio);
			directorioNuevo->padre = directorio_actual->padre;
			directorioNuevo->index = id_directorio;
			list_add(lista_directorios, directorioNuevo);
			sem_wait(&mutex_logger);
			log_info(logger, "Se creo el directorio %s", nomDirectorio);
			sem_post(&mutex_logger);
			persistirDirectorios();

	}
}

void copia_arch_local(){
	printf("copiar un archivo local al yamafs\n");
}

void crear_copia_bloque(){
	printf("copia de un bloque de un archivo\n");
}

void solicitar_md5(char* nomArchivo){

	bool archivoTengaMismoNombre(t_file* archivo) {

				return (string_equals_ignore_case(archivo->nombre, nomArchivo))
						&& (directorio_actual->id_directorio == archivo->id_directorio)
						&& (archivo->disponible == 0);
			}
	if (list_any_satisfy(listas_bloques_de_archivos,(void*) archivoTengaMismoNombre)) {
		t_file* archivo = list_find(listas_bloques_de_archivos, (void*) archivoTengaMismoNombre);

			char* ruta_temporal = string_new();
			string_append(&ruta_temporal, "/tmp/");
			string_append(&ruta_temporal, string_itoa((int) time(NULL)));
			string_append(&ruta_temporal, "-");
			string_append(&ruta_temporal, nomArchivo);

			FILE* archivoNuevo = fopen(ruta_temporal, "w");
			uint8_t i = rearmarArchivo(nomArchivo, archivoNuevo, archivo);
			persistirBloquesPartes();
			persistirNodos();
			if (i != 0) {
				sem_wait(&mutex_logger);
				log_error(logger, "No se pudo rearmar el archivo");
				sem_post(&mutex_logger);
			} else {
				fclose(archivoNuevo);

				char* comando = string_new();
				string_append(&comando, "md5sum ");
				string_append(&comando, ruta_temporal);

				system(comando);
			}
		} else {
			sem_wait(&mutex_logger);
			log_error(logger, "No se encuentra archivo %s en el directorio actual o no esta disponible",nomArchivo);
			sem_post(&mutex_logger);
		}
}

uint8_t rearmarArchivo(char* nomArchivo, FILE* archivoNuevo,t_file* archivoARearmar) {
	uint8_t i;
	t_list* partes = list_create();
	list_add_all(partes, archivoARearmar->archivoDisponible);
	uint32_t cant_partes = list_size(partes);
	char* ip = string_new();
	uint16_t puerto;
	char* contenido_bloque;

	t_nodo_bloque* nodo_bloque;
	t_nodo_fs* nodo;
	t_parte* parte;

	for (i = 0; i < cant_partes; i++) {
		parte = list_get(partes, i);
		uint16_t nro_parte = parte->nro_parte_archi;

		nodo_bloque = obtenerNodoQueTengaParte(nro_parte,archivoARearmar);
		if (nodo_bloque == NULL) {
			return 1;
		}
		ip = nodo_bloque->ip;
	    puerto = nodo_bloque->puerto;
		nodo = nodoBuscadoEnListaPorIPPuerto(ip, puerto);
		if (nodo == NULL) {
			return 1;
		}
		contenido_bloque = obtenerElContenidoDeUnBloque(nodo_bloque->nro_bloque_nodo, nodo);

		while (atoi(contenido_bloque) == 1) {
			desconexionDeNodo(nodo->socket_nodo);
			eliminarNodoDePartes(nodo);
			quitarNodoDeLista(nodo->socket_nodo);

			nodo_bloque = obtenerNodoQueTengaParte(nro_parte,archivoARearmar);
			if (nodo_bloque == NULL) {
				return 1;
			}
			ip = nodo_bloque->ip;
			puerto = nodo_bloque->puerto;

			nodo = nodoBuscadoEnListaPorIPPuerto(ip, puerto);
			if (nodo == NULL) {
				return 1;
			}
			contenido_bloque = obtenerElContenidoDeUnBloque(nodo_bloque->nro_bloque_nodo, nodo);
		}

		fwrite(contenido_bloque, sizeof(char), string_length(contenido_bloque),archivoNuevo);
		free(contenido_bloque);
	}

	return 0;
}
t_nodo_bloque* obtenerNodoQueTengaParte(uint16_t nro_bloque_archi,t_file* archivoARearmar) {

	bool tieneEsaParte(t_nodo_bloque* unNodoBloque) {

		bool tieneAlNodo(t_nodo_fs* nodo) {
			return nodo->id_nodo == unNodoBloque->id_nodo;
		}

		bool estaEnLaListaDePartes(t_parte* parte) {
			return parte->nro_parte_archi == nro_bloque_archi && list_any_satisfy(parte->lista_nodos_parte,(void*) tieneAlNodo);
		}

		return unNodoBloque->nro_bloque_archi == nro_bloque_archi&& list_any_satisfy(archivoARearmar->disponible,(void*) estaEnLaListaDePartes);
	}
	t_nodo_bloque* nodo_bloque = list_find(archivoARearmar->bloques,(void*) tieneEsaParte);
	return nodo_bloque;
}

t_nodo_fs* nodoBuscadoEnListaPorIPPuerto(char* ip, uint16_t puerto) {
	bool cumpleConIPPUERTO(t_nodo_fs* nodo) {
		return (nodo->puerto == puerto) && string_equals_ignore_case(nodo->ip, ip);
	}
	t_nodo_fs* nodo = list_find(lista_nodos_consola, (void*) cumpleConIPPUERTO);
	return nodo;
}

t_nodo_fs* nodoBuscadoEnListaNodosPorSocket(uint8_t socket) {
	bool cumpleConElSOCKET(t_nodo_fs* nodo) {
		return (nodo->socket_nodo == socket);
	}
	t_nodo_fs* nodo = list_find(lista_nodos, (void*) cumpleConElSOCKET);
	return nodo;
}

void desconexionDeNodo(uint8_t nodo_socket) {
	uint8_t codop = NODO_CAIDO;
	t_nodo_fs* nodo = nodoBuscadoEnListaNodosPorSocket(nodo_socket);
	sem_wait(&mutex_logger);
	//send(marta_socket, &codop, sizeof(cod_operacion), 0);
	//send(marta_socket, &nodo->id_nodo, sizeof(cod_operacion), 0);
	sem_post(&mutex_logger);
	validarSiArchivoSigueDispo(nodo);
}

void validarSiArchivoSigueDispo(t_nodo_fs* nodo_desconec) {
	uint8_t i;
	bool tieneunNodo(t_parte* parte) {
		if (list_size(parte->lista_nodos_parte) == 1) {
			t_nodo_fs* nodo = list_get(parte->lista_nodos_parte, 0);
			return (nodo->puerto == nodo_desconec->puerto)&& (string_equals_ignore_case(nodo->ip, nodo_desconec->ip));
		} else
			return false;
	}
	bool tienenUnSoloNodoySeDesconecta(t_file* bloques_archivo) {
		return list_any_satisfy(bloques_archivo->archivoDisponible,(void*) tieneunNodo) && bloques_archivo->disponible==0;
	}
	t_list* archivosNoDisponibles = list_filter(listas_bloques_de_archivos,(void*) tienenUnSoloNodoySeDesconecta);
	uint8_t cant_archivos = list_size(archivosNoDisponibles);

	if (cant_archivos > 0) {
		for (i = 0; i < cant_archivos; i++) {
			t_file* archivo = list_get(archivosNoDisponibles, i);
			archivo->disponible = 1;
			sem_wait(&mutex_logger);
			log_info(logger, "Archivo %s no disponible", archivo->nombre);
			sem_post(&mutex_logger);
		}
	}
}

t_directorio* obtenerDirectorioIngresado(char* rutaDestino) {

	if(rutaDestino[0] == '/'){
		rutaDestino= string_substring_from(rutaDestino,1);
	}

	char** ruta_archi = string_split(rutaDestino, "/");
	int8_t i = 0;

	while (ruta_archi[i] != NULL) {
		i++;
	}

	bool esElDirectorioRaiz(t_directorio* dir) {
		return dir->padre == 1024;
	}

	t_directorio** directorios_ruta = malloc(sizeof(t_directorio) * i);
	int8_t j;

	for (j = 0; j < i; j++) {

		if (string_equals_ignore_case(ruta_archi[0], "mdfs") && j == 0) {
			directorios_ruta[0] = list_find(lista_directorios,
					(void*) esElDirectorioRaiz);

		} else if (j == 0) {

			bool esDirectorioHijoDelActual(t_directorio* dir) {
				return string_equals_ignore_case(dir->nombre, ruta_archi[0])
						&& dir->padre== directorio_actual->padre;
			}

			directorios_ruta[0] = list_find(lista_directorios,(void*) esDirectorioHijoDelActual);
		} else {

			bool esHijoDelAnterior(t_directorio* dir) {
				return string_equals_ignore_case(dir->nombre, ruta_archi[j])
						&& dir->index == directorios_ruta[j - 1]->index;
			}

			directorios_ruta[j] = list_find(lista_directorios,(void*) esHijoDelAnterior);
		}
	}
	uint16_t id_dir = directorios_ruta[j - 1]->index;

	t_directorio* directorio_buscado = malloc(sizeof(t_directorio));
	bool cumpleConIDingresado(t_directorio* directorio) {
			return directorio->index ==id_dir;
		}
	directorio_buscado= list_find(lista_directorios,(void*)cumpleConIDingresado);
	return directorio_buscado;

}



void mover_arch(char* nomArchivo, char* rutaDestino){


		bool archivoTengaMismoNombre(t_file* archivo) {
			return (string_equals_ignore_case(archivo->nombre, nomArchivo))
					&& (directorio_actual->id_directorio == archivo->id_directorio)
					&& (archivo->disponible == 0);
		}

		if (list_any_satisfy(listas_bloques_de_archivos,
				(void*) archivoTengaMismoNombre)) {
			if (obtenerDirectorioIngresado(rutaDestino) != NULL) {
				t_file* archivoAMover = list_find(listas_bloques_de_archivos,(void*) archivoTengaMismoNombre);


				t_directorio* directorioDestino = obtenerDirectorioIngresado(rutaDestino);
				archivoAMover->id_directorio= directorioDestino->index;
				sem_wait(&mutex_logger);
				log_info(logger, "El archivo %s fue movido a %s ", nomArchivo, rutaDestino);
				sem_post(&mutex_logger);

				persistirBloquesPartes();
			} else {
				sem_wait(&mutex_logger);
				log_error(logger, "No se encuentra el directorio ingresado");
				sem_post(&mutex_logger);
			}
		} else {
			sem_wait(&mutex_logger);
			log_error(logger, "No se puede mover el archivo seleccionado");
			sem_post(&mutex_logger);
		}

}

void mover_directorio(char* nomDir, char* rutaDestino) {
	char** ruta_archi = string_split(rutaDestino, "/");
	int8_t i = 0;

	while (ruta_archi[i] != NULL) {
		i++;
	}

	bool esElDirectorioRaiz(t_directorio* dir) {
		return dir->padre == 1024;
	}

	t_directorio** directorios_ruta = malloc(sizeof(t_directorio) * i);
	int8_t j;

	for (j = 0; j < i; j++) {
		if (string_equals_ignore_case(ruta_archi[0], "fs") && j == 0) {
			directorios_ruta[0] = list_find(lista_directorios,(void*) esElDirectorioRaiz);
		} else if (j == 0) {
			bool esDirectorioHijoDelActual(t_directorio* dir) {
				return string_equals_ignore_case(dir->nombre, ruta_archi[0]) && dir->padre == directorio_actual->index;
			}
			directorios_ruta[0] = list_find(lista_directorios,(void*) esDirectorioHijoDelActual);
		} else {
			bool esHijoDelAnterior(t_directorio* dir) {
				return string_equals_ignore_case(dir->nombre, ruta_archi[j]) && dir->padre == directorios_ruta[j - 1]->index;
			}
			directorios_ruta[j] = list_find(lista_directorios,(void*) esHijoDelAnterior);
		}
	}

	bool esElDirectorioBuscado(t_directorio* directorio) {
		return string_equals_ignore_case(directorio->nombre, nomDir) && directorio->padre== directorio_actual->index;
	}

	if (directorios_ruta[j - 1] != NULL) {
		if (list_any_satisfy(lista_directorios,(void*) esElDirectorioBuscado)) {
			t_directorio* dir = list_find(lista_directorios,(void*) esElDirectorioBuscado);
			dir->padre = directorios_ruta[j - 1]->index;
			sem_wait(&mutex_logger);
			log_info(logger, "Se ha movido el directorio %s a %s", nomDir,rutaDestino);
			sem_post(&mutex_logger);
			persistirDirectorios();
		} else {
			sem_wait(&mutex_logger);
			log_error(logger, "No se ha encontrado el directorio a mover");
			sem_post(&mutex_logger);
		}
	} else {
		sem_wait(&mutex_logger);
		log_error(logger, "La ruta destino no existe");
		sem_post(&mutex_logger);
	}

	free(directorios_ruta);
}

void listar_archivos(){
	printf("listar los archivos de un directorio\n");
}

void mostrar_info(){
	printf("mostrar información\n");
}
char* Archivo;
char* nuevoNombre;
char* ruta;
void reconocer_comando(char * linea){ //reconoce comando

	int flag_comandoOK =0;
	char dirarch1[250];
	int dirarch2;
	fflush(stdin);

	if(flag_comandoOK==0 && strcmp(linea, "format")==0){
        formatear_filesystem();
	}else if(flag_comandoOK==0 && strcmp(linea, "rm")==0){
	    eliminar_arch_etc(Archivo);
	   }else if(flag_comandoOK==0 && strcmp(linea, "rename")==0){
		renombrar_arch_dir(Archivo,nuevoNombre);
	   }else if(flag_comandoOK==0 && strcmp(linea, "mv")==0){
		   mover_arch(Archivo, ruta);
	   }else if(flag_comandoOK==0 && strcmp(linea, "md")==0){
		   mover_directorio(Archivo, ruta);
	   }else if(flag_comandoOK==0 && strcmp(linea, "cat")==0){
		   mostrar_cont_arch(Archivo);
	    }else if(flag_comandoOK==0 && strcmp(linea, "mkdir")==0){

	    	printf("\nIngrese el nombre del nuevo directorio:\n\n>");
            nombDirectorio = malloc(255);

	        fgets(nombDirectorio, 255, stdin);
	        strtok(nombDirectorio,"\n");
	        printf("\nIngrese el nombre del nuevo directorio:\n\n>");
	    	crear_directorio(nombDirectorio);

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
	    	solicitar_md5(Archivo);
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
	sem_init(&mutex_logger, 0, 1);

	listaNodosActivos=list_create();
	listaNodosEsperando=list_create();
	listaNodosDesconectados=list_create();
	tablaDeArchivos=list_create();
	punteroNodoAllenar=0;
	nodoParaYama=list_create();
	listaNombreNodos=list_create();
	lista_directorios=list_create();

	tamanioLibreNodos = 0;
	tamanioTotalNodos = 0;


	id_directorio = 0;

	ruta_fs = "yamafs";

	directorio_actual = malloc(sizeof(t_directorio));
	directorio_actual->padre = -1;
	directorio_actual->index = 0;
	strcpy(directorio_actual->nombre,"Root");


	FILE* archivo_directorio;

	if ((archivo_directorio = fopen("/home/utnso/metadata/directorios.dat", "r")) != NULL){

		levantarDirectorios();
		log_info(logger, "levantando directorio\n");

	}else{

	persistirDirectorios();
	log_info(logger, "persistido del if\n");
	list_add(lista_directorios, directorio_actual);

	}

	log_info(logger,"Estructuras creadas correctamente");



}

void borrarBloquesDePartes(t_list* listaBloquesDelArchivo) {
	uint32_t bloquesAborrar = list_size(listaBloquesDelArchivo);
	uint32_t i;
	for (i = 0; i < bloquesAborrar; i++) {
		t_nodo_bloque* bloqueAborrar = list_get(listaBloquesDelArchivo, i);
		uint16_t nro_bloque = bloqueAborrar->nro_bloque_nodo;
		char* ip = bloqueAborrar->ip;
		uint16_t puerto = bloqueAborrar->puerto;
		t_nodo_fs* nodo = nodoBuscadoEnListaNodosPorIPYPuerto(ip, puerto);
		bitarray_clean_bit(nodo->bloquesNodo, nro_bloque);
		free(bloqueAborrar);
	}

}

void mostrarEspacioDispo(){
	uint8_t i;
	espacioDisponible=0;

	for(i=0;i<list_size(lista_nodos_consola); i++){
		t_nodo_fs* nodo =list_get(lista_nodos_consola,i);
		espacioDisponible += (bloquesLibresDeUnNodo(nodo)) * 20;
	}

	sem_wait(&mutex_logger);
	log_info(logger, "Espacio disponible: %ld MB", espacioDisponible);
	sem_post(&mutex_logger);
}

void persistirDirectorios() {
	FILE* archi_directorios = fopen("/home/utnso/metadata/directorios.dat", "wt");
	uint16_t cant_dir = list_size(lista_directorios);
	//fwrite(&cant_dir, sizeof(uint16_t), 1, archi_directorios);
	uint16_t i;
	for (i = 0; i < cant_dir; i++) {
		t_directorio* dire = list_get(lista_directorios, i);
		fprintf(archi_directorios,"%i",dire->index);
		fprintf(archi_directorios," ");
		fprintf(archi_directorios,"%s", dire->nombre);
		fprintf(archi_directorios," ");
		fprintf(archi_directorios,"%i",dire->padre);
		fprintf(archi_directorios,"\n");
	}
	log_info(logger, "OK persistir\n");



	fclose(archi_directorios);
}

void persistirBloquesPartes() {
	FILE* archi_bloques_partes = fopen("bloques_partes", "wb");
	uint16_t cant_bloques_partes = list_size(listas_bloques_de_archivos);
	fwrite(&cant_bloques_partes, sizeof(uint16_t), 1, archi_bloques_partes);

	uint16_t i;

	for (i = 0; i < cant_bloques_partes; i++) {
		t_file* bloques_partes = list_get(listas_bloques_de_archivos, i);

		uint8_t tam_nom = string_length(bloques_partes->nombre) + 1;
		fwrite(&tam_nom, sizeof(uint8_t), 1, archi_bloques_partes);
		fwrite(bloques_partes->nombre, sizeof(char), tam_nom,archi_bloques_partes);

		uint16_t tam_ruta = string_length(bloques_partes->ruta) + 1;
		fwrite(&tam_ruta, sizeof(uint16_t), 1, archi_bloques_partes);
		fwrite(bloques_partes->ruta, sizeof(char), tam_ruta,archi_bloques_partes);

		fwrite(&bloques_partes->id_directorio, sizeof(uint16_t), 1,archi_bloques_partes);
		fwrite(&bloques_partes->tamanio, sizeof(uint32_t), 1,archi_bloques_partes);
		fwrite(&bloques_partes->disponible, sizeof(uint8_t), 1,archi_bloques_partes);

		uint16_t j;
		uint16_t cant_nodos_bloques = list_size(bloques_partes->bloques);
		fwrite(&cant_nodos_bloques, sizeof(uint16_t), 1, archi_bloques_partes);

		for (j = 0; j <cant_nodos_bloques; j++) {

			t_nodo_bloque* nodo_bloque = list_get(bloques_partes->bloques, j);
			uint8_t tam_nom_bloque = string_length(nodo_bloque->nombre_archivo)+ 1;

			fwrite(&tam_nom_bloque, sizeof(uint8_t), 1, archi_bloques_partes);
			fwrite(nodo_bloque->nombre_archivo, sizeof(char), tam_nom_bloque,archi_bloques_partes);
			fwrite(&nodo_bloque->nombre_archivo_long, sizeof(uint8_t), 1,archi_bloques_partes);
			fwrite(&nodo_bloque->nro_bloque_archi, sizeof(uint16_t), 1,archi_bloques_partes);
			fwrite(&nodo_bloque->nro_bloque_archi_long, sizeof(uint16_t), 1,archi_bloques_partes);
			fwrite(&nodo_bloque->id_nodo, sizeof(uint8_t), 1,archi_bloques_partes);
			fwrite(&nodo_bloque->id_nodo_long, sizeof(uint8_t), 1,archi_bloques_partes);
			fwrite(&nodo_bloque->nro_bloque_nodo, sizeof(uint16_t), 1,archi_bloques_partes);
			fwrite(&nodo_bloque->nro_bloque_nodo_long, sizeof(uint16_t), 1,archi_bloques_partes);

			uint8_t tam_ip = string_length(nodo_bloque->ip) + 1;
			fwrite(&tam_ip, sizeof(uint8_t), 1, archi_bloques_partes);

			fwrite(nodo_bloque->ip, sizeof(char), tam_ip, archi_bloques_partes);
			fwrite(&nodo_bloque->ip_long, sizeof(uint8_t), 1,archi_bloques_partes);
			fwrite(&nodo_bloque->puerto, sizeof(uint16_t), 1,archi_bloques_partes);
			fwrite(&nodo_bloque->puerto_long, sizeof(uint16_t), 1,archi_bloques_partes);
		}

		uint16_t k;
		uint16_t cant_partes = list_size(bloques_partes->archivoDisponible);
		fwrite(&cant_partes, sizeof(uint16_t), 1, archi_bloques_partes);

		for (k = 0; k < list_size(bloques_partes->archivoDisponible); k++) {

			t_parte *parte = list_get(bloques_partes->archivoDisponible, k);
			fwrite(&parte->nro_parte_archi, sizeof(uint16_t), 1,archi_bloques_partes);

			uint16_t h;
			uint16_t cant_nodos_fs = list_size(parte->lista_nodos_parte);
			fwrite(&cant_nodos_fs, sizeof(uint16_t), 1, archi_bloques_partes);

			for (h = 0; h < cant_nodos_fs; h++) {
				t_nodo_fs* nodo = list_get(parte->lista_nodos_parte, h);
				fwrite(&nodo->id_nodo, sizeof(uint8_t), 1,archi_bloques_partes);
				uint8_t tam_ip = string_length(nodo->ip) + 1;
				fwrite(&tam_ip, sizeof(uint8_t), 1, archi_bloques_partes);
				fwrite(nodo->ip, sizeof(char), tam_ip, archi_bloques_partes);
				fwrite(&nodo->puerto, sizeof(uint16_t), 1,archi_bloques_partes);
				fwrite(&nodo->socket_nodo, sizeof(uint8_t), 1,archi_bloques_partes);
				fwrite(&nodo->estado, sizeof(uint8_t), 1, archi_bloques_partes);
				fwrite(&nodo->cantBloques, sizeof(uint16_t), 1,archi_bloques_partes);
				fwrite(&(nodo->bloquesNodo->size), sizeof(size_t), 1,archi_bloques_partes);
				uint16_t r;
				for(r=0;r<nodo->cantBloques;r++){
					uint8_t bit=bitarray_test_bit(nodo->bloquesNodo,r);
					fwrite(&bit, sizeof(uint8_t), 1, archi_bloques_partes);
				}


			}
		}
	}
	fclose(archi_bloques_partes);
}
void persistirNodos() {
	FILE* archi_nodos = fopen("nodos", "wb");
	uint16_t cant_nodos = list_size(lista_nodos);
	fwrite(&cant_nodos, sizeof(uint16_t), 1, archi_nodos);
	uint16_t i;
	for (i = 0; i < cant_nodos; i++) {
		t_nodo_fs* nodo = list_get(lista_nodos, i);

		fwrite(&nodo->id_nodo, sizeof(uint8_t), 1, archi_nodos);
		uint8_t tam_ip = string_length(nodo->ip) + 1;
		fwrite(&tam_ip, sizeof(uint8_t), 1, archi_nodos);
		fwrite(nodo->ip, sizeof(char), tam_ip, archi_nodos);
		fwrite(&nodo->puerto, sizeof(uint16_t), 1, archi_nodos);
		fwrite(&nodo->socket_nodo, sizeof(uint8_t), 1, archi_nodos);
		fwrite(&nodo->estado, sizeof(uint8_t), 1, archi_nodos);
		fwrite(&nodo->cantBloques, sizeof(uint16_t), 1, archi_nodos);
		fwrite(&(nodo->bloquesNodo->size), sizeof(size_t), 1, archi_nodos);

		uint16_t r;
		for(r=0;r<nodo->cantBloques;r++){
			uint8_t bit=bitarray_test_bit(nodo->bloquesNodo,r);
			fwrite(&bit, sizeof(uint8_t), 1, archi_nodos);
		}


	}
	fclose(archi_nodos);
}

void levantarVarGlobales() {
	FILE* archi_var_globales = fopen("var_global", "rb");

	if (archi_var_globales) {
		fread(&id_nodo, sizeof(uint8_t), 1, archi_var_globales);
		fread(&id_directorio, sizeof(uint16_t), 1, archi_var_globales);
		fclose(archi_var_globales);
	} else {
		id_nodo = 0;
		id_directorio = 0;
		list_add(lista_directorios, directorio_actual);

	}
}

void persistirVarGlobales() {
	FILE* archi_var_globales = fopen("var_global", "wb");
	fwrite(&id_nodo, sizeof(uint8_t), 1, archi_var_globales);
	fwrite(&id_directorio, sizeof(uint16_t), 1, archi_var_globales);
	fclose(archi_var_globales);
}
void levantarDirectorios() {

	FILE* archi_directorios = fopen("/home/utnso/metadata/directorios.dat", "rt");

	t_directorio dire;


	while(1){


				if(feof(archi_directorios)!=0)
					break;

				fscanf(archi_directorios,"%i %s %i",&dire.index,dire.nombre,&dire.padre);
				/*printf("Indice: %i\n",dire.index);
				printf("Nombre: %s\n",dire.nombre);
				printf("Padre: %i\n",dire.padre);*/
			}

   fclose(archi_directorios);

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

uint8_t seteoDeBloque(t_nodoInterno* nodo, uint16_t numero_bloque,uint8_t socket_nodo, char* datos) {

	int8_t status;
	uint8_t resultado;

	uint8_t seguir_dividiendo = 0;
	uint8_t abortar = 1;
	uint8_t enviar_otro_nodo = 2;

	codigo_operacion = SET_BLOQUE;

	send(socket_nodo, &codigo_operacion, sizeof(uint8_t),MSG_NOSIGNAL);
	send(socket_nodo, &numero_bloque, sizeof(uint16_t), MSG_NOSIGNAL);

	sem_wait(&mutex_logger);
	log_info(logger, "Envie para setear el bloque %d del nodo %d",numero_bloque, nodo->nombre_nodo);
	sem_post(&mutex_logger);

	uint32_t tam_datos = string_length(datos);
	send(socket_nodo, &tam_datos, sizeof(uint32_t),  MSG_NOSIGNAL);
	send(socket_nodo, datos, tam_datos,MSG_NOSIGNAL);

	status= recv(socket_nodo, &resultado, sizeof(uint8_t), 0);
	if (status == -1 || status==0) {
		//	status= nodoDesconectadoEnviarAOtroNodoOAbortar(nodo,socket_nodo); //TODO NODO DESCONECTADO CHEQUEAR
			if(status== enviar_otro_nodo)return enviar_otro_nodo;
			else if(status== abortar)return abortar;
	}


	if (resultado == 0) {
		sem_wait(&mutex_logger);
		log_info(logger,"La operacion de seteado se realizo correctamente\n");
		sem_post(&mutex_logger);
		bitarray_set_bit(nodo->bloques, numero_bloque);

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
        	log_error(logger, "Error al abrir archivo bitmap de %s\n", nodo->nombre_nodo);
			abort();
		}else{
			log_info(logger, "Archivo bitmap abierto correctamente de %s", nodo->nombre_nodo);
		   	} // seguir. Se necesita setear el archivo bitmap

		} else {
			sem_wait(&mutex_logger);
			log_error(logger, "El seteado no pudo realizarse");
			sem_post(&mutex_logger);
			bitarray_clean_bit(nodo->bloques, numero_bloque);
			return enviar_otro_nodo;
		}

	return seguir_dividiendo;
}

t_list* obtenerNodosMasLibres() {
	t_list* lista_nodos_libres = list_create();
	list_add_all(lista_nodos_libres, listaNodosActivos );
	list_sort(lista_nodos_libres, (void*) comparadorDeBloques);
	return lista_nodos_libres;
}

bool comparadorDeBloques(t_nodoInterno *nodo1, t_nodoInterno *nodo2) {
	return (bloquesLibresDeUnNodo(nodo1) > bloquesLibresDeUnNodo(nodo2));
}

uint16_t bloquesLibresDeUnNodo(t_nodoInterno *nodo) {
	uint16_t i;
	uint16_t j = 0;

	for (i = 0; i < nodo->cant_max_bloques; i++) {
		if (bitarray_test_bit(nodo->bloques, i) == 0) {
			j++;
		}
	}
	return j;
}
void eliminarNodoDePartes(t_nodo_fs* nodoDesconectado) {

	uint8_t cant_archivos = list_size(listas_bloques_de_archivos);
	uint8_t i;
	uint16_t j;

	for (i = 0; i < cant_archivos; i++) {

		t_file* archivo = list_get(listas_bloques_de_archivos,i);
		uint16_t cant_partes = list_size(archivo->archivoDisponible);

		for (j = 0; j < cant_partes; j++) {
			t_parte* parte_archivo = list_get(archivo->archivoDisponible, j);

			bool nodocumpleConipypuerto(t_nodo_fs* nodo_fs) {
				return string_equals_ignore_case(nodoDesconectado->ip, nodo_fs->ip) && nodoDesconectado->puerto==nodo_fs->puerto;
			}
			list_remove_by_condition(parte_archivo->lista_nodos_parte,(void*) nodocumpleConipypuerto);
		}

	}
}

char* obtenerElContenidoDeUnBloque(uint16_t nro_bloque, t_nodoInterno* nodo) {

	uint8_t cod_operacion = GET_BLOQUE;
	uint32_t tamanio_contenido;
	char* contenido_bloque;
	char* contenido;
	char* buffer;
	int buffer_size;
	uint32_t comparacion = 0;
	int8_t status=0;
	uint8_t socket= nodo->sockn;

	sem_wait(&mutex_logger);
	log_info(logger, "Envie solicitud para obtener el bloque %d del nodo %d \n",nro_bloque, nodo->nombre_nodo);
	sem_post(&mutex_logger);

	send(socket, &cod_operacion, sizeof(uint8_t),  MSG_NOSIGNAL);
	send(socket, &nro_bloque, sizeof(uint16_t),  MSG_NOSIGNAL);

	buffer = malloc(buffer_size = sizeof(uint32_t));
	status = recv(socket, buffer, sizeof(uint32_t), 0);

	if (status == 0 || status == -1) {
		return string_itoa(1);
	}

	memcpy(&tamanio_contenido, buffer, buffer_size);
	contenido = string_new();
	contenido_bloque = malloc(tamanio_contenido + 1);
	memset(contenido_bloque, '\0', tamanio_contenido + 1);

	while (comparacion != tamanio_contenido) {
		comparacion += recv(socket, contenido_bloque,
				tamanio_contenido - comparacion, 0);
		if (comparacion == -1) {
			return string_itoa(1);
		}
		string_append(&contenido, contenido_bloque);
		memset(contenido_bloque, '\0', tamanio_contenido + 1);
	}
	free(contenido_bloque);
	sem_wait(&mutex_logger);
	log_info(logger, "Recibi el contenido del bloque %d del nodo %s\n",	nro_bloque, nodo->nombre_nodo);
	sem_post(&mutex_logger);
	return contenido;
}

void quitarNodoDeLista(uint8_t nodo_socket) {

	bool busquedaDeUnNodo(t_nodo_fs* un_nodo) {
		return (un_nodo->socket_nodo == nodo_socket);
	}
	list_remove_by_condition(lista_nodos_consola, (void*) busquedaDeUnNodo);

}

int main(int argc, char *argv[]) {

	pthread_t th_consola;

	cargarConfiguraciones();

	//creo un hilo para escuchar a la consola del File system
	pthread_create(&th_consola, NULL, (void *) consola_imprimir_encabezado, NULL);
	log_info(logger,"se creo hilo para atender consola");

	crearEstructuraFS();

	fd_set read_fds;
	fd_set master;

	int sock_listen = 0;

	sock_listen = conectar_servidor(puertoFS, logger);
	log_info(logger, "se creo socket listen");

	socklen_t longstruct;
		struct sockaddr_in direccion;
		int newsock = 0;

		FD_ZERO(&master);
		FD_ZERO(&read_fds);

		//seteo en el select el socket del file system y el socket que hace el listen de los DataNodos y Yama

		FD_SET(sock_listen, &master);

		//busco el mayor para el fdmax del select

	//	int fdmax = sock_listen;

	//	while(1){
	//		sinSize = sizeof(struct sockaddr_in);
	//		if((newsock=accept(sock_listen, (struct sockaddr *) &their_addr, &sinSize))== -1)
	//		{
	//			log_error(logger,"error en accept");
	//			continue;
	//		}
	//		pthread_create(&conexiones[cantidad_conexiones],NULL,(void *) conexion_nueva,
	//				       (void *) &newsock);
	//		cantidad_conexiones++;
	//
	//	}

	//	int i;
		while (1) {
//			read_fds = master;
//			//pthread_mutex_lock(&mutex_list_principal);
//			if (select(fdmax + 1, &read_fds, NULL, NULL, NULL) == -1) {
//				log_error(logger, "Error en select");
//				return -1;
//			}
//			puts("alkascnlknvslknasvlknasvlknaslvknlasv");
//			//pthread_mutex_unlock(&mutex_list_principal);
//		for (i = 0; i <= fdmax; i++) {
				//busco socket
//				if (FD_ISSET(i, &read_fds)) {
//					if (i == sock_listen) //nueva conexion
//							{

						if ((newsock = accept(sock_listen,
								(struct sockaddr *) &direccion, &longstruct)) == -1) {
							log_error(logger, "error en accept");
							continue;
						}

						if (newsock == -1) {
							//si es -1 fallo asi que continuo descartando esta
							log_error(logger,
									"No se pudo agregar una nueva conexion");
							continue;
						}

						log_info(logger, "sock con: %d", newsock);
						conexion_nueva(newsock);

		}

	pthread_join(th_consola, NULL);

	close(sock_listen);
    log_info(logger, "<<Proceso FileSystem Finalizado>>");
    log_destroy(logger);
    return 1;

}
