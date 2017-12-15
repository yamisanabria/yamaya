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
char* nombDirectorio;
int configOk=1;
uint8_t socket_yama;

uint16_t cantidad_bloques;
uint16_t cantidad_bloques_con_copia;
uint32_t tamanio_espacio_datos;
char* mapeo;
char* dir_temporal;
uint16_t numerodebloque;

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

int conexion_datanode(int sockM) {

	t_mensaje mensaje;
	t_datanode nodoRecibido;
	uint8_t cod_op;
	int8_t status = 1;
	int8_t status2;
	int8_t status3 = 0;

	memset(&mensaje, 0, sizeof(t_mensaje));
	int size_mensaje = sizeof(t_mensaje);
	char* buffer;

	if ((buffer = (char*) malloc(sizeof(char) * size_mensaje)) == NULL) {
		log_error(logger, "Error al reservar memoria para el buffer de DATANODE");
		return -1;
	}

	int8_t estado = recibirYDeserializar(&nodoRecibido, sockM);

		 if (estado != -1) {
			 sem_wait(&mutex_logger);
		 	 log_debug(logger, "Deserialice un nodo\n");
		 	 sem_post(&mutex_logger);
		 	 status3 = agregarNodoAestructura(&nodoRecibido, sockM);
		}else {
			printf("Fallo el deserializado...(¡Verifica codigo!)");
		}

		if(status3 == -1){
			log_info(logger, "No se puede conectar DATANODE");
			return -1;
		}else{
			log_info(logger, "logro conexion correcta con DATANODE");
			log_info(logger, "socket de DATANODE: %d", sockM);
		}

	free(buffer);

	while(status){

		status2= recv(sockM, &cod_op, sizeof(uint8_t), 0);
		if (status2 == 0 || status2 == -1) {
			sem_wait(&mutex_logger);
			log_error(logger, "DATANODE: %d se ha desconectado.", sockM);
			sem_post(&mutex_logger);
			status=0;

			eliminarNodo(sockM);

		}
	}


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

void eliminarNodo(int sockND){

	desconexionDeNodo(sockND);
	//	eliminarNodoDePartes(nodo_viejo);
	//	eliminarNodoDeListaNodos(nodo_viejo->socket_nodo);
	//	quitarNodoDeLista(nodo_viejo->socket_nodo);

}

void desconexionDeNodo(int nodo_socket) {
	uint8_t codop = NODO_CAIDO;
	t_nodoInterno* nodo = nodoBuscadoEnListaNodosPorSocket(nodo_socket);

	//TODO mandar a YAMA código de NODO_CAIDO

	//validarSiArchivoSigueDispo(nodo);

	bool busquedaDeUnNodo(t_nodoInterno* un_nodo) {
			return (un_nodo->sockn == nodo_socket);
		}
	list_remove_by_condition(listaNodosActivos, (void*) busquedaDeUnNodo);
	list_add(listaNodosDesconectados, nodo);

}

t_nodoInterno* nodoBuscadoEnListaNodosPorSocket(int socketNodo) {
	bool cumpleConElSOCKET(t_nodoInterno* nodo) {
		return (nodo->sockn == socketNodo);
	}
	t_nodoInterno* nodo = list_find(listaNodosActivos, (void*) cumpleConElSOCKET);
	return nodo;
}

int agregarNodoAestructura(t_datanode* nodoRecibido, int sockN){

	log_info(logger,"Recibí de DATANODE: ip=%s, puerto=%d, nombre=%s, cantidad bloques=%d\n",nodoRecibido->ipNodo,nodoRecibido->puertoNodo ,nodoRecibido->nombreNodo, nodoRecibido->cantidad_bloques);

	uint16_t cantBloques = nodoRecibido->cantidad_bloques;
	char* ipNodo;
	ipNodo = nodoRecibido->ipNodo;
    uint16_t puerto = nodoRecibido->puertoNodo;
	char* nombre = nodoRecibido->nombreNodo;

	uint16_t i;

	t_nodoInterno *nodo = malloc(sizeof(t_nodoInterno));

	t_nodoInterno* nodo_viejo = nodoBuscadoEnListaNodosActivos(nodoRecibido->ipNodo, nodoRecibido->puertoNodo,nodoRecibido->nombreNodo);

	if (nodo_viejo != NULL) {
	log_error(logger,"El Nodo conectado contiene mismos datos que Nodo ya conectado, por lo tanto n no puede realiarse la nueva conexión");
    return -1;

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
	log_info(logger,"Agregué: ip=%s, puerto=%d, nombre=%s, cantidad bloques=%d, sock=%d\n",nodo->ip_nodo,nodo->puerto_nodo ,nodo->nombre_nodo, nodo->cant_max_bloques, nodo->sockn);


	FILE* bin;

	char* ruta_bin = "/home/utnso/metadata/nodos.bin";

	if ((bin = fopen(ruta_bin, "w+b")) == NULL) {
					log_error(logger, "Error al al crear archivo nodos.bin");
					abort();
			}else{
				log_info(logger, "Archivo nodos.bin creado correctamente");
			}

    int cantLibre = 0;
	persistirBloquesNodo(nodo);
	cantLibre = obtenerCantidadBloquesLibresNodo(nodo);

	t_tamanioNodo *tNodo = malloc(sizeof(t_tamanioNodo));

	tNodo->nombreNodo = nodo->nombre_nodo;
	tNodo->tamanioTotal = nodo->cant_max_bloques;
	tNodo->tamanioLibre = cantLibre;

	list_add(listaTamanioNodos, tNodo);

	tamanioTotalNodos = tamanioTotalNodos + nodo->cant_max_bloques; //TODO Hay que restar desconexiones
	fprintf(bin,"TAMANIO=%i\n",tamanioTotalNodos);
	tamanioLibreActual = tamanioLibreActual + cantLibre;
	fprintf(bin,"LIBRE=%i\n",tamanioLibreActual);

	int j;

	t_tamanioNodo *nodoEvaluado = malloc(sizeof(t_tamanioNodo));
	t_nodoInterno *nodoActivoAEvaluar = malloc(sizeof(t_nodoInterno));

	fprintf(bin,"NODOS=");
	for (j = 0; j < list_size(listaNodosActivos); ++j) {

		nodoActivoAEvaluar=list_get(listaNodosActivos,j);
		fprintf(bin,"%s,",nodoActivoAEvaluar->nombre_nodo);

	}//TODO ahora devuelve valor numerico

	fprintf(bin,"\n");

	for (j = 0; j < list_size(listaNodosActivos); ++j) {

	nodoActivoAEvaluar=list_get(listaNodosActivos,j);

	fprintf(bin,"%sTotal=%i\n",nodoActivoAEvaluar->nombre_nodo,nodoActivoAEvaluar->cant_max_bloques);

	nodoEvaluado = nodoBuscadoEnListaDeTamanios(nodoActivoAEvaluar->nombre_nodo);

	fprintf(bin,"%sLibre=%i\n",nodoActivoAEvaluar->nombre_nodo,nodoEvaluado->tamanioLibre);

	}
	fclose(bin);

    persistirBloquesNodo(nodo);

    return 0;


}

t_tamanioNodo* nodoBuscadoEnListaDeTamanios(char* nombre) {
	bool cumpleConNombre(t_tamanioNodo* nodo) {
			return (string_equals_ignore_case(nodo->nombreNodo,nombre));
		}
	t_tamanioNodo* nodo = list_find(listaTamanioNodos, (void*) cumpleConNombre);

	return nodo;
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

	char *code;
	size_t n = 0;
	int c;

	code = malloc(1000);
	while ((c = fgetc(bitmap)) != EOF)
		{
			code[n++] = (char) c;
		}

	code[n] = '\0';
	int posicion=0;
	int cantidadBloquesLibres=0;
	while(code[posicion]!='\0'){
	if(code[posicion]=='0'){
		cantidadBloquesLibres++;
	}
	posicion++;
	}

return cantidadBloquesLibres;

}

char* concat(char* s1, char* s2)
	{
	    char *result = malloc(strlen(s1)+strlen(s2)+1);//+1 for the null-terminator
	    //in real code you would check for errors in malloc here
	    strcpy(result, s1);
	    strcat(result, s2);
	    return result;
	}
short existsDirectorySystem(char *fname)
{
  int fd=open(fname, O_RDONLY);
  if (fd<0)         /* error */
    return (errno==ENOENT)?-1:-2;
  /* Si no hemos salido ya, cerramos */
  close(fd);
  return 0;
}
void persistirBloquesNodo(t_nodoInterno *nodoApersistir) {

	char* ruta_bitmapArchivo;
	char* extArch;
	char* rutaAux = malloc(sizeof(1));
	char* ruta = malloc (sizeof(1));


	extArch = ".dat";
	ruta = "/home/utnso/metadata/bitmaps/";

 	rutaAux = concat(nodoApersistir->nombre_nodo,extArch);
 	ruta_bitmapArchivo = concat(ruta,rutaAux);

	FILE* bitmap;

	if ((bitmap = fopen(ruta_bitmapArchivo, "rb")) == NULL){
		log_info(logger, "No existe el archivo bitmap, se crea \n");
		if ((bitmap = fopen(ruta_bitmapArchivo, "w+b")) == NULL) {
							log_error(logger, "Error al al crear archivo bitmap\n");
							abort();
					}else{
						log_info(logger, "Archivo bitmap creado correctamente");
						 int i;

						  for(i=0; i < nodoApersistir->cant_max_bloques; i++ ){

							  fprintf(bitmap,"0");

						  }
					}
	}else
	{
		log_info(logger, "Existe el archivo bitmap, se levanta.");
	}

	fclose(bitmap);
}


t_nodoInterno* nodoBuscadoEnListaNodosActivos(char* ip_nodo, uint16_t puerto_nodo, char* nombre) {
	bool cumpleConDatos(t_nodoInterno* nodo) {
			return (nodo->puerto_nodo == puerto_nodo) || string_equals_ignore_case(nodo->ip_nodo,ip_nodo) || string_equals_ignore_case(nodo->nombre_nodo,nombre);
		}
	t_nodoInterno* nodo = list_find(listaNodosActivos, (void*) cumpleConDatos);
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

uint8_t cpfrom(char* RutaDelArchivo,char* dirDestinofs){

	printf("Copiar un archivo local al yamafs\n");

		t_file *archi = malloc(sizeof(t_file));

		FILE* bin;

		struct stat stat_file;
		stat(RutaDelArchivo, &stat_file);

		if ((bin = fopen(RutaDelArchivo, "rt")) == NULL) {
			perror("Error");
				log_error(logger, "Error al abrir el archivo '%s' %i\n", RutaDelArchivo,strlen(RutaDelArchivo));
				abort();
		}

		tamanio_espacio_datos = tamanioArchivo(bin);
		log_info(logger,"El tamanio del archivo es %d MB", tamanio_espacio_datos/TAMANIO_BLOQUE);

		cantidad_bloques = calcularCantidadBloques();

		log_info(logger,"La cantidad de bloques es %i por copia", cantidad_bloques);


		cantidad_bloques_con_copia = cantidad_bloques *2;

		if(cantidad_bloques_con_copia > tamanioLibreNodos){

		uint8_t finalizoDivision = dividirElArchivoEnBloques(bin, archi,RutaDelArchivo);

			if (finalizoDivision != 0) {
				borrarBloquesDePartes(archi->bloques);
				//freeArchivo(archi);
			}
			return finalizoDivision;

			}else{
				log_error(logger, "No alcanza el espacio disponible para el archivo a copiar");
				return -1;
			}
		fclose(bin);
}

void freeArchivo(t_file* archivo){
	free(archivo->nombre);
	free(archivo);
}

void borrarBloquesDePartes(t_list* listaBloquesDelArchivo) {
	uint32_t bloquesAborrar = list_size(listaBloquesDelArchivo);
	uint32_t i;
	for (i = 0; i < bloquesAborrar; i++) {
		t_nodo_bloque* bloqueAborrar = list_get(listaBloquesDelArchivo, i);
		uint16_t nro_bloque = bloqueAborrar->nro_bloque_nodo;
		t_nodoInterno* nodo = nodoBuscadoEnListaNodosActivos(bloqueAborrar->ip, bloqueAborrar->puerto,bloqueAborrar->id_nodo);
		bitarray_clean_bit(nodo->bloques, nro_bloque);
		free(bloqueAborrar);
	}

}

uint8_t dividirElArchivoEnBloques(FILE* archivo, t_file* arch, char* ruta) {

	char* bloque = malloc((TAMANIO_BLOQUE + 1) * sizeof(char));
	uint32_t posHastaEspacio;
	uint32_t posActual = 0;
	uint16_t nro_bloque_archi=0;
	uint8_t abortar = 1;
	int copias = 2;

	t_list* partes= list_create();
	t_parte* parte;

	uint8_t seguirDividiendo = 0;
	char* nomArchi = arch->nombre;
	arch->bloques = list_create();

	fread((void*) bloque, sizeof(char), TAMANIO_BLOQUE, archivo);

	while (!feof(archivo)) {

		posHastaEspacio = posHastaElEspacio((char*) bloque);
		fseek(archivo, posActual, SEEK_SET);
		memset((char*) bloque, '\0', TAMANIO_BLOQUE);
		fread((void*) bloque, sizeof(char), posHastaEspacio, archivo);


		t_list* nodos_ordenados = list_create();
		nodos_ordenados = obtenerNodosMasLibres();
		parte = malloc(sizeof(t_parte));
		parte->lista_nodos_parte= list_create();

		uint8_t n = copias;
		while(n && list_size(nodos_ordenados)>0){
			t_nodoInterno* nodo_libre= list_get(nodos_ordenados,0);
			t_nodo_bloque* bloque_nodo =bloquesDeNodoLibre(nodo_libre,nro_bloque_archi,nomArchi);
			uint8_t result =seteoDeBloque(nodo_libre,bloque_nodo->nro_bloque_nodo,(char*) bloque);

			if(result==0){
				parte->nro_parte_archi = nro_bloque_archi;
				list_add(parte->lista_nodos_parte,nodo_libre);
				list_add(arch->bloques,bloque_nodo);
				n--;
				list_remove(nodos_ordenados,0);
			}

			if(result==2){
				list_remove(nodos_ordenados,0);
					free(bloque);
					return abortar;
			}
			if(result==1){
				free(bloque);
				return abortar;
			}
		}

		if(n!= 0){
			free(bloque);
			return abortar;
		}else list_add(partes,parte);

		//inicializacionParaProximoBloque
		posActual += posHastaEspacio;
		nro_bloque_archi++;
		memset((char*) bloque, '\0', TAMANIO_BLOQUE);
		fread((void*) bloque, sizeof(char), TAMANIO_BLOQUE + 1, archivo);

	}

	t_list* nodos_ordenados = list_create();
	nodos_ordenados = obtenerNodosMasLibres();

	parte = malloc(sizeof(t_parte));
	parte->lista_nodos_parte= list_create();
	uint8_t n= copias;

	while(n && list_size(nodos_ordenados)>0){
		t_nodoInterno* nodo_libre= list_get(nodos_ordenados,0);
		t_nodo_bloque* bloque_nodo =bloquesDeNodoLibre(nodo_libre,nro_bloque_archi,nomArchi);
		uint8_t result =seteoDeBloque(nodo_libre,bloque_nodo->nro_bloque_nodo,(char*) bloque);

		if(result==0){

			parte->nro_parte_archi = nro_bloque_archi;
			list_add(parte->lista_nodos_parte,nodo_libre);
			list_add(arch->bloques,bloque_nodo);
			n--;
			list_remove(nodos_ordenados,0);
		}

		if(result==2){
			list_remove(nodos_ordenados,0);
			if(copias==1){
				free(bloque);
				return abortar;
			}
		}

		if(result==1){
			free(bloque);
			return abortar;
		}

	}

	if(n!= 0){
		free(bloque);
		return abortar;
	}else list_add(partes,parte);

	arch->directorio = ruta;
	arch->nodosDePartes = partes;
	arch->disponible = 0;
	list_add(listas_bloques_de_archivos, arch);

	free(bloque);
	return seguirDividiendo;
}

uint32_t posHastaElEspacio(char* bloque) {
	uint32_t j;
	uint32_t posHastaEspacio;
	char comparable;
	for (j = 0; j < TAMANIO_BLOQUE; j++) {
		comparable = bloque[j];
		if (comparable == '\n') {
			posHastaEspacio = j + 1;
		}
	}
	return posHastaEspacio;
}

t_nodo_bloque* bloquesDeNodoLibre(t_nodoInterno  *nodo,uint16_t nro_bloque_archi, char* nom_archi) {

	t_nodo_bloque *nodo_bloque = malloc(sizeof(t_nodo_bloque));
	uint16_t result_bloque_libre = obtenerElPrimerBloqueLibre(nodo);

	nodo_bloque->nro_bloque_nodo = result_bloque_libre;
	nodo_bloque->nro_bloque_nodo_long = sizeof(uint16_t);
	nodo_bloque->nombre_archivo = nom_archi;
	nodo_bloque->nombre_archivo_long = string_length(nom_archi);
	nodo_bloque->ip = nodo->ip_nodo;
	nodo_bloque->ip_long = string_length(nodo->ip_nodo);
	nodo_bloque->id_nodo = nodo->nombre_nodo;
	nodo_bloque->id_nodo_long = string_length(nodo->nombre_nodo);
	nodo_bloque->puerto = nodo->puerto_nodo;
	nodo_bloque->puerto_long = sizeof(uint16_t);
	nodo_bloque->nro_bloque_archi = nro_bloque_archi;
	nodo_bloque->nro_bloque_archi_long = sizeof(uint16_t);



return nodo_bloque;

}

uint16_t obtenerElPrimerBloqueLibre(t_nodoInterno *nodo) {
	uint32_t cantidadBloquesNodo = (nodo->cant_max_bloques);
	uint32_t i = 0;
	int32_t encontroBloqueVacio = -1;
	int32_t numeroBloque = 0;
	while ((i < cantidadBloquesNodo) && (encontroBloqueVacio == -1)) {
		if (bitarray_test_bit(nodo->bloques, i) == 0) {
			encontroBloqueVacio = 1;
			numeroBloque = i;
			i = cantidadBloquesNodo;
		}
		i++;
	}
	if (encontroBloqueVacio == -1) {
		return encontroBloqueVacio;
	}
	return numeroBloque;
}

int verificarEspacioBloques(uint16_t cantidadBloques){

	if(tamanioTotalNodos >= cantidadBloques){
		log_info(logger,"Hay espacio disponible para realizar las copias\n");
	}else{
		log_error(logger, "No alcanza el espacio disponible para el archivo a copiar\n");
		return -1;
	}

	return 0;
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

void cpto(){
	printf("mcopiar arch local al yamafs\n");
}

void reconocer_comando(char * linea){ //reconoce comando

	int flag_comandoOK =0;
	char dirarch1[250];
	char dirDestinofs[250];
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

	    	printf("\nIngrese el nombre del nuevo directorio:\n\n>");
            nombDirectorio = malloc(255);

	        fgets(nombDirectorio, 255, stdin);
	        strtok(nombDirectorio,"\n");
	        printf("\nIngrese el nombre del nuevo directorio:\n\n>");
	    	crear_directorio(nombDirectorio);

	    }else if(flag_comandoOK==0 && strcmp(linea, "cpfrom")==0){

	    	printf("\nIngrese la dirección del archivo:\n\n>");
	    	scanf("%s",dirarch1);
	    	//fgets(dirarch1, sizeof(dirarch1), stdin);
	    	printf("\nIngrese el directorio del FS:\n\n>");
	    	scanf("%s",dirDestinofs);
	    	//fgets(dirDestinofs, sizeof(dirDestinofs), stdin);
	       //limpiar(dirarch1);

	        cpfrom(dirarch1,dirDestinofs);
	    	//devolverRedundanciaArchivo(redundanciaArchivo);


	    }else if(flag_comandoOK==0 && strcmp(linea, "cpto")==0){
	    	cpto();
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
	sem_init(&mutex_logger, 0, 1);

	listaNodosActivos=list_create();
	listas_bloques_de_archivos=list_create();

	tablaDeArchivos=list_create();
	punteroNodoAllenar=0;
	nodoParaYama=list_create();
	listaTamanioNodos=list_create();
	lista_directorios=list_create();

	tamanioLibreNodos = 0;
	tamanioTotalNodos = 0;
	tamanioLibreActual=0;
	espacioDisponible=0;


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

void persistirDirectorios() {
	char* rutaMetadata = "/home/utnso/metadata";
		char* rutaBitmap = "/home/utnso/metadata/bitmaps";

		int abre=existsDirectorySystem(rutaMetadata);
		  switch (abre)
		    {
		    case 0: log_info (logger,"Ya existe la carpeta: %s",rutaMetadata);
		    break;
		    case -1: {mkdir(rutaMetadata, 0777);
		    		mkdir(rutaBitmap,0777);
		    		log_info(logger, "Se creó la carpeta: %s %s",rutaMetadata,"y dentro de ella la carpeta bitmaps");
		    break;}
		    case -2: log_info (logger,"Ocurrió un error al abrir. %d (%s)\n", errno, strerror(errno));
		    break;

		    default: printf ("Nunca veremos este mensaje");
		    }

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



void levantarDirectorios() {

	FILE* archi_directorios = fopen("/home/utnso/metadata/directorios.dat", "rt");

	t_directorio dire;


	while(1){

				if(feof(archi_directorios)!=0)
					break;

				fscanf(archi_directorios,"%i %s %i",&dire.index,dire.nombre,&dire.padre);

			}

   fclose(archi_directorios);

}

uint8_t seteoDeBloque(t_nodoInterno* nodo, uint16_t numero_bloque, char* datos) {

	int8_t status;
	int resultado;
	int sockn;

	uint8_t seguir_dividiendo = 0;
	uint8_t abortar = 1;
	uint8_t enviar_otro_nodo = 2;

	sockn = nodo->sockn;

	codigo_operacion = SET_BLOQUE;

	enviarDatos_DN(sockn, datos,numero_bloque,codigo_operacion);

	sem_wait(&mutex_logger);
	log_info(logger, "Envie para setear el bloque %d del nodo %s",numero_bloque, nodo->nombre_nodo);
	sem_post(&mutex_logger);

	status= recv(sockn, &resultado, sizeof(int8_t), 0);
	if (status == -1 || status==0) {
			status= nodoDesconectadoEnviarAOtroNodoOAbortar(nodo,sockn);
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
		   	} //TODO seguir. Se necesita setear el archivo bitmap

		} else {
			sem_wait(&mutex_logger);
			log_error(logger, "El seteado no pudo realizarse");
			sem_post(&mutex_logger);
			bitarray_clean_bit(nodo->bloques, numero_bloque);
			return enviar_otro_nodo;
		}

	return seguir_dividiendo;
}

uint8_t nodoDesconectadoEnviarAOtroNodoOAbortar(t_nodoInterno* nodo, uint8_t socket_nodo){
	uint8_t enviar_otro_nodo = 2;
	uint8_t abortar = 1;

	sem_wait(&mutex_logger);
	log_error(logger,"Nodo %d desconectado y eliminado de la lista de consola",nodo->nombre_nodo);
	sem_post(&mutex_logger);
	uint8_t codop = NODO_CAIDO;

		sem_wait(&mutex_dataNode);
		send(socketDN, &codop, sizeof(cod_operacion), 0);
		send(socketDN, &nodo->nombre_nodo, sizeof(cod_operacion), 0);
		sem_post(&mutex_dataNode);

	quitarNodoDeLista(socket_nodo);

	if (list_size(listaNodosActivos) < 2) {
		sem_wait(&mutex_logger);
		log_error(logger,"No puede seguir dividiendo el archivo sin poder realizar 2 copias");
		sem_post(&mutex_logger);
		return abortar;
	}
return enviar_otro_nodo;
}

int enviarDatos_DN (int sockn,char* datos, uint16_t numero_bloque,uint8_t codigo_operacion){

	int resultado;

	t_datos datosAenviar;

	datosAenviar.codigo_operacion = codigo_operacion;
	datosAenviar.codigo_operacion_long = sizeof(codigo_operacion);
	datosAenviar.numero_bloque = numero_bloque;
	datosAenviar.numero_bloque_long = sizeof(numero_bloque);
	datosAenviar.datos = malloc(sizeof(datos));
	datosAenviar.datos= datos;
	datosAenviar.tam_datos = string_length(datos);

		char* paqueteSerializado;

		datosAenviar.tamanio_datos = sizeof(datosAenviar.codigo_operacion_long) +
								datosAenviar.codigo_operacion_long +
								sizeof(datosAenviar.numero_bloque_long) +
								datosAenviar.numero_bloque_long +
								sizeof(datosAenviar.tam_datos) +
								datosAenviar.tam_datos;

		paqueteSerializado = serializarEstructura(&datosAenviar);
		resultado = send(sockn, paqueteSerializado, datosAenviar.tamanio_datos,0);

		sem_wait(&mutex_logger);
		log_info(logger, "Envie a DATANODE los datos para setear" );
		sem_post(&mutex_logger);
		dispose_package(&paqueteSerializado);

		return resultado;
}

char* serializarEstructura(t_datos* datosAenviar){

	char *paqueteSerializado = malloc(datosAenviar->tamanio_datos);
	int offset = 0;
	int size;

	size =  sizeof(datosAenviar->codigo_operacion_long);
	memcpy(paqueteSerializado + offset, &(datosAenviar->codigo_operacion_long), size);
	offset += size;

	size =  datosAenviar->codigo_operacion_long;
	memcpy(paqueteSerializado + offset, &(datosAenviar->codigo_operacion), size);
	offset += size;

	size =  sizeof(datosAenviar->numero_bloque_long);
	memcpy(paqueteSerializado + offset, &(datosAenviar->numero_bloque_long), size);
	offset += size;

	size =  datosAenviar->numero_bloque_long;
	memcpy(paqueteSerializado + offset, &(datosAenviar->numero_bloque), size);
	offset += size;


	size =  sizeof(datosAenviar->tam_datos);
	memcpy(paqueteSerializado + offset, &(datosAenviar->tam_datos), size);
	offset += size;

	size =  datosAenviar->tam_datos;
	memcpy(paqueteSerializado + offset,datosAenviar->datos , size);
	offset += size;

	offset += size;

	return paqueteSerializado;
}

void dispose_package(char **package){
	free(*package);
}

t_list* obtenerNodosMasLibres() {
	t_list* lista_nodos_libres = list_create();
	list_add_all(lista_nodos_libres, listaNodosActivos );
	list_sort(lista_nodos_libres, (void*) comparadorDeBloques);
	return lista_nodos_libres;
}

void quitarNodoDeLista(uint8_t nodo_socket) {

	bool busquedaDeUnNodo(t_nodoInterno* un_nodo) {
		return (un_nodo->sockn == nodo_socket);
	}
	list_remove_by_condition(listaNodosActivos, (void*) busquedaDeUnNodo);

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

void mostrarEspacioDispo(){
	uint8_t i;
	espacioDisponible=0;

	for(i=0;i<list_size(listaNodosActivos); i++){
		t_nodoInterno* nodo =list_get(listaNodosActivos,i);
		espacioDisponible += (bloquesLibresDeUnNodo(nodo)) * TAMANIO_BLOQUE;
	}

	sem_wait(&mutex_logger);
	log_info(logger, "Espacio disponible: %ld MB", espacioDisponible);
	sem_post(&mutex_logger);
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

int calcularCantidadBloques() {
	return ((tamanio_espacio_datos + TAMANIO_BLOQUE + 1 )/TAMANIO_BLOQUE); //Un bloque = 1 MB
}

int tamanioArchivo(FILE* bin) {
	struct stat st;

	int descriptor_archivo = fileno(bin);
	fstat(descriptor_archivo, &st);

	return st.st_size;
}

char* mapearAMemoria(char* RutaDelArchivo){
		int mapper;
		char* mapeo2;
		FILE* bin;
		uint64_t tamanio;

		if ((bin = fopen(RutaDelArchivo, "r+t")) == NULL) {
			//Si no se pudo abrir, imprimir el error
			log_error(logger, "Error al abrir el archivo: %s\n", RutaDelArchivo);
		}

		mapper = fileno(bin);
		tamanio = tamanioArchivo(bin);

		if ((mapeo2 = mmap( NULL, tamanio, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_NORESERVE, mapper, 0)) == MAP_FAILED) {
			//Si no se pudo ejecutar el MMAP, imprimir el error
			log_error(logger,
					"Error al ejecutar MMAP del archivo %s de tamaño %d\n",
					RutaDelArchivo, tamanio);
		}

		close(mapper);
		fclose(bin);
		return mapeo2;
	}


char* getBloque(uint16_t numerodebloque) {
	char* contenido_bloque;

	contenido_bloque = string_substring(mapeo,numerodebloque*TAMANIO_BLOQUE,TAMANIO_BLOQUE);

	return contenido_bloque;
}

int informacionDeArchivoParaYama(int8_t* status){
	uint8_t long_path_archivo_original;
	uint16_t i;

	//RECIBO LARGO DEL PATH
		*status = recv(socket_yama, &long_path_archivo_original, sizeof(uint8_t), 0);
		sem_wait(&mutex_logger);
		log_debug(logger, "Recibido long %i", long_path_archivo_original);
		sem_post(&mutex_logger);

	//RECIBO EL PATH
		char* path_archivo_original=malloc(long_path_archivo_original);
		memset(path_archivo_original, '\0', long_path_archivo_original + 1);
		*status = recv(socket_yama, path_archivo_original, long_path_archivo_original, 0);
		sem_wait(&mutex_logger);
		log_debug(logger, "Recibido path %s", path_archivo_original);
		sem_post(&mutex_logger);

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
			//enviarUnBloque(nodo_bloque);
			}



		if (*status == 0 || *status == -1) {
				sem_wait(&mutex_logger);
				log_error(logger, "YAMA se ha desconectado.");
				sem_wait(&mutex_logger);
			}
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
		sem_wait(&mutex_logger);
		log_error(logger, "YAMA se ha desconectado.");
		sem_post(&mutex_logger);
		status=0;
	}else{
		switch (cod_op) {
			case SOLICITO_INFORMACION_BLOQUES_ARCHIVO:
				sem_wait(&mutex_logger);
				log_debug(logger, "YAMA SOLICITO_INFORMACION_BLOQUES_ARCHIVO.");
				sem_post(&mutex_logger);
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
void* clienteSocketThread(void* socket) {
		uint8_t cliente_socket;
		cliente_socket = *((int*) socket);
		uint8_t identificador;
		sem_wait(&mutex_logger);
		log_debug(logger, "Cliente conectado %i. Aguardamos identificacion", cliente_socket);
		sem_post(&mutex_logger);
		//handshake
		int8_t status = recv(cliente_socket, &identificador, sizeof(uint8_t), 0);  //DATANODE=30 YAMA=31
		if (status != -1) {
			if (identificador == DATANODE) {
				sem_wait(&mutex_logger);
				log_debug(logger, "Conexion por socket con DataNode");
				sem_post(&mutex_logger);
				sem_wait(&mutex_logger);
				log_info(logger, "Conectado a DATANODE");
				sem_post(&mutex_logger);
				conexion_datanode(cliente_socket);
			} else if (identificador == YAMA) {
				sem_wait(&mutex_logger);
				log_debug(logger, "Conexion por socket con YAMA");
				sem_post(&mutex_logger);
				sem_wait(&mutex_logger);
				log_info(logger, "Conectado a YAMA");
				sem_post(&mutex_logger);
				printf("Acitvos al momento: %i",list_size(listaNodosActivos));

				conexion_YAMA(cliente_socket);
			}else{
				sem_wait(&mutex_logger);
				log_error(logger, "He recibo una conexión de un cliente desconocido y no se como tratarlo. (¡Verifique código en FS!)");
				sem_post(&mutex_logger);
			}
		}

		return socket; //retorno solo para evitar el warning
}
void* socketListener(void* param)
{
	    sem_wait(&mutex_logger);
		log_debug(logger, "Iniciando Listener para escuchar clientes YAMA/DATANODES");
		sem_post(&mutex_logger);
		struct addrinfo hints;
		struct addrinfo *serverInfo;

		memset(&hints, 0, sizeof(hints));
		hints.ai_family = AF_UNSPEC; // No importa si uso IPv4 o IPv6
		hints.ai_flags = AI_PASSIVE; // Asigna el address del localhost: 127.0.0.1 //TODO VERIFICAR SI ES NECESARIO IP
		hints.ai_socktype = SOCK_STREAM; // Indica que usaremos el protocolo TCP

		sem_wait(&mutex_logger);
		log_debug(logger, "Hints completo");
		sem_post(&mutex_logger);

		// Le pasamos NULL como IP, ya que le indicamos que use localhost en AI_PASSIVE
		// Carga en serverInfo los datos de la conexion
		getaddrinfo(NULL, string_itoa(puertoFS), &hints, &serverInfo);

		//Creo socket que escuche las conexiones entrantes
		int listeningSocket;
		listeningSocket = socket(serverInfo->ai_family, serverInfo->ai_socktype,
				serverInfo->ai_protocol);
		sem_wait(&mutex_logger);
		log_debug(logger, "Socket para escuchar generado: %d", listeningSocket);
		sem_post(&mutex_logger);
		//Me pongo a escuchar conexiones entrantes
		if(bind(listeningSocket, serverInfo->ai_addr, serverInfo->ai_addrlen)==-1){
			log_error(logger,"El puerto de listening esta ocupado.");
			log_error(logger,"Interrumpiendo ejecución...");
			abort();
		}
		freeaddrinfo(serverInfo);
		sem_wait(&mutex_logger);
		log_debug(logger, "Escuchando por el puerto");
		sem_post(&mutex_logger);

		//El sistema esperara hasta que reciba una conexion entrante
		struct sockaddr_in addr; // Esta estructura contendra los datos de la conexion del cliente. IP, puerto, etc.
		socklen_t addrlen = sizeof(addr);

		while (true) {
			sem_wait(&mutex_logger);
			log_debug(logger, "Estoy escuchando");
			sem_post(&mutex_logger);
			listen(listeningSocket, BACKLOG); // IMPORTANTE: listen() es una syscall BLOQUEANTE.

			// Aceptamos la conexion entrante y creamos un nuevo socket para comunicarnos (el actual se queda escuchando más conexiones)
			int clienteSocket = accept(listeningSocket, (struct sockaddr *) &addr,
					&addrlen);
			pthread_t thr_cliente;
			sem_wait(&mutex_logger);
			log_debug(logger, "Llego alguien");
			sem_post(&mutex_logger);
			int cliente = pthread_create(&thr_cliente, NULL, clienteSocketThread,(void*) &clienteSocket);
			if(cliente){
						sem_wait(&mutex_logger);
						log_error(logger,"No se pudo crear el hilo Cliente");
						sem_post(&mutex_logger);
						abort();
				}
		}
		sem_wait(&mutex_logger);
		log_debug(logger, "Termino el hilo listener");
		sem_post(&mutex_logger);
		close(listeningSocket);
		return param;
}


int main(int argc, char *argv[]) {

	sem_init(&mutex_logger,0,1);
	pthread_t th_consola;
	pthread_t th_listener;

	cargarConfiguraciones();
	int listener= pthread_create(&th_listener, NULL, socketListener, NULL);

	if(listener){
		sem_wait(&mutex_logger);
		log_error(logger,"No se pudo crear el hilo Listener");
	    sem_post(&mutex_logger);
		abort();
		}

	int hiloConsola =pthread_create(&th_consola, NULL, (void *) consola_imprimir_encabezado, NULL);
		if(hiloConsola){
						sem_wait(&mutex_logger);
						log_error(logger,"No se pudo crear el hilo Consola\n");
						sem_post(&mutex_logger);
						abort();
				}

	crearEstructuraFS();


	//No quiero que termine hasta que se cierre la consola
	pthread_join(th_consola, NULL);
	pthread_join(th_listener, NULL);



    log_info(logger, "<<Proceso FileSystem Finalizado>>");
    log_destroy(logger);
    return 1;

}
