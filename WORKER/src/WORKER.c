/*
 ============================================================================
 Name        : WORKER.c
 Author      : yo
 Version     :
 Copyright   : Grupo YAMAYA
 Description : Hello World in C, Ansi-style
 ============================================================================
 */

#include "WORKER.h"


t_log* logger;
t_config* configuration;
char* nombreNodo;
uint16_t puerto_worker;
char* rutaDataBin;
int configOk=1;
char* ip_worker;
t_config* configuration;
t_log* logger;
char* dir_temporal;
uint16_t cantidad_bloques;
uint32_t tamanio_espacio_datos;
pthread_t thr_listener;
sem_t mutex_log;
char* mapeo;

#define TAMANIO_BLOQUE 20971520 //20 MB

#define ETAPA_TRANSFORMACION 4
#define ETAPA_REDUCCION_LOCAL 5
#define ETAPA_REDUCCION_GLOBAL 6

#define ID_PROC_MASTER 2
#define ID_PROC_WORKER 3

/** Puerto  */
// #define PORT       7000 Para que está?

/** Longitud del buffer  */
#define BUFFERSIZE 512

/** Número máximo de hijos */
#define MAX_CHILDS 3

void cargarConfiguraciones() {
	logger = log_create("logWorker", "WORKER LOG", true, LOG_LEVEL_DEBUG);
	log_info(logger, "<<Proceso Worker inició>>");
	configuration = config_create("/home/utnso/workspace/tp-2017-2c-Yamaya/CONFIG_NODO");
	log_info(logger, "Intentando levantar el archivo de configuraciones.");
	if(configuration==NULL){
		log_error(logger, "Error el archivo de configuraciones no existe. (Revisa el Path, recuerda que lo comparte con el NODO)");
		exit(-1);
	}

	if (config_has_property(configuration, "NOMBRE_NODO")) {

		nombreNodo = config_get_string_value(configuration, "NOMBRE_NODO");

		log_info(logger, "El nombre del Nodo es: %s", nombreNodo);

	} else {

		log_error(logger, "Error al obtener el Nombre del Nodo");

		configOk = 0;
	}

	if (config_has_property(configuration, "PUERTO_WORKER")) {

		puerto_worker = config_get_int_value(configuration, "PUERTO_WORKER");

		log_info(logger, "El puerto del Worker es: %i", puerto_worker);

	} else {

		log_error(logger, "Error al obtener el puerto del Worker.");

		configOk = 0;
	}
	if (config_has_property(configuration, "RUTA_DATABIN")) {

		rutaDataBin = config_get_string_value(configuration, "RUTA_DATABIN");

		log_info(logger, "La ruta del Data.Bin es: %s", rutaDataBin);

	} else {

		log_error(logger, "Error al obtener la ruta del Data.Bin");

		configOk = 0;
		}
	if (!configOk) {
		log_error(logger, "Debido a errores en las configuraciones, se aborta la ejecución... (REVISE ARCH. CONFIGURACIONES)");
		exit(-1);
	}


}

void* crearListenerSocket(void* param){


 	log_info(logger, "Creacion listener Master/Worker \n");

 	struct addrinfo hints;
	struct addrinfo *serverInfo;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_UNSPEC;		// Permite que la maquina se encargue de verificar si usamos IPv4 o IPv6
	hints.ai_socktype = SOCK_STREAM;	// Indica que usaremos el protocolo TCP

	log_debug(logger, "Hints completo\n");


	getaddrinfo(ip_worker, string_itoa(puerto_worker), &hints, &serverInfo);	 // Le pasamos NULL como IP, ya que le indicamos que use localhost en AI_PASSIVE

	//Creo un socket que escuche las conexiones entrantes
	int listening_socket;
	listening_socket = socket(serverInfo->ai_family, serverInfo->ai_socktype,serverInfo->ai_protocol);

	log_debug(logger, "Socket para escuchar generado: %d\n", listening_socket);


	//Me pongo a escuchar las conexiones entrantes
	if(bind(listening_socket, serverInfo->ai_addr, serverInfo->ai_addrlen)==-1){
		log_error(logger,"El puerto listening esta ocupado");
		log_error(logger,"Abortando...");
		abort();
	}
	freeaddrinfo(serverInfo); // Ya no lo vamos a necesitar

	log_debug(logger, "Escuchando el puerto\n");


	struct sockaddr_in addr; // Esta estructura contendra los datos de la conexion del cliente. IP, puerto, etc.
	socklen_t addrlen = sizeof(addr);

	while(true){	//Este hilo quedará ejecutando hasta que se mate el proceso nodo


		log_debug(logger, "Estoy escuchando\n");

		int8_t* parametro = malloc(sizeof(int8_t));

		// Aceptamos la conexión entrante y creamos un nuevo socket para comunicarnos (el actual se queda escuchando más conexiones)
		*parametro = accept(listening_socket, (struct sockaddr *) &addr, &addrlen);

		log_debug(logger, "Llego alguien\n");


		pthread_t* thr_cliente = malloc(sizeof(pthread_t));
		int8_t cliente = pthread_create(thr_cliente, NULL, hiloClienteSocket, (void*) parametro);
		if(cliente){

			log_error(logger,"No se pudo crear el hilo cliente\n");

			}
	}

	log_debug(logger, "Termino el hilo listener\n");
	close(listening_socket);
}


void* hiloClienteSocket(void* socket){
	int8_t cliente_socket;
	cliente_socket = *((int8_t*)socket);


	uint8_t identificacion;


	log_info(logger, "Cliente conectado. Esperando identificacion\n");

	int8_t status = recv(cliente_socket, &identificacion, sizeof(uint8_t), 0); //handshake
	if(status != -1){
		if(identificacion == ID_PROC_MASTER){

			levantarEscuchasParaMaster();
			realizarOperacionMaster(cliente_socket);

		}else if(identificacion == ID_PROC_WORKER){
			char* dir_temp;
			log_info(logger, "Se ha conectado un cliente Worker\n");
			recv(cliente_socket,&dir_temp,sizeof(uint8_t),0);
													}
											}
	return socket; //retorno solo para evitar el warning
}


int levantarEscuchasParaMaster(){
	  int socket_WorkerHost;
	    struct sockaddr_in master_addr;
	    struct sockaddr_in mi_addr;
	    struct timeval tv;      //Para el timeout del accept
	    socklen_t size_addr = 0;
	    int socket_master;
	    fd_set rfds;       // Conjunto de descriptores a vigilar
	    int childcount=0;
	    int exitcode;

	    int childpid;
	    int pidstatus;

	    int activated=1;
	    int loop=0;
	    socket_WorkerHost = socket(AF_INET, SOCK_STREAM, 0);
	    if(socket_WorkerHost == -1){
	    	log_error(logger, "No puedo inicializar el socket");}

	    mi_addr.sin_family = AF_INET ;
	    mi_addr.sin_port = htons(puerto_worker);
	    mi_addr.sin_addr.s_addr = INADDR_ANY ;

	    int activado=1;
		setsockopt(socket_WorkerHost, SOL_SOCKET, SO_REUSEADDR, &activado, sizeof(activado)); //Para decir al sistema de reusar el puerto


	    if( bind( socket_WorkerHost, (struct sockaddr*)&mi_addr, sizeof(mi_addr)) == -1 ){
	      log_error(logger, "Falló el bind. (¿El puerto no está en uso o hay dos instancias de Worker corriendo paralelas?)"); // Error al hacer el bind()
	    exit (-1);}

	    if(listen( socket_WorkerHost, 10) == -1 ){
	      log_error(logger, "Falló el listen, no puedo escuchar en el puerto especificado.");
	    }
	    size_addr = sizeof(struct sockaddr_in);


	    while(activated)
	      {
	    reloj(loop);
	    // select() se carga el valor de rfds
	    FD_ZERO(&rfds);
	    FD_SET(socket_WorkerHost, &rfds);

	    // select() se carga el valor de tv
	    tv.tv_sec = 0;
	    tv.tv_usec = 500000;    // Tiempo de espera

	    if (select(socket_WorkerHost+1, &rfds, NULL, NULL, &tv))
	      {
	        if((socket_master = accept( socket_WorkerHost, (struct sockaddr*)&master_addr, &size_addr))!= -1)
	          {
	        loop=-1;        //Para reiniciar el mensaje de Esperando conexión...
	        printf("\nSe ha conectado %s por su puerto %d\n", inet_ntoa(master_addr.sin_addr), master_addr.sin_port);
	        switch ( childpid=fork() )
	          {
	          case -1:  // Error
	        	  log_error(logger, "No se puede crear el proceso hijo. (FORK Trouble)");
	            break;
	          case 0:   //Somos proceso hijo
	            if (childcount<MAX_CHILDS)
	              exitcode=AtiendeAMaster(socket_master, master_addr);
	            else
	              exitcode=DemasiadosClientes(socket_master, master_addr);

	            exit(exitcode); // Código de salida
	          default:  // Somos proceso padre
	            childcount++; // Acabamos de tener un hijo
	            close(socket_master); // Nuestro hijo se las apaña con el cliente que
	                        // entró, para nosotros ya no existe.
	            break;
	          }
	          }
	        else
	        	 log_error(logger, "Error al aceptar la conexión.\n");
	      }

	    // Miramos si se ha cerrado algún hijo últimamente
	    childpid=waitpid(0, &pidstatus, WNOHANG);
	    if (childpid>0)
	      {
	        childcount--;   // Se acaba de morir un hijo
	        //Muchas veces nos dará 0 si no se ha muerto ningún hijo, o -1 si no tenemos hijos
	         //con errno=10 (No child process). Así nos quitamos esos mensajes

	        if (WIFEXITED(pidstatus))
	          {

	       //Tal vez querremos mirar algo cuando se ha cerrado un hijo correctamente
	        if (WEXITSTATUS(pidstatus)==99)
	          {
	            printf("\nSe ha pedido el cierre del programa\n");
	            activated=0;
	          }
	          }
	      }
	    loop++;
	    }

	    close(socket_WorkerHost);

	    return 0;
}

int AtiendeAMaster(int socket, struct sockaddr_in addr)
{

    char buffer[BUFFERSIZE];
    char aux[BUFFERSIZE];
    int bytecount;
    int fin=0;
    int code=0;         // Código de salida por defecto


    while (!fin)
      {

    memset(buffer, 0, BUFFERSIZE);
    if((bytecount = recv(socket, buffer, BUFFERSIZE, 0))== -1)
    	log_error(logger,"No se puede recibir información. (RECV)");

    //Evaluamos los comandos
    // El sistema de gestión de comandos es muy rudimentario, pero nos vale
     // Comando TIME - Da la hora
    if (strncmp(buffer, "EXIT", 4)==0)
      {
        memset(buffer, 0, BUFFERSIZE);
        sprintf(buffer, "Hasta luego. Vuelve pronto %s\n", inet_ntoa(addr.sin_addr));
        fin=1;
      }
    // Comando CERRAR - Cierra el servidor
    else if (strncmp(buffer, "CERRAR", 6)==0)
      {
        memset(buffer, 0, BUFFERSIZE);
        sprintf(buffer, "Adiós. Cierro el servidor\n");
        fin=1;
        code=99;        // Salir del programa
      }
    else
      {
        sprintf(aux, "ECHO: %s\n", buffer);
        strcpy(buffer, aux);
      }

    if((bytecount = send(socket, buffer, strlen(buffer), 0))== -1)
      error(6, "No puedo enviar información");
      }

    printf("\nCerramos la conexion del master con puerto %i\n", addr.sin_port);

    close(socket);
    return code;
}

int DemasiadosClientes(int socket, struct sockaddr_in addr)
{
    char buffer[BUFFERSIZE];
    int bytecount;

    memset(buffer, 0, BUFFERSIZE);

    sprintf(buffer, "Demasiados clientes conectados. Por favor, espere unos minutos\n");

    if((bytecount = send(socket, buffer, strlen(buffer), 0))== -1)
      error(6, "No puedo enviar información");

    close(socket);

    return 0;
}


void reloj(int loop)
{
  if (loop==0)
    printf("\n[WORKER] Esperando conexiones  ");

 printf("\033[1D");        /* Introducimos código ANSI para retroceder 2 caracteres */
  switch (loop%4)
    {
    case 0: printf("|"); break;
    case 1: printf("/"); break;
    case 2: printf("-"); break;
    case 3: printf("\\"); break;
    default:            /* No debemos estar aquí */
      break;
    }

  fflush(stdout);       /* Actualizamos la pantalla */
}

void error(int code, char *err)
{
  char *msg=(char*)malloc(strlen(err)+14);
  sprintf(msg, "Error %d: %s\n", code, err);
  //fprintf(stderr, msg);
  exit(1);
}


void realizarOperacionMaster(int8_t socket_master){
	uint8_t codop;
	char* buffer;
	int buffer_size;
	uint32_t comparacion = 0;

	uint32_t tamanio_script;
	char* programa_ejecutable;
	uint16_t numero_bloque;
	uint8_t tamanio_nombre_archivo_temporal = 0;
	char* nombre_archivo_temporal;
	char* nombre_tmp;
	uint8_t resultado; //0 ok, 1 fallo

	uint8_t cant_elem;
	uint8_t i;
	t_list* lista_archivo_workers = list_create();
	t_list* lista_archivos = list_create();
	char* concat_archivos = string_new();

	recv(socket_master, &codop, sizeof(uint8_t),0);


	log_debug(logger, "Recibi el siguiente codigo de operacion: %d\n", codop);


	switch (codop) {
		case ETAPA_TRANSFORMACION:
			//Recibo tamanio y char del script

			buffer = malloc(buffer_size = sizeof(uint32_t));
			recv(socket_master, buffer, sizeof(uint32_t), 0);
			memcpy(&tamanio_script, buffer, buffer_size);
			free(buffer);

			programa_ejecutable = malloc(sizeof(char)*tamanio_script);

			while(comparacion != tamanio_script){
				comparacion += recv(socket_master,programa_ejecutable+comparacion,tamanio_script-comparacion, 0);
			}


			log_info(logger, "Recibi el script de transformacion a ejecutar \n");

			//Recibo nro de bloque
			recv(socket_master, &numero_bloque, sizeof(uint16_t), 0);


			log_info(logger, "Se realizo una solicitud de transformacion para el bloque %d\n", numero_bloque);


			//Recibo tamanio y nombre del archivo temporal a guardar
			buffer = malloc(buffer_size = sizeof(uint8_t));
			recv(socket_master, buffer, sizeof(uint8_t), 0);
			memcpy(&tamanio_nombre_archivo_temporal, buffer, buffer_size);
			free(buffer);

			//recv(socket_master, nombre_archivo_temporal,tamanio_nombre_archivo_temporal, 0);
			nombre_archivo_temporal = string_new();
			nombre_tmp = malloc(tamanio_nombre_archivo_temporal+1);
			memset(nombre_tmp,'\0',tamanio_nombre_archivo_temporal+1);

			comparacion = 0;
			while(comparacion != tamanio_nombre_archivo_temporal){
				comparacion += recv(socket_master,nombre_tmp,tamanio_nombre_archivo_temporal-comparacion,0);
				string_append(&nombre_archivo_temporal,nombre_tmp);
				memset(nombre_tmp,'\0',tamanio_nombre_archivo_temporal+1);
			}


			log_info(logger, "Recibi el nombre de archivo temporal\n");


			//ejecuto transformacion, me devuelve si salio bien o no

			log_info(logger, "Ejecutando transformacion solicitada\n");


		    srand(socket_master);
		    uint8_t* num_rand = malloc(sizeof(uint8_t));
		    *num_rand = rand() % 100;

		    char* nombre_script = string_new();
		    string_append(&nombre_script,string_itoa((int)time(NULL)));
		    string_append(&nombre_script,"-");
		    string_append(&nombre_script,string_itoa(socket_master));
		    string_append(&nombre_script,"-");
		    string_append(&nombre_script,string_itoa(*num_rand));
		    string_append(&nombre_script,"-");
		    string_append(&nombre_script,string_itoa(puerto_worker));

		    //Paso el script a un archivo y le otorgo permisos de ejecucion
		    char* rutaScript = string_new();
		    rutaScript = pasarAArchivoTemporal(programa_ejecutable,nombre_script,".transformador",tamanio_script);
		    permitirEjecucion(rutaScript);

		    free(programa_ejecutable);
		    free(nombre_script);

			//wait(&mutex_cant);
			resultado = aplicarRutinaTransformacion(rutaScript, numero_bloque, nombre_archivo_temporal,socket_master);
			//Devuelvo resultado
			send(socket_master, &resultado, sizeof(resultado), 0);
			//post(&mutex_cant);


			log_info(logger, "Se envio el resultado de la etapa transformacion a master\n");


			break;

		case ETAPA_REDUCCION_LOCAL || ETAPA_REDUCCION_GLOBAL:


			log_info(logger, "Se realizo una solicitud de reduccion global\n");


			//Recibo tamanio y char del script
			buffer = malloc(buffer_size = sizeof(uint32_t));
			recv(socket_master, buffer, sizeof(uint32_t), 0);
			memcpy(&tamanio_script, buffer, buffer_size);
			free(buffer);

			programa_ejecutable = malloc(sizeof(char)*tamanio_script);

			while(comparacion != tamanio_script){
				comparacion += recv(socket_master,programa_ejecutable+comparacion,tamanio_script-comparacion, 0);
			}

			/*programa_ejecutable = string_new();
			programa_ejecutable_tmp = malloc(tamanio_script+1);
			memset(programa_ejecutable_tmp,'\0',tamanio_script+1);

			comparacion = 0;
			while(comparacion != tamanio_script){
				comparacion += recv(socket_master,programa_ejecutable_tmp,tamanio_script-comparacion, 0);
				string_append(&programa_ejecutable,programa_ejecutable_tmp);
				memset(programa_ejecutable_tmp,'\0',tamanio_script+1);
			}*/


			log_info(logger, "Recibi el script de reduccion a ejecutar\n");


			//Recibo cantidad de elementos de la lista
			recv(socket_master, &cant_elem, sizeof(cant_elem), 0);

			//Creo la lista, deserializando

			log_info(logger, "La cantidad de nodos a comunicarme es %d\n", cant_elem);

			for(i = 0; i <cant_elem; i++){
				t_worker_archivo* estructura_a_recibir = malloc(sizeof(t_worker_archivo));
				if(recibirYDeserializarMaster(estructura_a_recibir, socket_master)){
					list_add(lista_archivo_workers, estructura_a_recibir);
				}else{
					log_error(logger, "Error al deserializar la estructura mandada por Master\n");
				}
			}

			//Recibo tamanio y nombre del archivo temporal a guardar
			buffer = malloc(buffer_size = sizeof(uint8_t));
			recv(socket_master, buffer, sizeof(uint8_t), 0);
			memcpy(&tamanio_nombre_archivo_temporal, buffer, buffer_size);
			free(buffer);
			nombre_archivo_temporal = string_new();
			nombre_tmp = malloc(tamanio_nombre_archivo_temporal+1);
			memset(nombre_tmp,'\0',tamanio_nombre_archivo_temporal+1);

			comparacion = 0;
			while(comparacion != tamanio_nombre_archivo_temporal){
				comparacion += recv(socket_master,nombre_tmp,tamanio_nombre_archivo_temporal-comparacion,0);
				string_append(&nombre_archivo_temporal,nombre_tmp);
				memset(nombre_tmp,'\0',tamanio_nombre_archivo_temporal+1);
			}


			log_info(logger, "Recibi el nombre del archivo temporal %s\n", nombre_archivo_temporal);


			lista_archivos = buscarArchivosWorkers(lista_archivo_workers,codop);

			if(list_size(lista_archivos) == list_size(lista_archivo_workers)){

				concat_archivos = concatenarArchivos(lista_archivos);


				//ejecuto reduce, me devuelve si salio bien o no
				log_info(logger, "Ejecutando la etapa de reduccion solicitada\n");


				/*char* temporal = string_new();
				string_append(&temporal, dir_temporal);
				string_append(&temporal, nombre_archivo_temporal);*/

			    srand(socket_master);
			    uint8_t* num_rand = malloc(sizeof(uint8_t));
			    *num_rand = rand() % 100;

			    char* nombre_script = string_new();
			    string_append(&nombre_script,string_itoa((int)time(NULL)));
			    string_append(&nombre_script,"-");
			    string_append(&nombre_script,string_itoa(socket_master));
			    string_append(&nombre_script,"-");
			    string_append(&nombre_script,string_itoa(*num_rand));
			    string_append(&nombre_script,"-");
			    string_append(&nombre_script,string_itoa(puerto_worker));

			    //Paso el script a un archivo y le otorgo permisos de ejecucion
			    char* rutaScript = string_new();
			    rutaScript = pasarAArchivoTemporal(programa_ejecutable,nombre_script,".reductor",tamanio_script);
			    permitirEjecucion(rutaScript);
			    free(programa_ejecutable);
			    //free(nombre_script);

				//wait(&mutex_cant);
				resultado = aplicarRutinaReduccion(rutaScript, concat_archivos, nombre_archivo_temporal,socket_master);
				//post(&mutex_cant);
			}else{
				resultado = 0;
			}

			//Devuelvo resultado
			send(socket_master, &resultado, sizeof(resultado), 0);

			log_info(logger, "Se envio el resultado de la etapa reducción a Master\n");


			//free(nombre_archivo_temporal);
			//free(nombre_tmp);
			break;
	}
}

char* concatenarArchivos(t_list* lista_archivos){
	uint8_t cant_elem = list_size(lista_archivos);
	int i;
	uint32_t tamanio_archivo=0;

    srand(time(NULL));
    uint8_t num_rand = rand() % 100;

	char* nombre_archivo = string_new();
	string_append(&nombre_archivo,string_itoa((int)time(NULL)));
    string_append(&nombre_archivo,"-");
    string_append(&nombre_archivo,string_itoa(num_rand));
    string_append(&nombre_archivo,"-");
    string_append(&nombre_archivo,"-antesReduccion");
	string_append(&nombre_archivo,"-sort");
	string_append(&nombre_archivo,".txt");

	srand(cant_elem);
	num_rand = rand() % 100;

	char* antes_sortear = string_new();
	string_append(&antes_sortear,dir_temporal);
	string_append(&antes_sortear,string_itoa((int)time(NULL)));
	string_append(&antes_sortear,"-");
	string_append(&antes_sortear,string_itoa(num_rand));
	string_append(&antes_sortear,"-");
	string_append(&antes_sortear,"concatenacion");

	for(i = 0; i < cant_elem; i++){
		FILE* archi = fopen(list_get(lista_archivos,i),"r+");
		tamanio_archivo += tamanioArchivo(archi);
		fclose(archi);
	}

	FILE* concat = fopen(antes_sortear,"w+");
	fclose(concat);

	char* comando = string_new();
	string_append(&comando,"truncate --size=");
	string_append(&comando,string_itoa(tamanio_archivo));
	string_append(&comando," ");
	string_append(&comando,antes_sortear);


	if(system(comando)==-1) log_error(logger,"Error de system\n");


	char* mapeado = mapearAMemoria(antes_sortear);
	uint32_t posicion =0;
	for(i = 0; i < cant_elem; i++){
		char* parcial = mapearAMemoria(list_get(lista_archivos,i));
		strcpy(mapeado+posicion,parcial);
		posicion += string_length(parcial);
		munmap(parcial,string_length(parcial));
	}

	if(msync(mapeado,posicion,MS_SYNC)==-1){

		log_error(logger,"Error al ejecutar msync del espacio mapeado en memoria");

	}

	munmap(mapeado,posicion);

	sortearArchivo(antes_sortear,nombre_archivo);
	remove(antes_sortear);
	free(antes_sortear);
	free(comando);

	char* final = string_new();
	string_append(&final,dir_temporal);
	string_append(&final,nombre_archivo);

	return final;
}

char* pasarAArchivoTemporal(char* script, char* nombre_archivo, char* extension,uint32_t tamanio){
	char* ruta = string_new();

	//Armo la ruta del archivo temporal
	string_append(&ruta,dir_temporal);
	string_append(&ruta,nombre_archivo);
	string_append(&ruta,extension);

	FILE* archivo = fopen(ruta,"wb");

	fwrite(script,tamanio,1,archivo);

	fclose(archivo);
	//free(script);

	return ruta;
}
uint8_t aplicarRutinaTransformacion(char* script, uint32_t numero_bloque, char* nombre_archivo_tmp, int8_t socket){

	int pipes[2]; //Pipe para comunicar al padre con el hijo
    pid_t pid_hijo;
    uint8_t resultado = 1;

    srand(socket);
    uint8_t* num_rand = malloc(sizeof(uint8_t));
    *num_rand = rand() % 100;

    //Creo la ruta del archivo temporal a donde ira la transformación antes del sort
    //Socket para dos bloques del mismo nodo
    *num_rand = rand() % 100;
    char* ruta_temporal = string_new();
    string_append(&ruta_temporal,"/tmp/");
    string_append(&ruta_temporal,string_itoa((int)time(NULL)));
    string_append(&ruta_temporal,"-");
    string_append(&ruta_temporal,string_itoa(socket));
    string_append(&ruta_temporal,"-");
    string_append(&ruta_temporal,string_itoa(*num_rand));
    string_append(&ruta_temporal,"-");
    string_append(&ruta_temporal,"-rslt-transformador");
    string_append(&ruta_temporal,".txt");

    //creo el pipe propiamente dicho
    pipe(pipes);

    if((pid_hijo=fork()) == -1){

		log_error(logger,"Error en el fork de transformador");

		exit(1);
	}

    if(pid_hijo == 0) { //hijo
        char *argv[]={ script, 0}; //Parametro del execv

        int archivo_tmp_sin_sort = open(ruta_temporal,O_RDONLY|O_WRONLY|O_CREAT,S_IRUSR|S_IWUSR|S_IXUSR);

        //Cierro el extremo del pipe que no uso
        close(pipes[1]);

        //Reasigno el stdin al extremo de lectura del pipe, luego lo cierro
        dup2(pipes[0], STDIN_FILENO);
        close(pipes[0]);

        //Reasigno el stdput al archivo temporal, luego lo cierro
        dup2(archivo_tmp_sin_sort, STDOUT_FILENO);
        close(archivo_tmp_sin_sort);

        if((execv(argv[0], argv))==-1){ //ejecuto el script, devuelve -1 si el exec dio error
        	//log_error(logger,"Error al ejecutar el script mapper\n");
        	resultado = 0;
        }
    } else { //padre
        char* bloque = getBloque(numero_bloque);

        //Cierro el extremo del pipe que no uso
        close(pipes[0]);

        // Escribo el bloque al extremo de escritura del pipe, luego lo cierro
        int length = string_length(bloque);
        int bytesleft = length;
        int total = 0;
        while(total < bytesleft){
        	total += write(pipes[1], bloque+total, bytesleft);
        	bytesleft -= total;
        }
        close(pipes[1]); //IMPORTANTE! si no se cierra, el hijo se va a quedar esperando mas datos

        //Espero a que el hijo termine su ejecucion
        waitpid(pid_hijo,0,0);
        free(bloque);


        //Ordeno el archivo y lo mando al archivo con el nombre que me pasaron
        sortearArchivo(ruta_temporal, nombre_archivo_tmp);
    }

    remove(script);
    remove(ruta_temporal);
    free(script);
    free(ruta_temporal);
    return resultado;
}

uint8_t aplicarRutinaReduccion(char* script, char* concat_archivos, char* nombre_archivo, int8_t socket){
	int pipes[2]; //Pipe para comunicar al padre con el hijo
    pid_t pid_hijo;
    uint8_t resultado = 1;
    //FILE* archivo;
    //char* linea = string_new();

    srand(socket);
    uint8_t* num_rand = malloc(sizeof(uint8_t));
    *num_rand = rand() % 100;

	char* nombre_archivo_tmp = string_new();
	string_append(&nombre_archivo_tmp,dir_temporal);
	string_append(&nombre_archivo_tmp,string_itoa((int)time(NULL)));
    string_append(&nombre_archivo_tmp,"-");
    string_append(&nombre_archivo_tmp,string_itoa(*num_rand));
    string_append(&nombre_archivo_tmp,"-");
    string_append(&nombre_archivo_tmp,"-reduccion");
	string_append(&nombre_archivo_tmp,"-antesSort");
	string_append(&nombre_archivo_tmp,".txt");

    //creo el pipe propiamente dicho
    pipe(pipes);


    if((pid_hijo=fork()) == -1){

		log_error(logger,"Error en el fork de reductor\n");

		exit(1);
	}

    if(pid_hijo == 0) { //hijo
        char *argv[]={ script, 0}; //Parametro del execv

        int archivo_salida_reduccion = open(nombre_archivo_tmp,O_RDONLY|O_WRONLY|O_CREAT,S_IRUSR|S_IWUSR|S_IXUSR);

        //Cierro el extremo del pipe que no uso
        close(pipes[1]);

        //Reasigno el stdin al extremo de lectura del pipe, luego lo cierro
        dup2(pipes[0], STDIN_FILENO);
        close(pipes[0]);

        //Reasigno el stdput al archivo de salida del reduce, luego lo cierro
        dup2(archivo_salida_reduccion, STDOUT_FILENO);
        close(archivo_salida_reduccion);

        if((execv(argv[0], argv))==-1){ //ejecuto el script, devuelve -1 si el exec dio error
        	//log_error(logger,"Error al ejecutar el script reducer\n");
        	resultado = 0;
        }
    } else { //padre
        //Cierro el extremo del pipe que no uso
        close(pipes[0]);

        // Escribo el bloque al extremo de escritura del pipe, luego lo cierro
        //if ((archivo = fopen(concat_archivos, "r+")) == NULL) resultado = 0;
        //while (!feof(archivo)) {
        	//fgets(linea,512,archivo);
        char* concat = mapearAMemoria(concat_archivos);

        	int length = string_length(concat);
        	int bytesleft = length;
        	int total = 0;
        	while(total < bytesleft){
        		total += write(pipes[1], concat+total, bytesleft);
        	    bytesleft -= total;
        	}
        //}

       close(pipes[1]); //IMPORTANTE! si no se cierra, el hijo se va a quedar esperando mas datos

        //Espero a que el hijo termine su ejecucion
        waitpid(pid_hijo,0,0);
        munmap(concat, string_length(concat));
            }

    sortearArchivo(nombre_archivo_tmp,nombre_archivo);

    remove(script);
    remove(concat_archivos);
    remove(nombre_archivo_tmp);
    free(script);
    return resultado;
}

char* mapearAMemoria(char* ruta){
	int mapper;
	char* mapeo2;
	FILE* bin;
	uint64_t tamanio;

	if ((bin = fopen(ruta, "r+b")) == NULL) {
		//Si no se pudo abrir, imprimir el error
		log_error(logger, "Error al abrir el archivo: %s\n", ruta);
	}

	mapper = fileno(bin);
	tamanio = tamanioArchivo(bin);

	if ((mapeo2 = mmap( NULL, tamanio, PROT_READ|PROT_WRITE, MAP_SHARED|MAP_NORESERVE, mapper, 0)) == MAP_FAILED) {
		//Si no se pudo ejecutar el Mmap, imprimir el error
		log_error(logger,
				"Error al ejecutar Mmap del archivo %s de tamaño %d\n",
				 ruta, tamanio);
	}

	close(mapper);
	fclose(bin);
	return mapeo2;
}

uint32_t tamanioArchivo(FILE* bin) {
	struct stat st;

	int descriptor_archivo = fileno(bin);
	fstat(descriptor_archivo, &st);

	return st.st_size;
}

char* getBloque(uint16_t numero_bloque) {
	char* contenido_bloque;

	//voy a leer el mapeo desde que me posiciono en el bloque hasta su tamanio y eso lo devuelvo
	//contenido_bloque = malloc(TAMANIO_BLOQUE+1);
	contenido_bloque = string_substring(mapeo,numero_bloque*TAMANIO_BLOQUE,TAMANIO_BLOQUE);
	//contenido_bloque[TAMANIO_BLOQUE]='\0';

	return contenido_bloque;
}

void sortearArchivo(char* ruta_archivo_tmp, char* nombre_archivo){
	char* comando = string_new();

	//Creo el string con el comando para hacer el sort
	string_append(&comando,"cat ");
	string_append(&comando,ruta_archivo_tmp);
	string_append(&comando," | sort > ");
	string_append(&comando,dir_temporal);
	string_append(&comando,nombre_archivo);


	if(system(comando)==-1) log_error(logger,"Error de system\n");

	free(comando);
}

void permitirEjecucion(char* ruta) {
	chmod(ruta, 00777); //le doy todos los permisos al archivo
}
//Buscamos los archivos a reducir, por cada nodo validamos si somos nostros o no.
t_list* buscarArchivosWorkers(t_list* lista_archivo_workers,uint8_t codop){
	//uint8_t cant_elem = list_size(lista_archivo_workers);
	//uint8_t i;
	//t_nodo_archivo* elem = malloc(sizeof(t_nodo_archivo));
	t_list* lista_archivos =list_create();
	int8_t socket_worker;
	char* archi = string_new();
	//TODO
	if (codop == ETAPA_REDUCCION_LOCAL){

	}else{void EncontrarArchivoWorker(t_worker_archivo* worker){

		if(string_equals_ignore_case(worker->ip_worker,ip_worker) && (worker->puerto == puerto_worker)){
			char* archivo = string_new();
			string_append(&archivo,worker->dir_temp);
			string_append(&archivo,worker->nombre_archivo);
			list_add(lista_archivos, archivo);

		}else{
			socket_worker = conectarConWorker(worker->ip_worker,worker->puerto);
			if (socket_worker != -1) {
			archi = ContenidoOtroWorker(socket_worker,worker->nombre_archivo);
			if(!string_equals_ignore_case(archi,"1")){

				char* nombre = string_new();
				string_append(&nombre,string_itoa((int)time(NULL)));
				string_append(&nombre,"-");
				string_append(&nombre,string_itoa(socket_worker));

				char* ruta = pasarAArchivoTemporal(archi,nombre, ".txt",string_length(archi));

				list_add(lista_archivos,ruta);
				free(archi);
			}
			}
		}
	}


	list_iterate(lista_archivo_workers, (void*)EncontrarArchivoWorker);
	}
	return lista_archivos;
}

char* ContenidoOtroWorker(int8_t socket, char* nombre_archivo){
	int8_t status = 0;
	uint32_t tamanio_cont;
	uint8_t tamanio_nombre = string_length(nombre_archivo);
	uint32_t comparacion = 0;
	uint32_t valid = 0;
	int buffer_size;
	char* contenido_tmp;
	char* contenido;
	char* buffer;

	status = send (socket,ObtenerDatosArchivo,sizeof(uint8_t),0);
		{

		log_error(logger, "El Worker al que le tengo que pedir su archivo temporal se desconecto\n");

		return(string_itoa(1));
	}

	status = send(socket,&tamanio_nombre,sizeof(uint8_t),0);
	if (status == -1){

		log_error(logger, "El nodo al que le tengo que pedir su archivo temporal se desconecto\n");

		return(string_itoa(1));
	}
	status = send(socket, nombre_archivo, tamanio_nombre+1, 0);
	if (status == -1){

		log_error(logger, "El worker al que le tengo que pedir su archivo temporal se desconecto\n");

		return(string_itoa(1));
	}

	buffer = malloc(buffer_size = sizeof(uint32_t));
	status = recv(socket,buffer,sizeof(uint32_t),0);
	if (status == -1 || status == 0){

		log_error(logger, "El worker al que le tengo que pedir su archivo temporal se desconecto\n");

		return(string_itoa(1));
	}


	memcpy(&tamanio_cont, buffer, buffer_size);
	free(buffer);
	contenido_tmp = malloc(tamanio_cont+1);
	memset(contenido_tmp,'\0',tamanio_cont+1);

	while(comparacion != tamanio_cont){
		valid = recv(socket,contenido_tmp,tamanio_cont-comparacion, 0);
		if (valid == -1 || valid == 0){

			log_error(logger, "El worker al que le tengo que pedir su archivo temporal se desconecto\n");

			free(contenido_tmp);
			return(string_itoa(1));
		}
		comparacion += valid;
		string_append(&contenido,contenido_tmp);
		memset(contenido_tmp,'\0',tamanio_cont+1);
	}

	free(contenido_tmp);

	return contenido;
}

void ObtenerDatosArchivo(int8_t socket, int8_t* status){

		uint32_t tamanio_cont_temp;
		uint8_t tamanio_nombre;
		uint32_t comparacion = 0;
		uint8_t buffer_size;
		char* datos_tmp;
		char* nombre_archivo;
		char* contenido_temporal;
		char *buffer;

	buffer = malloc(buffer_size = sizeof(uint8_t));
				recv(socket,buffer,sizeof(uint8_t),0);
				memcpy(&tamanio_nombre, buffer, buffer_size);
				free(buffer);
				nombre_archivo = string_new();
				datos_tmp = malloc(tamanio_nombre+1);
				memset(datos_tmp,'\0',tamanio_nombre+1);

				while(comparacion != tamanio_nombre){
					comparacion += recv(socket,datos_tmp,tamanio_nombre-comparacion,0);
					string_append(&nombre_archivo,datos_tmp);
					memset(datos_tmp,'\0',tamanio_nombre+1);
				}

				sem_wait(&mutex_log);
				log_info(logger, "Solicitud para obtener el archivo: %s\n", nombre_archivo);
				sem_post(&mutex_log);

				contenido_temporal = string_new();
				contenido_temporal = getFileContent(nombre_archivo);

				tamanio_cont_temp = string_length(contenido_temporal);
				*status = send(socket, &tamanio_cont_temp, sizeof(uint32_t),0);

				*status = sendAll(socket, contenido_temporal, tamanio_cont_temp);

				sem_wait(&mutex_log);
				log_info(logger, "Se envio el contenido del archivo: %s\n", nombre_archivo);
				sem_post(&mutex_log);
/*
				if ((socket != fssocket) && (*status ==0 || *status==-1)) {
					sem_wait(&mutex_log);
					log_error(logger, "El nodo del socket:%d se ha desconectado\n", socket);
					sem_post(&mutex_log);
				}
*/
				munmap(contenido_temporal,tamanio_cont_temp);
				free(nombre_archivo);
				free(datos_tmp);

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

char* getFileContent(char* nombre_archivo) {
	char* archivo_mapeado;
	char* ruta = string_new();

	string_append(&ruta, dir_temporal);
	string_append(&ruta, nombre_archivo);

	archivo_mapeado = mapearAMemoria(ruta);

	free(ruta);
	return archivo_mapeado;
}

int8_t conectarConWorker(char* ip, uint16_t puerto){

	log_info(logger, "Conectando con un Worker\n");

	int8_t worker_socket;

	struct addrinfo hints;
	struct addrinfo *serverInfo;

	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;

	log_debug(logger, "Hints completo\n");

	getaddrinfo(ip, string_itoa(puerto), &hints, &serverInfo);	// Carga en serverInfo los datos de la conexion

	//Obtengo el socket
	worker_socket = socket(serverInfo->ai_family, serverInfo->ai_socktype, serverInfo->ai_protocol);
	log_debug(logger, "Socket de otro Worker generado: %d\n", worker_socket);

	int8_t validation = connect(worker_socket, serverInfo->ai_addr, serverInfo->ai_addrlen); // Me conecto
	freeaddrinfo(serverInfo);	// No lo necesitamos mas

	if (validation == -1) {
		log_info(logger, "No me pude conectar al Worker de IP: %s y puerto: %d\n",ip,puerto);
		return -1;
	}

	uint8_t identificacion;
	identificacion = ID_PROC_WORKER;
	send(worker_socket, &identificacion, sizeof(uint8_t), 0);

	log_info(logger, "Conectado al worker de IP: %s y puerto: %d\n",ip,puerto);


	return worker_socket;
}

int8_t recibirYDeserializarMaster(t_worker_archivo* estructura, int8_t socket){
	int8_t status = 1;
	uint8_t buffer_size;
	char*  buffer = malloc(sizeof(uint8_t));
	uint8_t comparacion = 0;
	char* datos_tmp;

	//Obtengo la ip
	uint8_t ip_long;
	buffer_size = sizeof(uint8_t);
	status = recv(socket,buffer,sizeof(estructura->ip_long),0);
	if(status <=0) return 0;
	memcpy(&ip_long, buffer, buffer_size);
	estructura->ip_long = ip_long;
	estructura->ip_worker= malloc(ip_long+1);
	memset(estructura->ip_worker,'\0',ip_long+1);
	datos_tmp = malloc(ip_long+1);
	memset(datos_tmp,'\0',ip_long+1);

	while(comparacion != ip_long){
			comparacion += recv(socket,datos_tmp,ip_long-comparacion, 0);
			string_append(&estructura->ip_worker,datos_tmp);
			memset(datos_tmp,'\0',ip_long+1);
	}
	//status = recv(socket, estructura->ip_nodo, ip_long, 0);

	//printf("ip %s\n", estructura->ip_nodo);
	if(status <=0) return 0;

	//Obtengo el puerto
	status = recv(socket, &(estructura->puerto), sizeof(uint16_t), 0);
	//printf("puerto %d\n", estructura->puerto);
	if(status <=0) return 0;

	//Obtengo el nombre de archivo
	uint8_t nombre_long;
	buffer_size = sizeof(uint8_t);
	status = recv(socket,buffer,sizeof(estructura->nombre_archivo_long),0);
	if(status <=0) return 0;
	memcpy(&nombre_long, buffer, buffer_size);
	estructura->nombre_archivo_long = nombre_long;
	estructura->nombre_archivo = malloc(nombre_long+1);
	memset(estructura->nombre_archivo,'\0',nombre_long+1);
	datos_tmp = malloc(nombre_long+1);
	memset(datos_tmp,'\0',nombre_long+1);

	comparacion = 0;
	while(comparacion != nombre_long){
			comparacion += recv(socket,datos_tmp,nombre_long-comparacion, 0);
			string_append(&estructura->nombre_archivo,datos_tmp);
			memset(datos_tmp,'\0',nombre_long+1);
	}

	//status = recv(socket, estructura->nombre_archivo, nombre_long, 0);
	//printf("nombre arch %s\n", estructura->nombre_archivo);
	if(status <=0) return 0;

	free(datos_tmp);
	free(buffer);
	return status;
}

int main(void) {

	cargarConfiguraciones();

	log_debug(logger, "Se cargaron correctamente las configuraciones.");
	int8_t listener = pthread_create(&thr_listener, NULL, crearListenerSocket, NULL);
	if(listener){
			sem_wait(&mutex_log);
			log_error(logger,"No se pudo crear el hilo listener\n");
			sem_post(&mutex_log);
			abort();
		}

	log_info(logger, "<<Proceso Worker finalizó>>");
	return EXIT_SUCCESS;
}
