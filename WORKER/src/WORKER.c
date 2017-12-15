/*
 ============================================================================
 Name        : WORKER.c
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

#include <pthread.h>

#include <fcntl.h>
#include <errno.h>
#include <netinet/in.h>
#include <resolv.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
t_log* logger;
t_config* configuration;
char* nombreNodo;
uint16_t puertoWorker;
char* rutaDataBin;
int configOk=1;

/** Puerto  */
#define PORT       7000

/** Longitud del buffer  */
#define BUFFERSIZE 512

/** Número máximo de hijos */
#define MAX_CHILDS 3

int AtiendeAMaster(int socket, struct sockaddr_in addr);
int DemasiadosClientes(int socket, struct sockaddr_in addr);
void error(int code, char *err);
void reloj(int loop);

void cargarConfiguraciones() {
	logger = log_create("logWorker", "WORKER LOG", true, LOG_LEVEL_DEBUG);
	log_info(logger, "<<Proceso Worker inició>>");
	configuration = config_create("/home/utnso/workspace/tp-2017-2c-Yamaya/CONFIG_NODO");
	log_info(logger, "Intentando levantar el archivo de configuraciones.");
	if(configuration==NULL){
		log_error(logger, "Error el archivo de configuraciones no existe.");
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

		puertoWorker = config_get_int_value(configuration, "PUERTO_WORKER");

		log_info(logger, "El puerto del Worker es: %i", puertoWorker);

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

int levantarEscuchasParaMaster(){
	  int socket_WorkerHost;
	    struct sockaddr_in master_addr;
	    struct sockaddr_in mi_addr;
	    struct timeval tv;      /* Para el timeout del accept */
	    socklen_t size_addr = 0;
	    int socket_master;
	    fd_set rfds;        /* Conjunto de descriptores a vigilar */
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
	    mi_addr.sin_port = htons(PORT);
	    mi_addr.sin_addr.s_addr = INADDR_ANY ;

	    int activado=1;
		setsockopt(socket_WorkerHost, SOL_SOCKET, SO_REUSEADDR, &activado, sizeof(activado)); //Para decir al sistema de reusar el puerto


	    if( bind( socket_WorkerHost, (struct sockaddr*)&mi_addr, sizeof(mi_addr)) == -1 ){
	      log_error(logger, "Falló el bind. (¿El puerto no está en uso o hay dos instancias de Worker corriendo paralelas?)"); /* Error al hacer el bind() */
	    exit (-1);}

	    if(listen( socket_WorkerHost, 10) == -1 ){
	      log_error(logger, "Falló el listen, no puedo escuchar en el puerto especificado.");
	    }
	    size_addr = sizeof(struct sockaddr_in);


	    while(activated)
	      {
	    reloj(loop);
	    /* select() se carga el valor de rfds */
	    FD_ZERO(&rfds);
	    FD_SET(socket_WorkerHost, &rfds);

	    /* select() se carga el valor de tv */
	    tv.tv_sec = 0;
	    tv.tv_usec = 500000;    /* Tiempo de espera */

	    if (select(socket_WorkerHost+1, &rfds, NULL, NULL, &tv))
	      {
	        if((socket_master = accept( socket_WorkerHost, (struct sockaddr*)&master_addr, &size_addr))!= -1)
	          {
	        loop=-1;        /* Para reiniciar el mensaje de Esperando conexión... */
	        printf("\nSe ha conectado %s por su puerto %d\n", inet_ntoa(master_addr.sin_addr), master_addr.sin_port);
	        switch ( childpid=fork() )
	          {
	          case -1:  /* Error */
	        	  log_error(logger, "No se puede crear el proceso hijo. (FORK Trouble)");
	            break;
	          case 0:   /* Somos proceso hijo */
	            if (childcount<MAX_CHILDS)
	              exitcode=AtiendeAMaster(socket_master, master_addr);
	            else
	              exitcode=DemasiadosClientes(socket_master, master_addr);

	            exit(exitcode); /* Código de salida */
	          default:  /* Somos proceso padre */
	            childcount++; /* Acabamos de tener un hijo */
	            close(socket_master); /* Nuestro hijo se las apaña con el cliente que
	                         entró, para nosotros ya no existe. */
	            break;
	          }
	          }
	        else
	        	 log_error(logger, "Error al aceptar la conexión.\n");
	      }

	    /* Miramos si se ha cerrado algún hijo últimamente */
	    childpid=waitpid(0, &pidstatus, WNOHANG);
	    if (childpid>0)
	      {
	        childcount--;   /* Se acaba de morir un hijo */

	        /* Muchas veces nos dará 0 si no se ha muerto ningún hijo, o -1 si no tenemos hijos
	         con errno=10 (No child process). Así nos quitamos esos mensajes*/

	        if (WIFEXITED(pidstatus))
	          {

	        /* Tal vez querremos mirar algo cuando se ha cerrado un hijo correctamente */
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
    int code=0;         /* Código de salida por defecto */


    while (!fin)
      {

    memset(buffer, 0, BUFFERSIZE);
    if((bytecount = recv(socket, buffer, BUFFERSIZE, 0))== -1)
    	log_error(logger,"No se puede recibir información. (RECV)");

    /* Evaluamos los comandos */
    /* El sistema de gestión de comandos es muy rudimentario, pero nos vale */
    /* Comando TIME - Da la hora */
    if (strncmp(buffer, "EXIT", 4)==0)
      {
        memset(buffer, 0, BUFFERSIZE);
        sprintf(buffer, "Hasta luego. Vuelve pronto %s\n", inet_ntoa(addr.sin_addr));
        fin=1;
      }
    /* Comando CERRAR - Cierra el servidor */
    else if (strncmp(buffer, "CERRAR", 6)==0)
      {
        memset(buffer, 0, BUFFERSIZE);
        sprintf(buffer, "Adiós. Cierro el servidor\n");
        fin=1;
        code=99;        /* Salir del programa */
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

int main(void) {

	cargarConfiguraciones();

	log_debug(logger, "Se cargaron correctamente las configuraciones.");
	levantarEscuchasParaMaster();

	log_info(logger, "<<Proceso Worker finalizó>>");
	return EXIT_SUCCESS;
}
