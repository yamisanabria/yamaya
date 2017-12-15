/*
 * fileSystem.h
 *
 *  Created on: 11/9/2017
 *      Author: utnso
 */

/*
 * fileSystem.h
 *
 *  Created on: 11/9/2017
 *      Author: utnso
 */

#ifndef FILESYSTEM_H_
#define FILESYSTEM_H

#include <commons/collections/list.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <readline/readline.h>
#include <commons/log.h>
#include <commons/config.h>
#include <commons/temporal.h>
#include <commons/string.h>
#include <stdint.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h> //SLEEP FUNCTION
#include <malloc.h>
#include <sys/stat.h> //tamaño archivo
#include <semaphore.h>
#include <commons/bitarray.h>
#include <string.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>

#define YAMA 200
#define FILESYSTEM 201
#define WORKER 202
#define DATANODE 203
#define MASTER 204

#define GET_BLOQUE 1
#define SET_BLOQUE 2

#define HANDSHAKE 100
#define HANDSHAKEOK 101
#define SIZE_MSG sizeof(t_mensaje)

t_list *listaNodosActivos; //lista de t_nodosInterno
t_list *listaNodosEsperando; //lista de t_nodosInterno
t_list *listaNodosDesconectados;//lista de t_nodosInterno
t_list *listaNombreNodos; //lista de Nombre Nodos
t_list *tablaDeArchivos; //lista de t_archivos
t_list *listaFinal;
t_list *nodoParaYama;
sem_t mutex_logger;
t_list* lista_directorios;

int punteroNodoAllenar;
int socketDataNode;
int cantNodosConec;
int contadorIdNodo;
char* redundanciaArchivo;
int sockYama;
int sockdataNode;
int yamaConectado;
uint8_t tamanioTotalNodos;
uint8_t tamanioLibreNodos;
uint8_t codigo_operacion;
uint16_t id_directorio;

typedef struct{
	char nombre[255];
	float tamanio;
	char tipo[255];
	int directorio;
	int disponible;
	t_list* bloques; //lista de lista de t_bloquesCopia
}t_file;

typedef struct{
	char nodo[16];
	int bloque;
	float tamanioBloque;
	char* disponible;
}t_bloquesCopia;

typedef struct{
	int index;
	char nombre[255];
	int padre;
}__attribute__((packed))t_directorio;

typedef struct{
	uint8_t tamanioTotal;
	uint8_t tamanioLibre;
}__attribute__((packed)) t_nodoConectado;

typedef struct {
	char id[16];
	int puerto;
	char ip[16];
} t_nodoYama;

typedef struct {
	char* nombre_nodo;
	uint16_t puerto_nodo;
	char* ip_nodo;
	uint16_t cant_max_bloques;
    int sockn;
    t_bitarray* bloques;
}__attribute__((packed)) t_nodoInterno;

typedef struct {
	uint16_t nombreNodo_long;
	char* nombreNodo;
	uint16_t puertoNodo_long;
	uint16_t puertoNodo;
	uint8_t ipNodo_long;
	char*ipNodo;
	uint16_t cantidad_bloques_long;
	uint16_t cantidad_bloques;
	uint32_t total_size;
}__attribute__((packed)) t_datanode;


typedef struct {
	int tipo;
	int id_proceso;
	int datosNumericos;
	char mensaje[16];
} t_mensaje;

void cargarConfiguraciones();
void leer_palabra();
void reconocer_comando(char * linea);
void formatear_filesystem();
//int enviar_saludo(int id_origen, int sock, t_log* logger,int tipo_mensaje);
//int recibir_saludo(int id_destino, int sock, t_log* logger,int tipo_mensaje);
int conexion_nueva(int new_socket);
int conectar_servidor (int puerto, t_log* logger);
int conexion_datanode(int socket);
int conexion_yama(void* param);
void limpiar(char *cadena);
int cantidadDeMemoriaSDisponible(void);
int existeArchivoEnDirectorio(int directorioActual,char* nomArch);
int dividirArchivoUsuario(char* pathArchLocal, int directorio);
t_nodoInterno* buscarNodo(t_list* listaNodosActivos,int idNodo);
void set_bit(t_list*  lista, int indice);
void clean_bit(t_list* lista,int indice);
int asigno_redundancia_nodo(void);
int proximo_bloque_libre(int nodo_redun);
int recibirYDeserializar(t_datanode* tnodo, int sockN);
void agregarNodoAestructura(t_datanode* tnodo, int sockN);
void cargarNodo(t_datanode* nodoRecibido, int socketN);
t_nodoInterno* nodoBuscadoEnListaNodosPorIPYPuerto(char* ip, uint16_t puerto);
void persistirBloquesNodo(t_nodoInterno *nodoApersistir);
int obtenerCantidadBloquesLibresNodo(t_nodoInterno* nodo);
char* concat(char* s1, char* s2);
void levantarDirectorios();
void levantarNodos();
void persistirDirectorios();
bool comparadorDeBloques(t_nodoInterno*nodo1, t_nodoInterno *nodo2);
t_list* obtenerNodosMasLibres();
uint16_t bloquesLibresDeUnNodo(t_nodoInterno *nodo);
char* obtenerElContenidoDeUnBloque(uint16_t nro_bloque, t_nodoInterno* nodo);
uint8_t seteoDeBloque(t_nodoInterno* nodo, uint16_t numero_bloque,uint8_t socket_nodo, char* datos);
char* retrocedoHastaLaBarra();
void mostrar_cont_arch();
int listar_archivos(char* ruta);
void error(const char *s);
void procesoArchivo(char *archivo);
int ValidarMetadata();


#endif /* FILESYSTEM_H_ */
