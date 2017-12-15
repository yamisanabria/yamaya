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
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>
#include <sys/wait.h>
#include <signal.h>
#include <errno.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>

#define FILESYSTEM 201
#define WORKER 202
#define DATANODE 30
#define MASTER 204
#define YAMA 31

#define BACKLOG 5
#define DATANODE 30
#define SOLICITO_INFORMACION_BLOQUES_ARCHIVO 82

#define GET_BLOQUE 1
#define SET_BLOQUE 2

#define HANDSHAKEOK 101
#define SIZE_MSG sizeof(t_mensaje)
#define TAMANIO_BLOQUE 1048576 //1mb
#define NODO_CAIDO 25

t_list *listaNodosActivos; //lista de t_nodosInterno
t_list *listaNodosEsperando; //lista de t_nodosInterno
t_list *listaNodosDesconectados;//lista de t_nodosInterno
t_list *listaNombreNodos; //lista de Nombre Nodos
t_list *tablaDeArchivos; //lista de t_archivos
t_list *listaFinal;
t_list *nodoParaYama;

sem_t mutex_logger;
sem_t mutex_dataNode;

t_list* lista_directorios;
t_list *listaTamanioNodos;
t_list *listas_bloques_de_archivos;

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
uint8_t tamanioLibreActual;
uint8_t codigo_operacion;
uint16_t id_directorio;
uint8_t espacioDisponible;
uint8_t cod_operacion;

typedef struct{
	char nombre[255];
	float tamanio;
	char tipo[255];
	char* directorio;
	int disponible;
	t_list* bloques; //lista de lista de t_bloquesCopia
	t_list* nodosDePartes; //sirve para saber si un archivo esta disponible
}__attribute__((packed)) t_file;

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
	char* nombreNodo;
	uint8_t tamanioTotal;
	uint8_t tamanioLibre;
}__attribute__((packed)) t_tamanioNodo;

typedef struct {
	char id[16];
	int puerto;
	char ip[16];
} t_nodoYama;

typedef struct t_parte {
	uint16_t nro_parte_archi;
	t_list* lista_nodos_parte;
} __attribute__((packed)) t_parte;

typedef struct {
	char* nombre_nodo;
	uint16_t puerto_nodo;
	char* ip_nodo;
	uint16_t cant_max_bloques;
    int sockn;
	uint8_t estado; //0 si es nuevo
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

typedef struct t_nodo_bloque {
	char* nombre_archivo;
	uint8_t nombre_archivo_long;
	uint16_t nro_bloque_archi;
	uint16_t nro_bloque_archi_long;
	char* id_nodo;
	uint8_t id_nodo_long;
	uint16_t nro_bloque_nodo;
	uint16_t nro_bloque_nodo_long;
	char* ip;
	uint8_t ip_long;
	uint16_t puerto;
	uint16_t puerto_long;
}__attribute__((packed)) t_nodo_bloque;

typedef struct {
	uint8_t codigo_operacion_long;
	uint8_t codigo_operacion;
	uint16_t numero_bloque_long;
	uint16_t numero_bloque;
	uint32_t tam_datos;
	char* datos;
	uint32_t tamanio_datos;
}__attribute__((packed)) t_datos;

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
int agregarNodoAestructura(t_datanode* tnodo, int sockN);
void cargarNodo(t_datanode* nodoRecibido, int socketN);
t_nodoInterno* nodoBuscadoEnListaNodosActivos(char* ip_nodo, uint16_t puerto_nodo, char* nombre) ;
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
uint8_t seteoDeBloque(t_nodoInterno* nodo, uint16_t numero_bloque, char* datos);
char* getBloque(uint16_t numerodebloque);
char* mapearAMemoria(char* RutaDelArchivo);
int tamanioArchivo(FILE* bin);
int calcularCantidadBloques() ;
uint8_t cpfrom(char* RutaDelArchivo,char* dirDestinofs);
t_tamanioNodo* nodoBuscadoEnListaDeTamanios(char* nombre);
void eliminarNodo(int sockNodo);
void desconexionDeNodo(int sockNodo);
t_nodoInterno* nodoBuscadoEnListaNodosPorSocket(int socket);
uint8_t dividirElArchivoEnBloques(FILE* archivo, t_file* arch, char* ruta);
t_nodo_bloque* bloquesDeNodoLibre(t_nodoInterno* nodo,uint16_t nro_bloque_archi, char* nom_archi);
uint16_t obtenerElPrimerBloqueLibre(t_nodoInterno *nodo);
uint32_t posHastaElEspacio(char* bloque);
void borrarBloquesDePartes(t_list* listaBloquesDelArchivo);
void freeArchivo(t_file* archivo);
uint8_t nodoDesconectadoEnviarAOtroNodoOAbortar(t_nodoInterno* nodo, uint8_t socket_nodo);
void quitarNodoDeLista(uint8_t nodo_socket);
int enviarDatos_DN (int sockn,char* datos, uint16_t numero_bloque,uint8_t codigo_operacion);
char* serializarEstructura(t_datos* datosAenviar);
void dispose_package(char **package);

#endif /* FILESYSTEM_H_ */
