#include <stdio.h>
#include <stdlib.h>
#include <semaphore.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>
#include <stdatomic.h>
#include "acquisitionManager.h"
#include "msg.h"
#include "iSensor.h"
#include "multitaskingAccumulator.h"
#include "iAcquisitionManager.h"
#include "debug.h"

#define BUFFER_SIZE PRODUCER_COUNT

//producer count storage
static atomic_int produceCount = 0;
static int ids[PRODUCER_COUNT];
MSG_BLOCK data_buffer[BUFFER_SIZE];
int tab_indices_libres[BUFFER_SIZE];
int tab_indices_pleines[BUFFER_SIZE];
int j_vide; // variable globale des indices du tableau cases vides pour pour produce (produceur)
int j_plein; // variable globale des indices du tableau cases pleines pour produce
int i_vide; // variable globale des indices du tableau cases vides pour getmessage (consumer)
int i_plein; // variable globale des indices du tableau cases pleines getmessage (consumer)

pthread_t producers[4];

static void *produce(void *params);

/**
* Semaphores and Mutex
*/
pthread_mutex_t mutex_idx_libre = PTHREAD_MUTEX_INITIALIZER; // 1 at the start
pthread_mutex_t mutex_idx_plein = PTHREAD_MUTEX_INITIALIZER; // 1 at the start

sem_t *sem_libre;
sem_t *sem_plein;
/*
* Creates the synchronization elements.
* @return ERROR_SUCCESS if the init is ok, ERROR_INIT otherwise
*/
static unsigned int createSynchronizationObjects(void);

/*
* Increments the produce count.
*/
static void incrementProducedCount(void);

static unsigned int createSynchronizationObjects(void)
{
	sem_unlink("/sem_libre");
	sem_libre = sem_open("/sem_libre",O_CREAT,0664,BUFFER_SIZE);
	if (sem_libre == SEM_FAILED)
	{
		perror("sem_open sem_libre");
		return ERROR_INIT;
	}
	sem_unlink("/sem_plein");
	
	sem_plein = sem_open("/sem_plein", O_CREAT, 0644, 0);
	if (sem_plein == SEM_FAILED)
	{
		perror("sem_open sem_plein");
		return ERROR_INIT;
	}
	printf("[acquisitionManager]Semaphore created\n");
	return ERROR_SUCCESS;
}

static void incrementProducedCount(void)
{
	produceCount++;
	//atomic_fetch_add(&produceCount, 1);
}

unsigned int getProducedCount(void)
{
	unsigned int p = 0;
	p = atomic_load(&produceCount);

	return p;
}

MSG_BLOCK getMessage(void){

	int i=0;
	sem_wait(sem_plein);
	i = tab_indices_pleines[i_plein]; // récupération indice de la cade pleine
	MSG_BLOCK val = data_buffer[i]; // sotckage de la donnée de la case pleine dans une variable
	i_plein=(i_plein+1)%BUFFER_SIZE;

	tab_indices_libres[i_vide]=i; //ajout de l'indice de la case lu dans le tableau des cases libres
	i_vide=(i_vide+1)%BUFFER_SIZE;

	sem_post(sem_libre);
	return val;
}


unsigned int acquisitionManagerInit(void)
{
	unsigned int i;
	printf("[acquisitionManager]Synchronization initialization in progress...\n");
	fflush( stdout );
	if (createSynchronizationObjects() == ERROR_INIT)
		return ERROR_INIT;
	
	printf("[acquisitionManager]Synchronization initialization done.\n");

	// initialisations des tableaux et variables

	for(i=0; i<BUFFER_SIZE; i++){
		tab_indices_pleines[i]=0;
		tab_indices_libres[i]=i;
	}

	for(i=0; i<PRODUCER_COUNT; i++){
		ids[i]=i;
	}

	j_vide =0;
	j_plein=0;
	i_vide=0;
	i_plein=0;

	for (i = 0; i < PRODUCER_COUNT; i++)
	{
		pthread_create(&producers[i], NULL, produce,&ids[i]);
	}

	return ERROR_SUCCESS;
}

void acquisitionManagerJoin(void)
{
	unsigned int i;
	for (i = 0; i < PRODUCER_COUNT; i++)
	{
		pthread_join(producers[i], NULL);
	}

	sem_close(sem_libre);
	sem_unlink("/sem_libre");
	sem_close(sem_plein);
	sem_unlink("/sem_plein");
	printf("[acquisitionManager]Semaphore cleaned\n");
}

void *produce(void* params)
{
	D(printf("[acquisitionManager]Producer created with id %d\n", gettid()));
	int capteur = *(int *) params;
	unsigned int i = 0;
	int j=0;
	while (i < PRODUCER_LOOP_LIMIT)
	{
		i++;
		sleep(PRODUCER_SLEEP_TIME+(rand() % 5));
		MSG_BLOCK local_buffer;
		getInput(capteur, &local_buffer); // Get the message from the sensor
		if (messageCheck(&local_buffer)){ //Message verification
			
			sem_wait(sem_libre);
			
			pthread_mutex_lock(&mutex_idx_libre);
			j=tab_indices_libres[j_vide];
			j_vide=(j_vide+1)%BUFFER_SIZE;
			pthread_mutex_unlock(&mutex_idx_libre);
			
			data_buffer[j]=local_buffer;
			
			pthread_mutex_lock(&mutex_idx_plein);
			tab_indices_pleines[j_plein]=j;
			j_plein=(j_plein +1)%BUFFER_SIZE;
			pthread_mutex_unlock(&mutex_idx_plein);

			incrementProducedCount();

			sem_post(sem_plein);
		}
		
	}
	printf("[acquisitionManager] %d termination\n", gettid());
	pthread_exit(NULL);
}