#include <stdio.h>
#include <stdlib.h>
#include <semaphore.h> 
#include <unistd.h>
#include <pthread.h>
#include "messageAdder.h"
#include "msg.h"
#include "iMessageAdder.h"
#include "multitaskingAccumulator.h"
#include "iAcquisitionManager.h"
#include "debug.h"

//consumer thread
pthread_t consumer;
//Message computed
volatile MSG_BLOCK out;
//Consumer count storage
volatile unsigned int consumeCount = 0;

pthread_mutex_t mutex_consume= PTHREAD_MUTEX_INITIALIZER; // 1 at the start


/**
 * Increments the consume count.
 */
static void incrementConsumeCount(void);

/**
 * Consumer entry point.
 */
static void *sum( void *parameters );

static void incrementConsumeCount(void)
{
	consumeCount++;
}


MSG_BLOCK getCurrentSum(){
	MSG_BLOCK c;
	pthread_mutex_lock(&mutex_consume);
	c = out;
	pthread_mutex_unlock(&mutex_consume);

	return c;
}

void getCoherentCount(MSG_BLOCK *sum, unsigned int *count) // Q8 amélioration de la cohérence en regroupant les deux fonctions
{
	pthread_mutex_lock(&mutex_consume);
	*count = consumeCount;  // copie du compteur, dans la MÊME section critique

	*sum   = out;           // copie de la somme
	pthread_mutex_unlock(&mutex_consume);
}


unsigned int getConsumedCount(){
	pthread_mutex_lock(&mutex_consume);
	unsigned int csm_count = consumeCount;
	pthread_mutex_unlock(&mutex_consume);
	return csm_count;
	
}

void messageAdderInit(void){
	out.checksum = 0;
	for (size_t i = 0; i < DATA_SIZE; i++)
	{
		out.mData[i] = 0;
	}
	pthread_create(&consumer, NULL, sum,NULL);
}

void messageAdderJoin(void){
	pthread_join(consumer, NULL);
}

static void *sum( void *parameters )
{
	D(printf("[messageAdder]Thread created for sum with id %d\n", gettid()));
	unsigned int i = 0;
	MSG_BLOCK new_message;

	while(i<ADDER_LOOP_LIMIT){
		i++;
		sleep(ADDER_SLEEP_TIME);
		new_message = getMessage();
		pthread_mutex_lock(&mutex_consume);
		messageAdd(&out,&new_message);
		incrementConsumeCount();
		pthread_mutex_unlock(&mutex_consume);

	}
	printf("[messageAdder] %d termination\n", gettid());
	pthread_exit(NULL);
}


