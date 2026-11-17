#include <unistd.h>
#include <stdio.h>
#include <pthread.h>
#include "displayManager.h"
#include "iDisplay.h"
#include "iAcquisitionManager.h"
#include "iMessageAdder.h"
#include "msg.h"
#include "multitaskingAccumulator.h"
#include "debug.h"

// DisplayManager thread.
pthread_t displayThread;

/**
 * Display manager entry point.
 * */
static void *display( void *parameters );


void displayManagerInit(void){
	pthread_create(&displayThread, NULL, display, NULL);
}

void displayManagerJoin(void){
	pthread_join(displayThread, NULL);
} 

static void *display( void *parameters )
{
	D(printf("[displayManager]Thread created for display with id %d\n", gettid()));
	unsigned int diffCount = 0;
	while(diffCount < DISPLAY_LOOP_LIMIT){
		sleep(DISPLAY_SLEEP_TIME);
		MSG_BLOCK message;
		unsigned int consumedCount;
		getCoherentCount(&message, &consumedCount);  // Q8 fonction pour une meilleure cohérence
		unsigned int producedCount = getProducedCount(); 

		messageDisplay(&message);
		print(producedCount, consumedCount);

		if (consumedCount == PRODUCER_COUNT * PRODUCER_LOOP_LIMIT)
		{
		diffCount++;
		}
	}
	printf("[displayManager] %d termination\n", gettid());
    pthread_exit(NULL);

}