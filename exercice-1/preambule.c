#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <semaphore.h>
#include <fcntl.h>

#define ERROR_INIT 1
#define ERROR_SUCCESS 0
#define SEMAPHORE_INITIAL_VALUE 0
#define SEM_NAME "/preambule_sem"

pthread_t thread_1; // Q2. Nom du thread
sem_t *semaphore;   // Q3. Nom du sémaphore

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER; // Q4. Nom du mutex

void *produce(void *params)
{
    printf("Trying to own the mutex\n");
    pthread_mutex_lock(&mutex);
    printf("Owns the mutex\n");
    pthread_mutex_unlock(&mutex);
    printf("Mutex released\n");
    sem_post(semaphore);
    printf("Semaphore posted\n");
    printf("Trying to gets the semaphore\n");
    sem_wait(semaphore);
    printf("Semaphore taken\n");
    printf("Thread ended\n");
    pthread_exit(NULL);
}

int main()
{
    sem_unlink(SEM_NAME);
    semaphore = sem_open(SEM_NAME, O_CREAT, 0644, SEMAPHORE_INITIAL_VALUE);
    if (semaphore == SEM_FAILED)
    {
        perror("[sem_open");
        return ERROR_INIT;
    }
    printf("Creating the thread\n");
    pthread_create(&thread_1, NULL, produce, NULL); // Q5. Création du thread & Q6. Point d'entrée du thread (produce)
    // Q8. Thread par défaut: thread 1 (premier thread)

    printf("Waiting the thread end\n");
    pthread_join(thread_1, NULL); // Q7. Attente de la n du thread
    printf("Deleting the semaphore\n");
    sem_destroy(semaphore);
    printf("Process ended\n");
    return ERROR_SUCCESS;
}

// Q9. On utilise make qui va faire appel au compilateur pour générer un exécutable puis lancer ce-dernier
// Q10. Cf. Diagramme (note: ./preambule est un EXEC par un FORK)
// Q11.