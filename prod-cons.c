#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <math.h>
#include <pthread.h>
#include <sys/time.h>

#define SIZEOFQUEUE 10
#define JOBS_PER_PRODUCER 15000
#define NUM_PRODUCERS 2
#define NUM_CONSUMERS 2

// pthread_mutex_t store_mut;
// int store_cnt = 0;
// double *results;

// double *start_time;
// double *end_time;

// This is the function and it's arguments --> takes jobArg type arguments

typedef struct workfunction{
    void * (*work)(void *);
    void *arg;
}workfunction;

typedef struct{
    workfunction *buf[SIZEOFQUEUE]; // Circular buffer
    int head, tail, count;
    pthread_mutex_t mut;
    pthread_cond_t notFull;
    pthread_cond_t notEmpty;
}queue;

// This is used in producer to feed the queue addjob function

typedef struct{
    int producer_id;
    int job_id;
    double start_angle;
}jobArg;

// This is used in main to feed the producer function

typedef struct{
    queue *q;
    int producer_id;
}producerArg;

// Functions

void queueInit(queue *q);
void queueDestroy(queue *q);
void queueAdd(queue *q, workfunction *wf);
workfunction *queueDel(queue *q);
void *func(void *arg);
void *producer(void *arg);
void *consumer(void *arg);

int main(void){

    // results = (double *)calloc(JOBS_PER_PRODUCER*NUM_PRODUCERS, sizeof(double));
    // start_time = (double *)malloc(sizeof(double)*JOBS_PER_PRODUCER*NUM_PRODUCERS); 
    // end_time = (double *)malloc(sizeof(double)*JOBS_PER_PRODUCER*NUM_PRODUCERS);

    // double av_time = 0;

    queue q;
    pthread_t pth_id[NUM_PRODUCERS];
    pthread_t cth_id[NUM_CONSUMERS];
    producerArg pargs[NUM_PRODUCERS];

    queueInit(&q);

    // Creating threads

    for (int i=0; i<NUM_CONSUMERS; i++){
        pthread_create(&cth_id[i], NULL, consumer, &q);
    }

    for (int i=0; i<NUM_PRODUCERS; i++){

        pargs[i].q = &q;
        pargs[i].producer_id = i + 1;

        pthread_create(&pth_id[i], NULL, producer, &pargs[i]);
    }

    // Joining threads

    for (int i=0; i<NUM_PRODUCERS; i++){
        pthread_join(pth_id[i], NULL);
    }

    printf("Producers have placed all of their jobs in line!\n");

    // After all of the producers put their jobs in the line, we put +1
    // for each of the consumers, in order to kill them

    for (int i=0; i<NUM_CONSUMERS; i++){
        workfunction *poison_job = malloc(sizeof(workfunction));

        if (poison_job == NULL){
            printf("Malloc of poison_job #%d error", (i+1));
            exit(1);
        }

        poison_job->work = NULL;
        poison_job->arg = NULL;
        queueAdd(&q, poison_job);
    }

    // Wait for consumers to terminate thanks to the poison pill

    for (int i=0; i<NUM_CONSUMERS; i++){
        pthread_join(cth_id[i], NULL);
    }
    
    printf("Consumers ended, end of program!\n");

    queueDestroy(&q);

    // printf("Results:\n");

    // for (int i=0; i<JOBS_PER_PRODUCER*NUM_PRODUCERS-1; i++){
    //     printf("%f, ", results[i]);
    // }

    // for (int i=JOBS_PER_PRODUCER*NUM_PRODUCERS-1; i<JOBS_PER_PRODUCER*NUM_PRODUCERS; i++){
    //     printf("%f\n", results[i]);
    // }

    // Average time calculation

    // for (int i=0; i<NUM_PRODUCERS*JOBS_PER_PRODUCER; i++){
    //     av_time = av_time + end_time[i] - start_time[i];
    // }
    
    // av_time = av_time/(NUM_PRODUCERS*JOBS_PER_PRODUCER); 

    // printf("Place a job in the queue until a consumer receives this specific job. Average time: %lf us\n", av_time);

    // free(results);
    // free(start_time);
    // free(end_time);

    return 0;
}

void queueInit(queue *q){
    q->head = 0;
    q->tail = 0;
    q->count = 0;
    pthread_mutex_init(&q->mut, NULL);
    // pthread_mutex_init(&store_mut, NULL);
    pthread_cond_init(&q->notFull, NULL);
    pthread_cond_init(&q->notEmpty, NULL);
}

void queueDestroy(queue *q){
    pthread_mutex_destroy(&q->mut);
    // pthread_mutex_destroy(&store_mut);
    pthread_cond_destroy(&q->notFull);
    pthread_cond_destroy(&q->notEmpty);
}

void queueAdd(queue *q, workfunction *wf){
    pthread_mutex_lock(&q->mut);

    while (q->count == SIZEOFQUEUE){
        // printf("Queue full, waiting for free space . . .\n");
        pthread_cond_wait(&q->notFull, &q->mut);
    }

    q->buf[q->tail] = wf;
    q->tail = (q->tail + 1) % SIZEOFQUEUE;
    q->count++;

    // Saving the time that the job got added in the queue
    // if (wf->arg != NULL){

    //     struct timeval current_time;
    //     gettimeofday(&current_time, NULL);
    //     start_time[(((((jobArg *)(wf->arg))->producer_id)-1)*JOBS_PER_PRODUCER)+(((jobArg *)(wf->arg))->job_id)-1] = (current_time.tv_sec * 1e6) + current_time.tv_usec; // Classic way of storing data (i*N +j)   
    // } 

    pthread_mutex_unlock(&q->mut);
    pthread_cond_signal(&q->notEmpty);
}

workfunction *queueDel(queue *q){
    workfunction *wf;  
    
    pthread_mutex_lock(&q->mut);

    while (q->count == 0){
        // printf("Queue empty, waiting for more work . . .\n");
        pthread_cond_wait(&q->notEmpty, &q->mut);
    }

    wf = q->buf[q->head];
    q->head = (q->head + 1) % SIZEOFQUEUE; // Queue makes loops
    q->count--;

    pthread_mutex_unlock(&q->mut);
    pthread_cond_signal(&q->notFull);

    // Saving the time that the job got removed from the queue and will be handed to a consumer
    // if (wf->arg != NULL){

    //     struct timeval current_time;
    //     gettimeofday(&current_time, NULL);
    //     end_time[(((((jobArg *)(wf->arg))->producer_id)-1)*JOBS_PER_PRODUCER)+(((jobArg *)(wf->arg))->job_id)-1] = (current_time.tv_sec * 1e6) + current_time.tv_usec;
    // } 

    return wf;

}

void *func(void *arg){
    jobArg *func_args = (jobArg *)arg;

    // printf("A consumer executes the following job: Producer %d, job #%d.\n", func_args->producer_id, func_args->job_id);

    double angle;
    angle = func_args->start_angle;

    for (int i=0; i <10; i++){
        angle = angle + i*SIZEOFQUEUE;
    }

    angle = sin(angle);

    // Storing the results in a global array, using global variables!!!

    // pthread_mutex_lock(&store_mut);

    // results[store_cnt] = angle;
    // store_cnt++;

    // pthread_mutex_unlock(&store_mut);

    return NULL;

}

void *producer(void *arg){
    producerArg *parg = (producerArg *)arg;
    queue *q = parg->q;
    // int pid = parg->producer_id;

    for (int i=0; i<JOBS_PER_PRODUCER; i++){

        workfunction *job = malloc(sizeof(workfunction));
        jobArg *jarg = malloc(sizeof(jobArg));

        if (job == NULL){
            printf("Malloc error on job #%d\n", i);
            exit(1);
        }

        if (jarg == NULL){
            printf("Malloc error on job argument (arg) #%d\n",i);
            exit(1);
        }

        jarg->producer_id = parg->producer_id;
        jarg->job_id = i + 1;
        jarg->start_angle = 0.55 + i*(parg->producer_id)*SIZEOFQUEUE;

        job->work = func;
        job->arg = jarg;

        queueAdd(q, job);

    }

    return NULL;

}

void *consumer(void *arg){
    queue *q = (queue *)arg;

    while(1){

        workfunction *wf = queueDel(q);

        if (wf->work == NULL){  

            // printf("No more work left - One consumer abandoning . . .\n");
            free(wf->arg);
            free(wf);
            break;
        }
            
        wf->work(wf->arg);
        free(wf->arg); // Free the space of this specific workfunction struct and it's arguments 
        free(wf);      // because they were malloced in the producer function.
        
    }

    return NULL;

}
