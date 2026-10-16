#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdatomic.h>
#include <time.h>
#include "restaurant.h"
#include "restaurant_log.h"

SimLog *simLog = NULL;
atomic_int sim_done = 0;
int totalTables = 5;



void *customer_thread(void *arg) {

    Customer *customer = arg;

    char message[50];

    customer->state = STATE_ARRIVING;
    snprintf(message, sizeof(message), "Customer %d arrived", customer->id);
    log_customer_state(simLog, customer->id, "ARRIVING", customer->table_id, message);

    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 100 * 1000000L;
    nanosleep(&ts, NULL);

    customer->state = STATE_QUEUED;
    snprintf(message, sizeof(message), "Customer %d waiting for table", customer->id);
    log_customer_state(simLog, customer->id, "QUEUED", customer->table_id, message);

    nanosleep(&ts, NULL);

    customer->table_id = customer->id % totalTables;
    customer->state = STATE_SEATED;
    snprintf(message, sizeof(message), "Customer %d seated", customer->id);
    log_customer_state(simLog, customer->id, "SEATED", customer->table_id, message);

    nanosleep(&ts, NULL);

    customer->state = STATE_ORDERING;
    snprintf(message, sizeof(message), "Customer %d placing order", customer->id);
    log_customer_state(simLog, customer->id, "ORDERING", customer->table_id, message);

    customer->state = STATE_WAITING_FOR_MEAL;
    snprintf(message, sizeof(message), "Customer %d waiting for meal", customer->id);
    log_customer_state(simLog, customer->id, "WAITING_FOR_MEAL", customer->table_id, message);

    ts.tv_nsec = 300 * 1000000L;
    nanosleep(&ts, NULL);

    customer->state = STATE_EATING;
    snprintf(message, sizeof(message), "Customer %d eating", customer->id);
    log_customer_state(simLog, customer->id, "EATING", customer->table_id, message);

    ts.tv_nsec = 500 * 1000000L;
    nanosleep(&ts, NULL);

    customer->state = STATE_LEAVING;
    snprintf(message, sizeof(message), "Customer %d leaving", customer->id);
    log_customer_state(simLog, customer->id, "LEAVING", customer->table_id, message);

    
    return NULL;
}

void *waiter_thread(void *arg) {

    Waiter *waiter = arg;

    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 100 * 1000000L;

    while (!sim_done) {

        nanosleep(&ts, NULL);

    }

    (void)waiter;

    return NULL;

}

void *chef_thread(void *arg) {

    int *chef_id = arg;

    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 100 * 1000000L;

    while (!sim_done) {

        nanosleep(&ts, NULL);

    }

    (void)chef_id;

    return NULL;

}

int main(int argc, char *argv[]) {

    int numCustomers = TOTAL_CUSTOMERS;
    int numTables = 5;
    int numWaiters = NUM_WAITERS;
    int numChefs = NUM_CHEFS;
 

    if (argc != 1 && argc != 5) {
        fprintf(stderr, "Invalid number of arguments\n");
        return 1;
    }


    if (argc == 5) {
        numCustomers = atoi(argv[1]);
        numTables = atoi(argv[2]);
        numWaiters = atoi(argv[3]);
        numChefs = atoi(argv[4]);

        if (numCustomers < 1 || numCustomers > TOTAL_CUSTOMERS) {
            fprintf(stderr, "Invalid number of customers\n");
            return 1;
        }

        if (numTables < 1) {
            fprintf(stderr, "Invalid number of tables\n");
            return 1;
        }

        if (numWaiters < 1 || numWaiters > NUM_WAITERS) {
            fprintf(stderr, "Invalid number of waiters\n");
            return 1;
        }

        if (numChefs < 1 || numChefs > NUM_CHEFS) {
            fprintf(stderr, "Invalid number of chefs\n");
            return 1;
        }
    }

    totalTables = numTables;

    Customer *customers = malloc(numCustomers * sizeof(Customer));
    pthread_t *customerThreads = malloc(numCustomers * sizeof(pthread_t));

    Waiter *waiters = malloc(numWaiters * sizeof(Waiter));
    pthread_t *waiterThreads = malloc(numWaiters * sizeof(pthread_t));

    pthread_t *chefThreads = malloc(numChefs * sizeof(pthread_t));
    int *chefIDs = malloc(numChefs * sizeof(int));

    if (customers == NULL || customerThreads == NULL || waiters == NULL || 
        waiterThreads == NULL || chefThreads == NULL || chefIDs == NULL) {

            fprintf(stderr, "Memory allocation failed\n");

            free(customers);
            free(customerThreads);
            free(waiters);
            free(waiterThreads);
            free(chefThreads);
            free(chefIDs);

            return 1;
        }

        for (int i = 0; i < numCustomers; i++) {
            customers[i].id = i;
            customers[i].state = STATE_ARRIVING;
            customers[i].table_id = -1;
            customers[i].waiter_id = -1;
            customers[i].meal_ready = 0;
        }

        for (int i = 0; i < numWaiters; i++) {
            waiters[i].id = i;
            waiters[i].current_customer_count = 0;
        }

        simLog = log_init("simulation.log", "tuv12869", numCustomers, numTables, 
            numWaiters, numChefs);

        if (simLog == NULL) {
            fprintf(stderr, "Failed to initialize log\n");

            free(customers);
            free(customerThreads);
            free(waiters);
            free(waiterThreads);
            free(chefThreads);
            free(chefIDs);

            return 1;
        }

        for (int i = 0; i < numCustomers; i++) {

            if (pthread_create(&customerThreads[i], NULL, customer_thread,
                &customers[i]) != 0) {
                    fprintf(stderr, "Failed to create customer thread\n");
                    exit(1);
                }
        }

        for (int i = 0; i < numWaiters; i++) {
            if (pthread_create(&waiterThreads[i], NULL, waiter_thread, &waiters[i]) != 0) {
                fprintf(stderr, "Failed to create waiter thread\n");
                exit(1);
            }
        }

        for (int i = 0; i < numChefs; i++) {

            chefIDs[i] = i;

            if (pthread_create(&chefThreads[i], NULL, chef_thread, &chefIDs[i]) != 0) {
                fprintf(stderr, "Failed to create chef thread\n");
                exit(1);
            }
        }

        for (int i = 0; i < numCustomers; i++) {
            pthread_join(customerThreads[i], NULL);
        }

        sim_done = 1;

        for (int i = 0; i < numWaiters; i++) {
            pthread_join(waiterThreads[i], NULL);
        }

        for (int i = 0; i < numChefs; i++) {
            pthread_join(chefThreads[i], NULL);
        }

    log_sim_end(simLog, numCustomers);
    log_destroy(simLog); 

    free(customers);
    free(customerThreads);
    free(waiters);
    free(waiterThreads);
    free(chefThreads);
    free(chefIDs);


    return 0;
}