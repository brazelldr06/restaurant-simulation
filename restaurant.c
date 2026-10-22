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
WaitingQueue wq;
OrderQueue oq;
sem_t tables_sem;
int *tableOccupied;
pthread_mutex_t table_mutex;
Customer *customers;
Waiter *waiters;

void release_customer_slot(Customer *customer) {

    pthread_mutex_lock(&table_mutex);
    tableOccupied[customer->table_id] = 0;
    pthread_mutex_unlock(&table_mutex);
    sem_post(&tables_sem);
    Waiter *waiter = &waiters[customer->waiter_id];
    pthread_mutex_lock(&waiter->load_mutex);
    waiter->current_customer_count --;
    log_waiter_load(simLog, waiter->id, customer->id, waiter->current_customer_count,
                    "Waiter released customer");
    pthread_cond_signal(&waiter->load_cond);
    pthread_mutex_unlock(&waiter->load_mutex);
}

void waiting_queue_enqueue(WaitingQueue *q, int id) {
    pthread_mutex_lock(&q->mutex);

    while (q->count == q->capacity) {
        pthread_cond_wait(&q->not_full, &q->mutex);
    }

    q->buf[q->tail] = id;
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);
}

int order_queue_enqueue(OrderQueue *q, Order order) {
    pthread_mutex_lock(&q->mutex);

    while(q->count == q->capacity) {
        pthread_cond_wait(&q->not_full, &q->mutex);
    }

    q->buf[q->tail] = order;
    q->tail = (q->tail + 1) % q->capacity;
    q->count++;
    int queueLength = q->count;
    pthread_cond_signal(&q->not_empty);
    pthread_mutex_unlock(&q->mutex);

    return queueLength;
}

Order order_queue_dequeue(OrderQueue *q) {
    pthread_mutex_lock(&q->mutex);

    while (q->count == 0) {
        if (sim_done) {
            pthread_mutex_unlock(&q->mutex);

            Order finished = {-1,-1,NULL};
            return finished;
        }

        struct timespec dl;
        clock_gettime(CLOCK_REALTIME, &dl);
        dl.tv_nsec += 100000000L;

        if (dl.tv_nsec >= 1000000000L) {
            dl.tv_sec += 1;
            dl.tv_nsec -= 1000000000L;
        }

        pthread_cond_timedwait(&q->not_empty, &q->mutex, &dl);
    }

    Order order = q->buf[q->head];
    q->head = (q->head + 1) % q->capacity;
    q->count--;
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
    return order;
    
}

int waiting_queue_dequeue(WaitingQueue *q) {
    pthread_mutex_lock(&q->mutex);

    while (q->count == 0) {
        if (sim_done) {
            pthread_mutex_unlock(&q->mutex);
            return -1;
        }

        struct timespec dl;
        clock_gettime(CLOCK_REALTIME, &dl);
        dl.tv_nsec += 100000000L;

        if (dl.tv_nsec >= 1000000000L) {
            dl.tv_sec += 1;
            dl.tv_nsec -= 1000000000L;
        }

        pthread_cond_timedwait(&q->not_empty, &q->mutex, &dl);
    }

    int id = q->buf[q->head];
    q->head = (q->head + 1) % q->capacity;
    q->count--;
    pthread_cond_signal(&q->not_full);
    pthread_mutex_unlock(&q->mutex);
    return id;
}

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

    waiting_queue_enqueue(&wq, customer->id);

    for (;;) {

        pthread_mutex_lock(&customer->meal_mutex);
        int seated = (customer->state == STATE_SEATED);
        pthread_mutex_unlock(&customer->meal_mutex);

        if (seated) {
            break;
        }

        nanosleep(&ts, NULL);
    }

    snprintf(message, sizeof(message), "Customer %d seated", customer->id);
    log_customer_state(simLog, customer->id, "SEATED", customer->table_id, message);

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

    release_customer_slot(customer);
    
    return NULL;
}

void *waiter_thread(void *arg) {

    Waiter *waiter = arg;

    for (;;) {
        int cid = waiting_queue_dequeue(&wq);

        if (cid < 0) {
            break;
        }

        sem_wait(&tables_sem);
        pthread_mutex_lock(&table_mutex);
        int table = -1;

        for (int i = 0; i < totalTables; i++) {
            if (tableOccupied[i] == 0) {
                tableOccupied[i] = 1;
                table = i;
                break;
            }
        }

        pthread_mutex_unlock(&table_mutex);
        Customer *customer = &customers[cid];
        pthread_mutex_lock(&waiter->load_mutex);
        waiter->current_customer_count ++;
        log_waiter_load(simLog, waiter->id, cid, waiter->current_customer_count,
                        "waiter seated customer");
        pthread_mutex_unlock(&waiter->load_mutex);
        pthread_mutex_lock(&customer->meal_mutex);
        customer->table_id = table;
        customer->waiter_id = waiter->id;
        customer->state = STATE_SEATED;
        pthread_mutex_unlock(&customer->meal_mutex);

        Order order;
        order.customer_id = cid;
        order.waiter_id = waiter->id;
        order.item = MENU[cid % MENU_SIZE];
        int queueLength = order_queue_enqueue(&oq, order);
        log_order_queued(simLog, waiter->id, cid, order.item, queueLength);
    }

    return NULL;

}

void *chef_thread(void *arg) {

    int *chef_id = arg;

    for (;;) {
        Order order = order_queue_dequeue(&oq);

        if (order.customer_id < 0) {
            break;
        }

        struct timespec ts;
        ts.tv_sec = 0;
        ts.tv_nsec = COOK_MIN_MS * 1000000L;

        nanosleep(&ts, NULL);

        log_meal_ready(simLog, *chef_id, order.customer_id, order.item);
    }

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

    if (sem_init(&tables_sem, 0, numTables) != 0) {
        fprintf(stderr, "Failed to initialize table semaphore\n");
        return 1;
    }

    tableOccupied = malloc(numTables * sizeof(int));

    if (tableOccupied == NULL) {
        fprintf(stderr, "Failed to allocate tables\n");
        sem_destroy(&tables_sem);
        return 1;
    }

    for (int i = 0; i < numTables; i++) {
        tableOccupied[i] = 0;
    }

    pthread_mutex_init(&table_mutex, NULL);


    wq.capacity = numCustomers;
    wq.buf = malloc(wq.capacity * sizeof(int));

    if (wq.buf == NULL) {
        fprintf(stderr, "Failed to allocate waiting queue\n");

        pthread_mutex_destroy(&table_mutex);
        sem_destroy(&tables_sem);
        free(tableOccupied);

        return 1;
    }

    wq.head = 0;
    wq.tail = 0;
    wq.count = 0;

    pthread_mutex_init(&wq.mutex, NULL);
    pthread_cond_init(&wq.not_empty, NULL);
    pthread_cond_init(&wq.not_full, NULL);

    oq.capacity = numCustomers;
    oq.buf = malloc(oq.capacity * sizeof(Order));

    if (oq.buf == NULL) {
        fprintf(stderr, "Failed to allocate order queue\n");

        pthread_mutex_destroy(&wq.mutex);
        pthread_cond_destroy(&wq.not_empty);
        pthread_cond_destroy(&wq.not_full);
        free(wq.buf);
        pthread_mutex_destroy(&table_mutex);
        sem_destroy(&tables_sem);
        free(tableOccupied);

        return 1;

    }

    oq.head = 0;
    oq.tail = 0;
    oq.count = 0;

    pthread_mutex_init(&oq.mutex, NULL);
    pthread_cond_init(&oq.not_empty, NULL);
    pthread_cond_init(&oq.not_full, NULL);

    customers = malloc(numCustomers * sizeof(Customer));
    pthread_t *customerThreads = malloc(numCustomers * sizeof(pthread_t));

    waiters = malloc(numWaiters * sizeof(Waiter));
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
            pthread_mutex_init(&customers[i].meal_mutex, NULL);
            pthread_cond_init(&customers[i].meal_cond, NULL);
        }

        for (int i = 0; i < numWaiters; i++) {
            waiters[i].id = i;
            waiters[i].current_customer_count = 0;

            pthread_mutex_init(&waiters[i].load_mutex, NULL);
            pthread_cond_init(&waiters[i].load_cond, NULL);
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

        for (int i = 0; i < numCustomers; i++) {
            pthread_mutex_destroy(&customers[i].meal_mutex);
            pthread_cond_destroy(&customers[i].meal_cond);
        }

        for (int i = 0; i < numWaiters; i++) {
            pthread_mutex_destroy(&waiters[i].load_mutex);
            pthread_cond_destroy(&waiters[i].load_cond);
        }


    pthread_mutex_destroy(&wq.mutex);
    pthread_cond_destroy(&wq.not_empty);
    pthread_cond_destroy(&wq.not_full);
    pthread_mutex_destroy(&table_mutex);
    sem_destroy(&tables_sem);
    pthread_mutex_destroy(&oq.mutex);
    pthread_cond_destroy(&oq.not_empty);
    pthread_cond_destroy(&oq.not_full);


    log_sim_end(simLog, numCustomers);
    log_destroy(simLog); 

    free(customers);
    free(customerThreads);
    free(waiters);
    free(waiterThreads);
    free(chefThreads);
    free(chefIDs);
    free(wq.buf);
    free(tableOccupied);
    free(oq.buf);

    return 0;
}