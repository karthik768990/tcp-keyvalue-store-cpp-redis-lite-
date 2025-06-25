#include <assert.h>
#include "threads.h"

static void *worker(void *args){
    ThreadPool *tp = (ThreadPool *)args;
    while(true){
        pthread_mutex_lock(&tp->mu);
        //wait for a condition which here is a non empty queue
        while(tp->queue.empty()){
            pthread_cond_wait(&tp->non_empty,&tp->mu);
        }
        //if it is not empty then we got a new job and wake the workers
        Work w = tp->queue.front();
        tp->queue.pop_front();
        pthread_mutex_unlock(&tp->mu);
        //now do the work which has been revied
        w.f(w.arg);
    }
    return NULL;
}

void thread_pool_init(ThreadPool *tp,size_t num_threads){
    assert(num_threads>0);

    int rv= pthread_mutex_init(&tp->mu,NULL);
    assert(rv==0);
    rv = pthread_cond_init(&tp->non_empty,NULL);
    assert(rv==0);

    tp->threads.resize(num_threads);
    for(size_t i=0;i<num_threads;++i){
        int rv = pthread_create(&tp->threads[i],NULL,&worker,tp);
        assert(rv==0);
    }
}

void thread_pool_queue(ThreadPool *tp,void (* f)(void *),void *args){
    pthread_mutex_lock(&tp->mu);
    tp->queue.push_back(Work{f,args});
    pthread_cond_signal(&tp->non_empty);
    pthread_mutex_unlock(&tp->mu);
}