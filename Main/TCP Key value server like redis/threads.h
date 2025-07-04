#pragma once

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>
#include <vector>
#include <deque>

struct Work{
    void (*f)(void *) = NULL;
    void *arg = NULL;
};

struct ThreadPool{
    std::vector<pthread_t> threads;
    std::deque<Work> queue;
    pthread_mutex_t mu;
    pthread_cond_t non_empty;
};

void thread_pool_init(ThreadPool *tp,size_t num_threads);
void thread_pool_queue(ThreadPool *tp,void (* f)(void *),void *args);