#ifndef BARRIER_H
#define BARRIER_H
#include <pthread.h>

// a multiple use barrier

class Barrier {
public:
	Barrier(int numThreads);
	~Barrier();
	void barrier();

private:
    pthread_mutex_t mutex;
    pthread_mutex_t shuffle_mutex;
    pthread_cond_t cv;
    pthread_cond_t shuffle_cv;
    int count;
    int numThreads;

    void shuffle(int pid);
};

#endif //BARRIER_H


