//
// Created by rani.toukhy on 24/04/2022.
//

#include <cstdio>
#include "MapReduceFramework.h"
#include "Barrier/Barrier.h"
#include <atomic>
#include <algorithm>

struct ThreadContext {
    int threadID;
    Barrier* barrier;
    const MapReduceClient *client;
    const InputVec *thread_input;
    IntermediateVec *interMediates;
    int total_jobs;
    std::atomic<uint32_t> *job_counter;
};
bool sortbykey(const IntermediatePair& p1,
               const IntermediatePair& p2)
{
    return p1.first < p2.first;
}
void* mapWithBarrier(void* arg){

    /* map */
    ThreadContext* tc = (ThreadContext*) arg;

    printf("Before barriers: %d\n", tc->threadID);
    uint32_t old_value = (*(tc->job_counter))++;
    while (old_value < tc->total_jobs){
        auto k1 = tc->thread_input->at(old_value).first;
        auto v1 = tc->thread_input->at(old_value).second;
        tc->client->map(k1, v1, tc);
        old_value = (*(tc->job_counter))++;

        tc->barrier->barrier();

        /* sort */
        printf("Between barriers: %d\n", tc->threadID);
        std::sort(tc->interMediates->begin(), tc->interMediates->end(), sortbykey);
        tc->barrier->barrier();

        /* shuffle  means rani is dancing all night long */
        if (tc->threadID != 0) {

            // waiting for 0 to call us
        } else {
            // do shuffle
        }

        printf("After barriers: %d\n", tc->threadID);

        return 0;
    }
    void thread_partition(const MapReduceClient &client, const InputVec &inputVec, std::atomic<uint32_t> thread_counter, pthread_t* threads){

    }
    JobHandle
    startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel) {
        pthread_t threads[multiThreadLevel];
        ThreadContext contexts[multiThreadLevel];
        Barrier barrier(multiThreadLevel);
        std::atomic<uint32_t> job_counter;
        for (int i = 0; i < multiThreadLevel; ++i) {
            IntermediateVec intermediateVec;
            contexts[i] = {i,&barrier,&client, &inputVec,&intermediateVec ,static_cast<int>(inputVec.size()),&job_counter};
        }
        for (int i = 0; i < multiThreadLevel; ++i) {
            pthread_create(threads + i, NULL, mapWithBarrier, contexts + i);
        }
        //thread_partition(client, inputVec, thread_counter, threads);

        return nullptr;
    }
    void emit2 (K2* key, V2* value, void* context){
        ThreadContext* tc = (ThreadContext*) context;
        tc->interMediates->push_back(IntermediatePair(key,value));
    }



