//
// Created by rani.toukhy on 24/04/2022.
//

#include <cstdio>
#include "MapReduceFramework.h"
#include "Barrier/Barrier.h"
#include <atomic>
#include <algorithm>
#include <semaphore.h>
#include <set>
#include <iostream>

/* struct  */
struct ThreadContext {
    int threadID;
    Barrier *barrier;
    IntermediateVec *interMediates;
};
struct JobInfo {
    std::set<K2 *> keySet;
    std::atomic<uint64_t> status{};
    sem_t sem{};
    std::vector<ThreadContext> contextVec;
    const MapReduceClient *client;
    const InputVec *thread_input;

} jobInfo;

/* util function*/
bool sortByKey(const IntermediatePair &p1, const IntermediatePair &p2) { return p1.first < p2.first; }

void setJobState(stage_t stage) {
//    auto res = jobInfo.status.load() & (1 << 63);
}

int getAndIncDoneJobs() {
    uint64_t temp = (jobInfo.status++) & ((uint64_t(1) << 32) - 1);
    return (int) temp;
}

int getTotalJobs() {
    uint64_t temp = jobInfo.status.load() & (((uint64_t(1) << 32) - 1) << 31);
    temp = temp >> 31;
    std::cout << temp << std::endl;
    return (int) temp;
}

void *mapWithBarrier(void *arg) {
    /* map */
    ThreadContext *tc = (ThreadContext *) arg;
    printf("start Map thread: %d\n", tc->threadID);
    uint32_t old_value = jobInfo.status++;
    while (old_value < getTotalJobs()) {
        auto k1 = jobInfo.thread_input->at(old_value).first;
        auto v1 = jobInfo.thread_input->at(old_value).second;
        jobInfo.client->map(k1, v1, tc);
        old_value = getAndIncDoneJobs();
    }
    printf("finish Map thread: %d\n", tc->threadID);

    /* sort */
    printf("starting to sort, ThreadId: %d\n", tc->threadID);
    std::sort(tc->interMediates->begin(), tc->interMediates->end(), sortByKey);
    printf("finish sorting, ThreadId: %d\n", tc->threadID);

    tc->barrier->barrier();
    printf("end");
//    return;
//    /* shuffle  means rani is dancing all night long */
//    if (tc->threadID != 0) {
//
//        // waiting for 0 to call us
//    } else {
//        // do shuffle
//    }
//
//    printf("After barriers: %d\n", tc->threadID);
//    return 0;
    return nullptr;
}


JobHandle
startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel) {
    // init
    pthread_t threads[multiThreadLevel];
    ThreadContext contexts[multiThreadLevel];
    Barrier barrier(multiThreadLevel);
    sem_init(&jobInfo.sem, 1, multiThreadLevel);
    jobInfo.status = inputVec.size() << 31;
    for (int i = 0; i < multiThreadLevel; ++i) {
        IntermediateVec intermediateVec;
        contexts[i] = {i, &barrier, &intermediateVec};
        jobInfo.contextVec.push_back(contexts[i]);
    }

    jobInfo.status = jobInfo.status.load() + (uint64_t(MAP_STAGE) << 60); // check if working
    for (int i = 0; i < multiThreadLevel; ++i) {
        pthread_create(threads + i, NULL, mapWithBarrier, contexts + i);
    }
    return &jobInfo;
}

void emit2(K2 *key, V2 *value, void *context) {
    auto *tc = (ThreadContext *) context;
    tc->interMediates->push_back(IntermediatePair(key, value));
    jobInfo.keySet.insert(key);
}

void emit3(K3 *key, V3 *value, void *context) {}

void waitForJob(JobHandle job) {}

void getJobState(JobHandle job, JobState *state) {}

void closeJobHandle(JobHandle job) {}


