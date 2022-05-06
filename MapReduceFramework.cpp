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
#include <bitset>
#include <unistd.h>

/* struct  */
struct ThreadContext {
    int threadID;
    Barrier *barrier;
    IntermediateVec *interMediates;
};
struct JobInfo {
    std::set<K2*> keySet;
    std::atomic<uint64_t> *status;
    std::atomic<uint32_t> *numOfIntermediates;
    std::vector<ThreadContext *> contextVec;
    MapReduceClient const *client;
    InputVec const *thread_input;
    pthread_mutex_t shuffle_mutex;
    pthread_cond_t shuffle_cv;
    std::vector<std::vector<IntermediatePair>> allVecs;
} jobInfo;

/* util function*/
bool sortByKey(const IntermediatePair &p1, const IntermediatePair &p2) { return p1.first < p2.first; }

int getStartJobs() {
    uint64_t temp = (jobInfo.status->load()) & ((uint64_t(1) << 31) - 1);
    return (int) temp;
}

int getAndIncStartedJobs() {
    uint64_t temp = ((*jobInfo.status)++) & ((uint64_t(1) << 31) - 1);
    return (int) temp;
}

int getDoneJobs() {
    uint64_t temp = (*jobInfo.status).load() >> 31 & ((uint64_t(1) << 31) - 1);

    return (int) temp;
}

void increaseDoneJobs() {
    *jobInfo.status += (1 << 30);
    std::cout << "DONE:" << getDoneJobs() << "\n";
}

IntermediateVec keyFromAllThreads(K2 *key) {
    IntermediateVec vec;
    for (auto context: jobInfo.contextVec) {

        while (!context->interMediates->empty() &&
               !(*key < *context->interMediates->back().first) &&
               !(*context->interMediates->back().first < *key)) {

            vec.push_back(context->interMediates->back());
            std::cout << "rani is a magican" << *(char *) key << std::endl;

            context->interMediates->pop_back();
            increaseDoneJobs();
        }
    }
    return vec;
}


void shuffle() {
//    std::vector<K2 *> keyVec_(jobInfo.keySet.begin(), jobInfo.keySet.end());
    std::vector<K2*> keyVec(jobInfo.keySet.begin(), jobInfo.keySet.end());
    std::vector<std::vector<IntermediatePair>> allVecs;
//    std::copy(jobInfo.keySet.begin(), jobInfo.keySet.end(), keyVec.begin());
    std::sort(keyVec.begin(), keyVec.end());
    std::reverse(keyVec.begin(), keyVec.end());


    while (!keyVec.empty()) {
        auto key = keyVec.back();
        auto res = keyFromAllThreads(key);
        allVecs.push_back(res);
//        allVecs.push_back(keyFromAllThreads(key));
        keyVec.pop_back();
    }
    jobInfo.allVecs = allVecs;

}


void *mapWithBarrier(void *arg) {
    /* map */
    auto *tc = (ThreadContext *) arg;
    std::cout << "start Map thread:\n" << tc->threadID;
    int old_value = getAndIncStartedJobs();
    std::cout << "OLD VALUE:" << old_value << std::endl;
    while (old_value < jobInfo.thread_input->size()) {
        K1 *k1 = jobInfo.thread_input->at(old_value).first;
        V1 *v1 = jobInfo.thread_input->at(old_value).second;
        jobInfo.client->map(k1, v1, tc);
        increaseDoneJobs();
        old_value = getAndIncStartedJobs();
    }
    printf("finish Map thread: %d\n", tc->threadID);

    /* sort */
    printf("starting to sort, ThreadId: %d\n", tc->threadID);
    std::sort(tc->interMediates->begin(), tc->interMediates->end(), sortByKey);
    printf("finish sorting, ThreadId: %d\n", tc->threadID);

    tc->barrier->barrier();
    // mutex

    std::cout << "end" << std::endl;

    if (tc->threadID != 0) {
        tc->barrier->barrier();
    } else {
        *(jobInfo.status) = ((uint64_t) SHUFFLE_STAGE << 62);
        shuffle();
        tc->barrier->barrier();
    }


//
//    if (pthread_mutex_lock(&jobInfo.shuffle_mutex) != 0){
//        fprintf(stderr, "[[Barrier]] error on pthread_mutex_lock");
//        exit(1);
//    }
//    if (tc->threadID != 0) {
//        if (pthread_cond_wait(&jobInfo.shuffle_cv, &jobInfo.shuffle_mutex) != 0){
//            fprintf(stderr, "[[Barrier]] error on pthread_cond_wait");
//            exit(1);
//        }
//    } else {
//        shuffle();
//        if (pthread_cond_broadcast(&jobInfo.shuffle_cv) != 0) {
//            fprintf(stderr, "[[Barrier]] error on pthread_cond_broadcast");
//            exit(1);
//        }
//    }
//    if (pthread_mutex_unlock(&jobInfo.shuffle_mutex) != 0) {
//        fprintf(stderr, "[[Barrier]] error on pthread_mutex_unlock");
//        exit(1);
//    }
    return nullptr;
}


JobHandle
startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel) {
    // init Job info
    jobInfo.client = &client;
    jobInfo.status = new std::atomic<uint64_t>(0);
    jobInfo.numOfIntermediates = new std::atomic<uint32_t>(0);
    std::vector<pthread_t> threads;
    threads.resize(multiThreadLevel);
    std::vector<ThreadContext *> contexts;

    auto *barrier = new Barrier(multiThreadLevel);

    for (int i = 0; i < multiThreadLevel; ++i) {
        auto intermediateVec = new IntermediateVec();
        auto cont = new ThreadContext{i, barrier, intermediateVec};
        contexts.push_back(cont);
        jobInfo.contextVec.push_back(cont);
    }
    std::atomic<uint32_t> status_two(0);
    jobInfo.numOfIntermediates = &status_two;


    jobInfo.thread_input = &inputVec;
    *jobInfo.status = (*jobInfo.status).load() + (uint64_t(MAP_STAGE) << 62); // check if working
    for (int i = 0; i < multiThreadLevel; ++i) {
        printf("thread number :%d \n", i);
        pthread_create(&threads[i], NULL, mapWithBarrier, jobInfo.contextVec[i]);
    }
    return &jobInfo;
}

void emit2(K2 *key, V2 *value, void *context) {
    std::cout << "Is Calling emit2" << std::endl;
    auto *tc = (ThreadContext *) context;
    tc->interMediates->push_back(IntermediatePair(key, value));
    jobInfo.keySet.insert(key);
    jobInfo.numOfIntermediates++;
}

void emit3(K3 *key, V3 *value, void *context) {}

void waitForJob(JobHandle job) {}

void getJobState(JobHandle job, JobState *state) {
    auto jobInfo_ = (JobInfo *) job;
    int temp = (int) ((*jobInfo_->status).load() >> 62);
    state->stage = (stage_t) temp;
    std::cout << "this is the amount of done jobs " << getDoneJobs() << std::endl;
    std::cout << "this is the amount of total jobs " << jobInfo_->thread_input->size() << std::endl;
    state->percentage = ((float) getDoneJobs() / (float) jobInfo_->thread_input->size()) * 100;
}


void closeJobHandle(JobHandle job) {}


