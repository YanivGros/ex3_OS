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
    std::vector<K2*> keyVec;
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
int getDoneJobs();

static void print_start(std::string msg, int ID){
    printf("<Before: %s, Thread ID: %d, JobsD: %d>\n", msg.c_str(), ID,getDoneJobs());
    printf("number of jobs done: %d\n", getDoneJobs());
}
static void print_end(std::string msg, int ID){
    printf("</After: %s, Thread ID: %d, JobsD: %d>\n", msg.c_str(), ID,getDoneJobs());
    printf("number of jobs done: %d\n", getDoneJobs());
}

bool sortByKey(const IntermediatePair &p1, const IntermediatePair &p2) { return *p1.first < *p2.first; }
bool sortKeys(const K2 *k2_1, const K2 *k2_2) { return *k2_1 < *k2_2; }

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
            context->interMediates->pop_back();
            increaseDoneJobs();
        }
    }
    return vec;
}


void shuffle() {
    std::vector<std::vector<IntermediatePair>> allVecs;
    std::sort(jobInfo.keyVec.begin(), jobInfo.keyVec.end(), sortKeys);
    while (!jobInfo.keyVec.empty()) {
        auto key = jobInfo.keyVec.back();
        auto res = keyFromAllThreads(key);
        allVecs.push_back(res);
        while (!jobInfo.keyVec.empty() &&
               !(*key < *jobInfo.keyVec.back()) &&
               !(*jobInfo.keyVec.back() < *key)) {
            jobInfo.keyVec.pop_back();
        }

    }

    jobInfo.allVecs = allVecs;

}


void *mapWithBarrier(void *arg) {
    /* map */
    auto *tc = (ThreadContext *) arg;
    int old_value = getAndIncStartedJobs();
    while (old_value < jobInfo.thread_input->size()) {
        K1 *k1 = jobInfo.thread_input->at(old_value).first;
        V1 *v1 = jobInfo.thread_input->at(old_value).second;
        print_start("map", tc->threadID);
        jobInfo.client->map(k1, v1, tc);
        increaseDoneJobs();
        print_end("map", tc->threadID);
        old_value = getAndIncStartedJobs();
    }

    /* sort */
    print_start("sort", tc->threadID);
    std::sort(tc->interMediates->begin(), tc->interMediates->end(), sortByKey);
    print_end("sort", tc->threadID);

    tc->barrier->barrier();
    // mutex

    if (tc->threadID != 0) {
        print_start("shuffle", tc->threadID);
        tc->barrier->barrier();
        print_end("shuffle", tc->threadID);
    } else {
        print_start("shuffle_thread_0", tc->threadID);
        * (jobInfo.status) = ((uint64_t) SHUFFLE_STAGE << 62);
        shuffle();
        tc->barrier->barrier();
        print_end("shuffle_thread_0", tc->threadID);
    }
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
    pthread_t p;

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
        if (pthread_create(&threads[i], NULL, mapWithBarrier, jobInfo.contextVec[i])) {
            fprintf(stderr, "[[Barrier]] error on pthread_cond_wait");
            exit(EXIT_FAILURE) ;
        }
    }
    return &jobInfo;
}

void emit2(K2 *key, V2 *value, void *context) {

    auto *tc = (ThreadContext *) context;
    print_start("emit2", tc->threadID);
    tc->interMediates->push_back(IntermediatePair(key, value));
    jobInfo.keyVec.push_back(key);
    jobInfo.numOfIntermediates++;
    print_end("emit2", tc->threadID);
}

void emit3(K3 *key, V3 *value, void *context) {}

void waitForJob(JobHandle job) {}

void getJobState(JobHandle job, JobState *state) {
    auto jobInfo_ = (JobInfo *) job;
    int temp = (int) ((*jobInfo_->status).load() >> 62);
    state->stage = (stage_t) temp;
//    std::cout << "this is the amount of done jobs " << getDoneJobs() << std::endl;
//    std::cout << "this is the amount of total jobs " << jobInfo_->thread_input->size() << std::endl;
    state->percentage = ((float) getDoneJobs() / (float) jobInfo_->thread_input->size()) * 100;
}


void closeJobHandle(JobHandle job) {}


