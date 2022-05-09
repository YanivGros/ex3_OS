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
struct JobInfo;

struct ThreadContext {
    int threadID;
    IntermediateVec *interMediates;
    JobInfo *jobInfo;
};
struct JobInfo {
    std::vector<K2 *> keyVec;
    std::vector<ThreadContext *> contextVec;
    std::vector<std::vector<IntermediatePair>> allVecs;
    std::vector<pthread_t> threadsVec;
    pthread_mutex_t mutexEmit3;
    pthread_mutex_t mutexEmit2;

    MapReduceClient const *client{};
    InputVec const *thread_input{};

    Barrier *barrier{};
    std::atomic_flag hasWaited = ATOMIC_FLAG_INIT;
    std::atomic<uint64_t> *status{};
    std::atomic<uint32_t> *numOfIntermediates{};
    OutputVec *outputVec;
    int totalJobs;
};

/* util function*/
int getDoneJobs(JobInfo *jobInfo);

static void print_start(std::string msg, int ID){
//    printf("<Before: %s, Thread ID: %d>\n", msg.c_str(), ID);
}
static void print_end(std::string msg, int ID){
//    printf("</After: %s, Thread ID: %d>\n", msg.c_str(), ID);
}

bool sortByKey(const IntermediatePair &p1, const IntermediatePair &p2) { return *p1.first < *p2.first; }
bool sortKeys(const K2 *k2_1, const K2 *k2_2) { return *k2_1 < *k2_2; }

int getStartJobs(uint64_t n) {
    return (int) (n & ((uint64_t(1) << 31) - 1));
}
//int getStartJobs(JobInfo *jobInfo) {
//    uint64_t temp = ((jobInfo)->status->load()) & ((uint64_t(1) << 31) - 1);
//    return (int) temp;
//}

int getAndIncStartedJobs(JobInfo *jobInfo) {
    uint64_t temp = ((*(jobInfo->status))++) & ((uint64_t(1) << 31) - 1);
//    printf("the number u got from get and increase is: %d\n", (int)temp);
    return getStartJobs(temp);
}

int getDoneJobs(JobInfo *jobInfo) {
    uint64_t temp = (*jobInfo->status).load() >> 31 & ((uint64_t(1) << 31) - 1);
    return (int) temp;
}

void increaseDoneJobs(JobInfo *jobInfo) {
    std::bitset<64>j(jobInfo->status->load());
    std::cout << j << "\n";
    *jobInfo->status = *jobInfo->status + (uint64_t(1) << 31);

    std::bitset<64>y(jobInfo->status->load());
    std::cout << y << '\n';
}

IntermediateVec keyFromAllThreads(K2 *key,JobInfo* jobInfo) {
    IntermediateVec vec;
    for (auto context: jobInfo->contextVec) {
        while (!context->interMediates->empty() &&
               !(*key < *context->interMediates->back().first) &&
               !(*context->interMediates->back().first < *key)) {

            vec.push_back(context->interMediates->back());
            context->interMediates->pop_back();
            increaseDoneJobs(jobInfo);
        }
    }
    return vec;
}


void shuffle(JobInfo *jobInfo) {
    std::vector<std::vector<IntermediatePair>> allVecs;
    std::sort(jobInfo->keyVec.begin(), jobInfo->keyVec.end(), sortKeys);
    jobInfo->totalJobs = (int)jobInfo->numOfIntermediates->load();
    while (!jobInfo->keyVec.empty()) {
        auto key = jobInfo->keyVec.back();
        auto res = keyFromAllThreads(key,jobInfo);
        allVecs.push_back(res);
        while (!jobInfo->keyVec.empty() &&
               !(*key < *jobInfo->keyVec.back()) &&
               !(*jobInfo->keyVec.back() < *key)) {
            jobInfo->keyVec.pop_back();
        }

    }

    jobInfo->allVecs = allVecs;

}


void *mapWithBarrier(void *arg) {
    /* map */
    auto *tc = (ThreadContext *) arg;
    JobInfo *jobInfo = tc->jobInfo;
    int old_value = getAndIncStartedJobs(jobInfo);
    while (old_value < jobInfo->thread_input->size()) {
        K1 *k1 = jobInfo->thread_input->at(old_value).first;
        V1 *v1 = jobInfo->thread_input->at(old_value).second;
        print_start("map", tc->threadID);
        jobInfo->client->map(k1, v1, tc);
        increaseDoneJobs(jobInfo);
        print_end("map", tc->threadID);
        old_value = getAndIncStartedJobs(jobInfo);
    }

    /* sort */
    print_start("sort", tc->threadID);
    std::sort(tc->interMediates->begin(), tc->interMediates->end(), sortByKey);
    print_end("sort", tc->threadID);

    jobInfo->barrier->barrier();
    // mutexEmit3
//    usleep(10000);


    if (tc->threadID != 0) {
        print_start("shuffle", tc->threadID);
        jobInfo->barrier->barrier();
        print_end("shuffle", tc->threadID);
    } else {
        print_start("shuffle_thread_0", tc->threadID);
        * (jobInfo->status) = ((uint64_t) SHUFFLE_STAGE << 62);
        shuffle(jobInfo);
        jobInfo->barrier->barrier();
        print_end("shuffle_thread_0", tc->threadID);

        *jobInfo->status = (uint64_t)REDUCE_STAGE << 62;
        jobInfo->totalJobs = (int)jobInfo->allVecs.size();
    }
    getDoneJobs(jobInfo);
    print_start("reduce", tc->threadID);
    jobInfo->totalJobs = (int)jobInfo->allVecs.size();
    old_value = getAndIncStartedJobs(jobInfo);
    while (old_value < jobInfo->allVecs.size()) {
        auto curr_vec = jobInfo->allVecs[old_value];
        jobInfo->client->reduce(&curr_vec, tc);
        old_value = getAndIncStartedJobs(jobInfo);
    }
    print_end("reduce", tc->threadID);
    return nullptr;
}


JobHandle
startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel) {
    // init Job info
    auto jobInfo = new JobInfo;
    jobInfo->client = &client;
    jobInfo->status = new std::atomic<uint64_t>(0);
    jobInfo->numOfIntermediates = new std::atomic<uint32_t>(0);
    jobInfo->threadsVec.resize(multiThreadLevel);
    jobInfo->thread_input = &inputVec;
    *(jobInfo->status) = uint64_t(MAP_STAGE) << 62; // check if working
    jobInfo->outputVec = &outputVec;
    jobInfo->totalJobs = (int) inputVec.size();
    jobInfo->hasWaited.test_and_set(static_cast<std::memory_order>(false));
    auto *barrier = new Barrier(multiThreadLevel);
    jobInfo->barrier = barrier;
    jobInfo->mutexEmit3 = PTHREAD_MUTEX_INITIALIZER;
    jobInfo->mutexEmit2 = PTHREAD_MUTEX_INITIALIZER;

    for (int i = 0; i < multiThreadLevel; ++i) {
        auto intermediateVec = new IntermediateVec();
        auto cont = new ThreadContext{i, intermediateVec, jobInfo};
        jobInfo->contextVec.push_back(cont);
    }


    for (int i = 0; i < multiThreadLevel; ++i) {
//        printf("thread number :%d \n", i);
        if (pthread_create(&jobInfo->threadsVec[i], NULL, mapWithBarrier, jobInfo->contextVec[i])) {
            fprintf(stderr, "[[Barrier]] error on pthread_cond_wait");
            exit(EXIT_FAILURE) ;
        }
    }
    return jobInfo;
}

void emit2(K2 *key, V2 *value, void *context) {
    auto *tc = (ThreadContext *) context;

    print_start("emit2", tc->threadID);
    tc->interMediates->push_back(IntermediatePair(key, value));
    JobInfo *jobInfo = tc->jobInfo;
    if (pthread_mutex_lock(&jobInfo->mutexEmit2) != 0) {
        fprintf(stderr, "[[emit3]] error on pthread_mutex_lock");
        exit(EXIT_FAILURE);
    }
    jobInfo->keyVec.push_back(key);
    jobInfo->numOfIntermediates++;
    if (pthread_mutex_unlock(&jobInfo->mutexEmit2) != 0) {
        fprintf(stderr, "[[emit3]] error on pthread_mutex_unlock");
        exit(EXIT_FAILURE);
    }
    print_end("emit2", tc->threadID);
}

void emit3(K3 *key, V3 *value, void *context) {
    auto *tc = (ThreadContext *) context;
    JobInfo *jobInfo = tc->jobInfo;
    if (pthread_mutex_lock(&jobInfo->mutexEmit3) != 0) {
        fprintf(stderr, "[[emit3]] error on pthread_mutex_lock");
        exit(EXIT_FAILURE);
    }
    print_start("emit3 inside mutex", tc->threadID);
    tc->jobInfo->outputVec->push_back(OutputPair(key, value));
    increaseDoneJobs(jobInfo);
    print_end("emit3 exiting mutex", tc->threadID);
    if (pthread_mutex_unlock(&jobInfo->mutexEmit3) != 0) {
        fprintf(stderr, "[[emit3]] error on pthread_mutex_unlock");
        exit(EXIT_FAILURE);
    }
}

void waitForJob(JobHandle job) {
    auto jobInfo = (JobInfo*) job;
    if (jobInfo->hasWaited.test_and_set(static_cast<std::memory_order>(true))){
        return;
    }

    for (int i = 0; i < jobInfo->contextVec.size(); ++i) {
        pthread_join(jobInfo->threadsVec[i], NULL);
    }
}

void getJobState(JobHandle job, JobState *state) {
    auto jobInfo_ = (JobInfo *) job;
    int temp = (int) ((*jobInfo_->status).load() >> 62);
    state->stage = (stage_t) temp;
    int done = getDoneJobs(jobInfo_);
    int total = jobInfo_->totalJobs;
    state->percentage = ((float) getDoneJobs(jobInfo_) / (float) jobInfo_->totalJobs) * 100;
}

void closeJobHandle(JobHandle job) {
    auto jobInfo_ = (JobInfo *) job;
    if (pthread_mutex_destroy(&jobInfo_->mutexEmit3) != 0) {
        fprintf(stderr, "[[closeJobHandle]] error on pthread_mutex_destroy");
        exit(1);
    }

    delete jobInfo_->barrier;
    delete jobInfo_->status;
    delete jobInfo_->numOfIntermediates;
}


