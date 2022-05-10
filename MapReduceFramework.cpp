//
// Created by rani.toukhy on 24/04/2022.
//

#include <cstdio>
#include "MapReduceFramework.h"
#include "Barrier.h"
#include <atomic>
#include <algorithm>
#include <semaphore.h>
#include <set>
#include <iostream>
#include <bitset>
#include <unistd.h>
#include <memory>

/* struct  */
struct JobInfo;

struct ThreadContext {
    int threadID;
    IntermediateVec *interMediates;
    JobInfo *jobInfo;
};
struct JobInfo {
    std::vector<K2 *> keyVec;
    std::vector<std::shared_ptr<ThreadContext>> contextVec;
    std::vector<std::vector<IntermediatePair>> allVecs;
    std::vector<pthread_t> threadsVec;

    pthread_mutex_t mutexEmit3;
    pthread_mutex_t mutexEmit2;
    pthread_mutex_t mutexTotalJobs;
    pthread_mutex_t mutexUpdate;
    pthread_mutex_t mutexMap;
    pthread_mutex_t mutexAtomic;
    pthread_mutex_t mutexIncrease;


    const MapReduceClient *client;
    InputVec const *thread_input{};

    std::atomic_flag hasWaited = ATOMIC_FLAG_INIT;
    std::atomic_flag hasClosed = ATOMIC_FLAG_INIT;

    std::shared_ptr<Barrier> barrier;
    std::shared_ptr<std::atomic<uint64_t>> status{};
    std::shared_ptr<std::atomic<uint32_t>> numOfIntermediates{};

    OutputVec *outputVec;
    int totalJobs;
};

/* util function*/
int getDoneJobs(JobInfo *jobInfo);

void reduce(ThreadContext *tc, JobInfo *jobInfo);

void shuffle_phase(const ThreadContext *tc, JobInfo *jobInfo);

void map(ThreadContext *tc, JobInfo *jobInfo);

static void print_start(std::string msg, int ID) {
//    printf("<Before: %s, Thread ID: %d>\n", msg.c_str(), ID);
}

static void print_end(std::string msg, int ID) {
//    printf("</After: %s, Thread ID: %d>\n", msg.c_str(), ID);
}

static void printErr(std::string msg, int ID) {
    fprintf(stderr, "system error: %s, thread number: %d\n", msg.c_str(), ID);
    exit(EXIT_FAILURE);
}

bool sortByKey(const IntermediatePair &p1, const IntermediatePair &p2) { return *p1.first < *p2.first; }

bool sortKeys(const K2 *k2_1, const K2 *k2_2) { return *k2_1 < *k2_2; }

int getAndIncStartedJobs(JobInfo *jobInfo) {

    if (pthread_mutex_lock(&jobInfo->mutexAtomic) != 0) {
        printErr("error on pthread_mutex_lock [getAndIncStartedJobs]", -1);
    }
    uint64_t temp = ((*(jobInfo->status))++) & ((uint64_t(1) << 31) - 1);
    if (pthread_mutex_unlock(&jobInfo->mutexAtomic) != 0) {
        printErr("error on pthread_mutex_unlock [getAndIncStartedJobs]", -1);
    }

//    printf("the number u got from get and increase is: %d\n", (int)temp);
    return temp;
}

int getDoneJobs(JobInfo *jobInfo) {
    uint64_t temp = ((*jobInfo->status).load() >> 31) & ((uint64_t(1) << 31) - 1);
    return (int) temp;
}

void increaseDoneJobs(JobInfo *jobInfo) {
    if (pthread_mutex_lock(&jobInfo->mutexIncrease) != 0) {
        printErr("error on pthread_mutex_lock [increaseDoneJobs]", -1);
    }
    *jobInfo->status = *jobInfo->status + power_31;
    if (pthread_mutex_unlock(&jobInfo->mutexIncrease) != 0) {
        printErr("error on pthread_mutex_unlock [increaseDoneJobs]", -1);
    }
}

IntermediateVec keyFromAllThreads(K2 *key, JobInfo *jobInfo) {
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

    jobInfo->totalJobs = (int) jobInfo->numOfIntermediates->load();

    while (!jobInfo->keyVec.empty()) {
        auto key = jobInfo->keyVec.back();
        jobInfo->keyVec.pop_back();
        auto res = keyFromAllThreads(key, jobInfo);
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
    map(tc, jobInfo);

    /* sort */
    print_start("sort", tc->threadID);
    std::sort(tc->interMediates->begin(), tc->interMediates->end(), sortByKey);
    print_end("sort", tc->threadID);

    jobInfo->barrier->barrier();

//  shuffle
    shuffle_phase(tc, jobInfo);
    //reduce

    print_start("reduce", tc->threadID);
    reduce(tc, jobInfo);
    print_end("reduce", tc->threadID);
    return nullptr;
}

void map(ThreadContext *tc, JobInfo *jobInfo) {
    int old_value = getAndIncStartedJobs(jobInfo);
    auto total_jobs_to_run = (int) jobInfo->thread_input->size();
    while (old_value < total_jobs_to_run) {
        if (pthread_mutex_lock(&jobInfo->mutexMap) != 0) {
            printErr("error on pthread_mutex_lock [map]", -1);
        }
        K1 *k1 = jobInfo->thread_input->at(old_value).first;
        V1 *v1 = jobInfo->thread_input->at(old_value).second;
        printf("");
        if (pthread_mutex_unlock(&jobInfo->mutexMap) != 0) {
            printErr("error on pthread_mutex_unlock [map]", -1);
        }
        print_start("map", tc->threadID);

        jobInfo->client->map(k1, v1, tc);
        increaseDoneJobs(jobInfo);
        print_end("map", tc->threadID);
        old_value = getAndIncStartedJobs(jobInfo);
    }
}
//}
//void map(ThreadContext *tc, JobInfo *jobInfo) {
//    int old_value = getAndIncStartedJobs(jobInfo);
//    auto total_jobs_to_run = (int) jobInfo->thread_input->size();
//    auto vec_to_run = jobInfo->thread_input;
//    while (old_value < total_jobs_to_run) {
//        if (pthread_mutex_lock(&jobInfo->mutexMap) != 0) {
//            printErr("error on pthread_mutex_lock [map]", -1);
//        }
//        printf("the next number is: %d\n", old_value);
//        K1 *k1 = jobInfo->thread_input->at(old_value).first;
//        V1 *v1 = jobInfo->thread_input->at(old_value).second;
//        printf("");
//        if (pthread_mutex_unlock(&jobInfo->mutexMap) != 0) {
//            printErr("error on pthread_mutex_unlock [map]", -1);
//        }
//        print_start("map", tc->threadID);
//
//        jobInfo->client->map(k1, v1, tc);
//        increaseDoneJobs(jobInfo);
//        print_end("map", tc->threadID);
//        old_value = getAndIncStartedJobs(jobInfo);
//    }
//}

void shuffle_phase(const ThreadContext *tc, JobInfo *jobInfo) {
    if (tc->threadID != 0) {
        jobInfo->barrier->barrier();
    } else {
        std::vector<std::vector<IntermediatePair>> allVec;
        std::sort(jobInfo->keyVec.begin(), jobInfo->keyVec.end(), sortKeys);
        jobInfo->totalJobs = (int) jobInfo->numOfIntermediates->load();
        auto totalKey = jobInfo->keyVec.size();
        bool flag = true;
        while (flag) {
            flag = jobInfo->keyVec.empty();
            if (flag) {
                auto next = jobInfo->keyVec.back();
                jobInfo->keyVec.pop_back();

                for (auto cont: jobInfo->contextVec) {

                }
            }
        }
        print_start("shuffle_thread_0", tc->threadID);
        *(jobInfo->status) = ((uint64_t) SHUFFLE_STAGE << 62);
        shuffle(jobInfo);
        print_end("shuffle_thread_0", tc->threadID);

        if (pthread_mutex_lock(&jobInfo->mutexUpdate) != 0) {
            printErr("error on pthread_mutex_lock [mapWithBarrier]", -1);
        }
        *jobInfo->status = (uint64_t) REDUCE_STAGE << 62;
        int stage = (int) ((*jobInfo->status).load() >> 62);
        printf("stage: %d", stage);
        jobInfo->totalJobs = (int) jobInfo->allVecs.size();
        jobInfo->barrier->barrier();
        if (pthread_mutex_unlock(&jobInfo->mutexUpdate) != 0) {
            printErr("error on pthread_mutex_unlock [mapWithBarrier]", -1);
        }
    }
}

void reduce(ThreadContext *tc, JobInfo *jobInfo) {
    int old_value = getAndIncStartedJobs(jobInfo);
    int don = getDoneJobs(jobInfo);
//    while (old_value < jobInfo->totalJobs ) {
////        printf("this is old value: %d and this is the total job %d\n", old_value, jobInfo->totalJobs);
//
////        printf("this is done jobs: %d\n", don);
//        if (pthread_mutex_lock(&tc->jobInfo->mutexTotalJobs) != 0) {
//            printErr("error on pthread_mutex_lock [reduce]", tc->threadID);
//        }
//        auto curr_vec = jobInfo->allVecs.at(old_value);
//        if (pthread_mutex_unlock(&tc->jobInfo->mutexTotalJobs) != 0) {
//            printErr("error on pthread_mutex_lock [reduce]", tc->threadID);
//        }
//        jobInfo->client->reduce(&curr_vec, tc);
//        if (pthread_mutex_lock(&jobInfo->mutexUpdate) != 0) {
//            printErr("error on pthread_mutex_lock [mapWithBarrier]", -1);
//        }
//        increaseDoneJobs(jobInfo);
//        don = getDoneJobs(jobInfo);
//        old_value = getAndIncStartedJobs(jobInfo);
//        printf("old :%d, don:%d\n", old_value, don);
//        if (pthread_mutex_unlock(&jobInfo->mutexUpdate) != 0) {
//            printErr("error on pthread_mutex_unlock [mapWithBarrier]", -1);
//        }
    bool flag = true;
    while (flag) {
        if (pthread_mutex_lock(&tc->jobInfo->mutexTotalJobs) != 0) {
            printErr("error on pthread_mutex_lock [reduce]", tc->threadID);
        }
        printf("len of vector: %lu \n", jobInfo->allVecs.size());
        if (jobInfo->allVecs.empty()) {
            flag = false;
            if (pthread_mutex_unlock(&tc->jobInfo->mutexTotalJobs) != 0) {
                printErr("error on pthread_mutex_lock [reduce]", tc->threadID);
            }
        } else {
            auto pairs = &jobInfo->allVecs.back();
            jobInfo->allVecs.pop_back();
            if (pthread_mutex_unlock(&tc->jobInfo->mutexTotalJobs) != 0) {
                printErr("error on pthread_mutex_lock [reduce]", tc->threadID);
            }
            jobInfo->client->reduce(pairs, tc);
            increaseDoneJobs(jobInfo);
        }
    }
    jobInfo->barrier->barrier();
}


JobHandle
startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel) {
    // init Job info
//    multiThreadLevel = 2;
    auto jobInfo = new JobInfo;
    jobInfo->client = &client;
    jobInfo->thread_input = &inputVec;
    jobInfo->outputVec = &outputVec;

    jobInfo->status = std::make_shared<std::atomic<uint64_t>>(0);
    jobInfo->numOfIntermediates = std::make_shared<std::atomic<uint32_t>>(0);
    jobInfo->barrier = std::make_shared<Barrier>(multiThreadLevel);

    jobInfo->threadsVec.resize(multiThreadLevel);
    *(jobInfo->status) = uint64_t(MAP_STAGE) << 62; // check if working

    jobInfo->hasWaited.test_and_set(static_cast<std::memory_order>(false));
    jobInfo->hasClosed.test_and_set(static_cast<std::memory_order>(false));

    jobInfo->mutexEmit3 = PTHREAD_MUTEX_INITIALIZER;
    jobInfo->mutexEmit2 = PTHREAD_MUTEX_INITIALIZER;
    jobInfo->mutexTotalJobs = PTHREAD_MUTEX_INITIALIZER;
    jobInfo->mutexUpdate = PTHREAD_MUTEX_INITIALIZER;
    jobInfo->mutexMap = PTHREAD_MUTEX_INITIALIZER;
    jobInfo->mutexAtomic = PTHREAD_MUTEX_INITIALIZER;
    jobInfo->mutexIncrease = PTHREAD_MUTEX_INITIALIZER;


    jobInfo->totalJobs = (int) inputVec.size();
    for (int i = 0; i < multiThreadLevel; ++i) {
        auto intermediateVec = new IntermediateVec();
        auto cont = std::make_shared<ThreadContext>();

        cont->threadID = i;
        cont->interMediates = intermediateVec;
        cont->jobInfo = jobInfo;
        jobInfo->contextVec.push_back(cont);

    }

//            {i, intermediateVec, jobInfo};
//        auto cont = new ThreadContext{i, intermediateVec, jobInfo};
//        jobInfo->contextVec.push_back(cont);


    for (int i = 0; i < multiThreadLevel; ++i) {
        if (pthread_create(&jobInfo->threadsVec[i], NULL, mapWithBarrier, jobInfo->contextVec[i].get())) {
            printErr("error on pthread crate", -1);
        }
    }
    return jobInfo;
}

void emit2(K2 *key, V2 *value, void *context) {
    auto *tc = (ThreadContext *) context;
    if (pthread_mutex_lock(&tc->jobInfo->mutexEmit2) != 0) {
        printErr("error on pthread_mutex_lock [emit2]", tc->threadID);
    }
    print_start("emit2", tc->threadID);
    tc->interMediates->push_back(IntermediatePair(key, value));
    tc->jobInfo->keyVec.push_back(key);
    tc->jobInfo->numOfIntermediates->operator++();
    print_end("emit2", tc->threadID);
    if (pthread_mutex_unlock(&tc->jobInfo->mutexEmit2) != 0) {
        printErr("error on pthread_mutex_unlock [emit2]", tc->threadID);
    }
}

void emit3(K3 *key, V3 *value, void *context) {
    auto *tc = (ThreadContext *) context;
    JobInfo *jobInfo = tc->jobInfo;
    if (pthread_mutex_lock(&jobInfo->mutexEmit3) != 0) {
        printErr("error on pthread_mutex_lock [emit3]", tc->threadID);
    }
    print_start("emit3 inside mutex", tc->threadID);
//    tc->jobInfo->outputVec->insert(tc->jobInfo->outputVec->cbegin(), OutputPair(key, value));
    tc->jobInfo->outputVec->push_back(OutputPair(key, value));
    print_end("emit3 exiting mutex", tc->threadID);
    if (pthread_mutex_unlock(&jobInfo->mutexEmit3) != 0) {
        printErr("error on pthread_mutex_unlock [emit3]", tc->threadID);
    }
}

void waitForJob(JobHandle job) {
    auto jobInfo = (JobInfo *) job;
    if (jobInfo->hasWaited.test_and_set(static_cast<std::memory_order>(true))) {
        return;
    }
    for (int i = 0; i < (int) jobInfo->contextVec.size(); ++i) {
        pthread_join(jobInfo->threadsVec[i], NULL);
    }
}

void getJobState(JobHandle job, JobState *state) {
    auto jobInfo = (JobInfo *) job;
    if (pthread_mutex_lock(&jobInfo->mutexUpdate) != 0) {
        printErr("error on pthread_mutex_lock [getJobState]", -1);
    }
    int temp = (int) ((*jobInfo->status).load() >> 62);
    state->stage = (stage_t) temp;
    auto done = (float) getDoneJobs(jobInfo);
    auto total = (float) jobInfo->totalJobs;
    float percentage = done / total;
    float true_per = percentage * 100;
    state->percentage = true_per;
    if (pthread_mutex_unlock(&jobInfo->mutexUpdate) != 0) {
        printErr("error on pthread_mutex_unlock [getJobState]", -1);
    }
}

void closeJobHandle(JobHandle job) {
    auto jobInfo = (JobInfo *) job;
    if (jobInfo->hasWaited.test_and_set(static_cast<std::memory_order>(true))) {
        return;
    }
    waitForJob(job);
    if (pthread_mutex_destroy(&(jobInfo->mutexEmit2)) != 0) {
        printErr("error on pthread_mutex_destroy (mutexEmit2) [closeJobHandle]", -1);
    }
    if (pthread_mutex_destroy(&(jobInfo->mutexEmit3)) != 0) {
        printErr("error on pthread_mutex_destroy (mutexEmit3) [closeJobHandle]", -1);
    }
    delete jobInfo;
}



