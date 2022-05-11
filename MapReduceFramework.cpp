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
#include <bitset>
#include <unistd.h>
#include <memory>

static const uint64_t FIRST31BIT = 0x7fffffff;
static const uint64_t BIT32 = 0x80000000;

/* struct  */
struct JobInfo;

struct ThreadContext {
    int threadID{};
    std::shared_ptr<IntermediateVec> interMediates;
    JobInfo *jobInfo{};
};

bool sortByKey(const IntermediatePair &p1, const IntermediatePair &p2) { return *p1.first < *p2.first; }

bool cmpBy(const K2 *a, const K2 *b) { return *a < *b; }

struct CmpByKey {
    bool operator()(K2 *a, K2 *b) const {
        return *a < *b;
    }
};

static void printErr(const std::string &msg, int ID) {
    fprintf(stderr, "system error: %s, thread number: %d\n", msg.c_str(), ID);
    exit(EXIT_FAILURE);
}

struct JobInfo {
    std::vector<K2 *> keyVec;
    std::vector<std::shared_ptr<ThreadContext>> contextVec;
    std::vector<std::vector<IntermediatePair>> allVecs;
    std::vector<pthread_t> threadsVec;
    std::set<K2 *, CmpByKey> keySet;

    pthread_mutex_t mutexUpdate{};
    pthread_mutex_t mutexMap{};
    pthread_mutex_t mutexEmit2{};
    pthread_mutex_t mutexEmit3{};

    pthread_mutex_t mutexReduce{};
    pthread_mutex_t mutexIncrease{};

    const MapReduceClient *client{};
    InputVec const *thread_input{};

    std::atomic_flag hasWaited = ATOMIC_FLAG_INIT;
    std::atomic_flag hasClosed = ATOMIC_FLAG_INIT;

    std::shared_ptr<Barrier> barrier;
    std::shared_ptr<std::atomic<uint64_t>> status{};
    std::shared_ptr<std::atomic<uint32_t>> numOfIntermediates{};

    OutputVec *outputVec{};
    unsigned long totalJobs{};

};

void *mapReduceJob(void *arg);

void resetStageAndTotalJobs(JobInfo *jobInfo, stage_t stage, unsigned int totalJobs);

stage_t getStage(JobInfo *jobInfo);

void setStage(JobInfo *jobInfo, stage_t stage);

void shufflePhase(JobInfo *jobInfo, ThreadContext *tc);

void mapPhase(JobInfo *jobInfo, ThreadContext *tc);

void sortPhase(JobInfo *jobInfo, ThreadContext *tc);

const std::pair<K1 *, V1 *> *getNextPair(JobInfo *jobInfo);

int getStartedJob(JobInfo *jobInfo);

void increaseStartedJob(JobInfo *jobInfo);

void increaseDoneJobs(JobInfo *jobInfo);

int getDoneJob(JobInfo *jobInfo);

void reducePhase(JobInfo *jobInfo, ThreadContext *tc);

JobHandle
startMapReduceJob(const MapReduceClient &client, const InputVec &inputVec, OutputVec &outputVec, int multiThreadLevel) {

    auto jobInfo = new JobInfo();

    jobInfo->client = &client;
    jobInfo->thread_input = &inputVec;
    jobInfo->outputVec = &outputVec;

    jobInfo->status = std::make_shared<std::atomic<uint64_t>>(0);
    jobInfo->numOfIntermediates = std::make_shared<std::atomic<uint32_t>>(0);
    jobInfo->barrier = std::make_shared<Barrier>(multiThreadLevel);

    jobInfo->threadsVec.resize(multiThreadLevel);
    setStage(jobInfo, UNDEFINED_STAGE);

    jobInfo->hasWaited.test_and_set(static_cast<std::memory_order>(false));
    jobInfo->hasClosed.test_and_set(static_cast<std::memory_order>(false));

    jobInfo->mutexUpdate = PTHREAD_MUTEX_INITIALIZER;
    jobInfo->mutexMap = PTHREAD_MUTEX_INITIALIZER;
    jobInfo->mutexEmit2 = PTHREAD_MUTEX_INITIALIZER;
    jobInfo->mutexEmit3 = PTHREAD_MUTEX_INITIALIZER;
    jobInfo->mutexReduce = PTHREAD_MUTEX_INITIALIZER;
    jobInfo->mutexIncrease = PTHREAD_MUTEX_INITIALIZER;
    jobInfo->totalJobs = inputVec.size();
    for (int i = 0; i < multiThreadLevel; ++i) {
        auto intermediateVec = std::make_shared<IntermediateVec>();
        auto cont = std::make_shared<ThreadContext>();
        cont->threadID = i;
        cont->interMediates = intermediateVec;
        cont->jobInfo = jobInfo;
        jobInfo->contextVec.push_back(cont);
    }

    for (int i = 0; i < multiThreadLevel; ++i) {
        if (pthread_create(&jobInfo->threadsVec[i], NULL, mapReduceJob, jobInfo->contextVec[i].get())) {
            printErr("error on pthread crate", -1);
        }
    }

    return jobInfo;
}

void *mapReduceJob(void *arg) {
    auto *tc = (ThreadContext *) arg;
    auto jobInfo = tc->jobInfo;
    std::cout << tc->threadID << std::endl;

    setStage(jobInfo, MAP_STAGE);
    mapPhase(jobInfo, tc);
    sortPhase(jobInfo, tc);
    jobInfo->barrier->barrier();
    if (tc->threadID != 0) {
        jobInfo->barrier->barrier();
    } else {
        resetStageAndTotalJobs(jobInfo, SHUFFLE_STAGE, jobInfo->numOfIntermediates->load());
        jobInfo->keyVec = std::vector<K2 *>(jobInfo->keySet.begin(), jobInfo->keySet.end());
        std::sort(jobInfo->keyVec.begin(), jobInfo->keyVec.end(), cmpBy);
        std::reverse(jobInfo->keyVec.begin(), jobInfo->keyVec.end());
        shufflePhase(jobInfo, tc);
        resetStageAndTotalJobs(jobInfo, REDUCE_STAGE, jobInfo->allVecs.size());
        jobInfo->barrier->barrier();
    }
    reducePhase(jobInfo, tc);
    jobInfo->barrier->barrier();
    printf("");
    return nullptr;
}

void reducePhase(JobInfo *jobInfo, ThreadContext *tc) {
    bool flag = true;
    std::vector<IntermediatePair> *pairs;
    while (flag) {
        if (pthread_mutex_lock(&tc->jobInfo->mutexReduce) != 0) {
            printErr("error on pthread_mutex_lock [reducePhase]", tc->threadID);
        }
        flag = getStartedJob(jobInfo) < (int) jobInfo->allVecs.size();
        if (flag) {
            pairs = &jobInfo->allVecs.at(getStartedJob(jobInfo));
            increaseStartedJob(jobInfo);
        }
        if (pthread_mutex_unlock(&tc->jobInfo->mutexReduce) != 0) {
            printErr("error on pthread_mutex_unlock [reducePhase]", tc->threadID);
        }
        if (flag) {
            jobInfo->client->reduce(pairs, tc);
            increaseDoneJobs(jobInfo);
        }
    }
}

void mapPhase(JobInfo *jobInfo, ThreadContext *tc) {
    bool flag = true;
    while (flag) {
        auto nextPair = getNextPair(jobInfo);
        if (nextPair != nullptr) {
            jobInfo->client->map(nextPair->first, nextPair->second, tc);
            increaseDoneJobs(jobInfo);
        } else {
            flag = false;
        }

    }
}

const std::pair<K1 *, V1 *> *getNextPair(JobInfo *jobInfo) {
    const std::pair<K1 *, V1 *> *nextPair = nullptr;

    if (pthread_mutex_lock(&jobInfo->mutexMap) != 0) {
        printErr("error on pthread_mutex_lock [getStage]", -1);
    }
    int startedJob = getStartedJob(jobInfo);
    increaseStartedJob(jobInfo);
    if (startedJob < (int) jobInfo->thread_input->size()) {
        nextPair = &(jobInfo->thread_input->at(startedJob));
    }
    if (pthread_mutex_unlock(&jobInfo->mutexMap) != 0) {
        printErr("error on pthread_mutex_unlock [getNextPair]", -1);
    }
    return nextPair;
}

void sortPhase(JobInfo *jobInfo, ThreadContext *tc) {
    std::sort(tc->interMediates->begin(), tc->interMediates->end(), sortByKey);
}

void shufflePhase(JobInfo *jobInfo, ThreadContext *tc) {
    std::vector<std::vector<IntermediatePair>> allVec;
    CmpByKey cmpByKey;
    for (auto key: jobInfo->keyVec) {
        auto vec = std::make_shared<IntermediateVec>();
        for (const auto &context: jobInfo->contextVec) {
            while ((!(context->interMediates->empty())) &&
                   (!cmpByKey(context->interMediates->back().first, key)) &&
                   (!cmpByKey(key, context->interMediates->back().first))
                    ) {
                vec->push_back(context->interMediates->back());
                context->interMediates->pop_back();
                increaseDoneJobs(jobInfo);
            }
        }
        allVec.push_back(*vec);
    }
    jobInfo->allVecs = allVec;
}

void emit2(K2 *key, V2 *value, void *context) {
    auto *tc = (ThreadContext *) context;
    if (pthread_mutex_lock(&tc->jobInfo->mutexEmit2) != 0) {
        printErr("error on pthread_mutex_lock [emit2]", tc->threadID);
    }
    tc->interMediates->push_back(IntermediatePair(key, value));
    tc->jobInfo->keySet.insert(key);
    tc->jobInfo->numOfIntermediates->operator++();
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
        tc->jobInfo->outputVec->push_back(OutputPair(key, value));
    if (pthread_mutex_unlock(&jobInfo->mutexEmit3) != 0) {
        printErr("error on pthread_mutex_unlock [emit3]", tc->threadID);
    }


}

int getDoneJob(JobInfo *jobInfo) {

    int ret = static_cast<int> ((jobInfo->status->load() >> 31) & FIRST31BIT);

    return ret;
}

void increaseDoneJobs(JobInfo *jobInfo) {
    if (pthread_mutex_lock(&jobInfo->mutexUpdate) != 0) {
        printErr("error on pthread_mutex_lock [getDoneJob]", -1);
    }
    jobInfo->status->fetch_add(BIT32);
    if (pthread_mutex_unlock(&jobInfo->mutexUpdate) != 0) {
        printErr("error on pthread_mutex_unlock [getDoneJob]", -1);
    }
}

void increaseStartedJob(JobInfo *jobInfo) {
    if (pthread_mutex_lock(&jobInfo->mutexUpdate) != 0) {
        printErr("error on pthread_mutex_lock [increaseStartedJob]", -1);
    }
    jobInfo->status->operator++();
    if (pthread_mutex_unlock(&jobInfo->mutexUpdate) != 0) {
        printErr("error on pthread_mutex_unlock [increaseStartedJob]", -1);
    }
}

int getStartedJob(JobInfo *jobInfo) {
    if (pthread_mutex_lock(&jobInfo->mutexIncrease) != 0) {
        printErr("error on pthread_mutex_lock [getStartedJob]", -1);
    }
    uint64_t startedJob = jobInfo->status->load() & FIRST31BIT;
    if (pthread_mutex_unlock(&jobInfo->mutexIncrease) != 0) {
        printErr("error on pthread_mutex_unlock [getStartedJob]", -1);
    }
    return static_cast<int>(startedJob);
}

stage_t getStage(JobInfo *jobInfo) {
    return (stage_t) ((uint64_t) jobInfo->status->load() >> 62);
}

void resetStageAndTotalJobs(JobInfo *jobInfo, stage_t stage, unsigned int totalJobs) {
    if (pthread_mutex_lock(&jobInfo->mutexUpdate) != 0) {
        printErr("error on pthread_mutex_lock [getStage]", -1);
    }
    *jobInfo->status = (uint64_t) stage << 62;
    jobInfo->totalJobs = totalJobs;
    if (pthread_mutex_unlock(&jobInfo->mutexUpdate) != 0) {
        printErr("error on pthread_mutex_unlock [getStage]", -1);
    }
    return jobInfo;
}

void setStage(JobInfo *jobInfo, stage_t stage) {
    if (pthread_mutex_lock(&jobInfo->mutexUpdate) != 0) {
        printErr("error on pthread_mutex_lock [getStage]", -1);
    }
    *jobInfo->status = jobInfo->status->load() | ((uint64_t) stage << 62);
    if (pthread_mutex_unlock(&jobInfo->mutexUpdate) != 0) {
        printErr("error on pthread_mutex_unlock [getStage]", -1);
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
    if (!jobInfo) {
        state->stage = UNDEFINED_STAGE;
        state->percentage = 0;
        return;
    }
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
    if (jobInfo->hasClosed.test_and_set(static_cast<std::memory_order>(true))) {
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


//
//stage_t s = getStage(jobInfo);
//setStageAndReset(jobInfo, MAP_STAGE);
//s = getStage(jobInfo);
//setStageAndReset(jobInfo, SHUFFLE_STAGE);
//s = getStage(jobInfo);
//setStageAndReset(jobInfo, REDUCE_STAGE);
//s = getStage(jobInfo);