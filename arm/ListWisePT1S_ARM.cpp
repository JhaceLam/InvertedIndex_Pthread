#include <iostream>
#include <cstdio>
#include <string>
#include <sstream>
#include <fstream>
#include <bitset>
#include "config.h"
#include <sys/time.h>
#include <pthread.h>
#include <semaphore.h>
using namespace std;

const int INF = 1 << 30;
const int NUM_THREADS = 4;

int numQuery = 0;
int query[10];
ifstream queryFile, indexFile;
int numIndex = 0;
int lenIndex[maxNumIndex];
int iindex[maxNumIndex][maxLenIndex];

bitset <32> **bindex;
int *len_bindex;

bitset <32> *rst;
int *result;
int resultCount;
int rstLen;

int max_len_bindex = -1;

pthread_mutex_t amutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t readMutex = PTHREAD_MUTEX_INITIALIZER;

int sect_itv;
int read_itv;

sem_t sem_main;
sem_t sem_calcStart;
sem_t sem_calcEnd;

bool EndReadQuery = false;

class MyTimer {
    private:
        struct timeval start_time, finish_time;
        double timeMS;

    public:
        MyTimer() {
            timeMS = 0.0;
        }

        void start() {
            gettimeofday(&start_time, NULL);
        }

        void finish() {
            gettimeofday(&finish_time, NULL);
        }
        
        double getTime() {
            // ms
            double duration = 0.0;
            long long startUsec, finishUsec;
            startUsec = start_time.tv_sec * 1e6 + start_time.tv_usec;
            finishUsec = finish_time.tv_sec * 1e6 + finish_time.tv_usec;
            duration = (finishUsec - startUsec) / 1e3;
            return duration;
        }
};

typedef struct {
    int threadID;
    int sect;
} threadParm_t;

typedef struct {
    int threadID;
} cthreadParm_t;

void* ReadIndex(void *param) {
    threadParm_t *p = (threadParm_t*) param;
    int t_id = p->threadID;
    int read_start = p->sect;
    int read_end = read_start + read_itv;
    if(t_id == NUM_THREADS - 1) read_end = numIndex;

    // Pthread
    int i;
    for(i = read_start; i < read_end; i++) {
        int bstNum = iindex[i][lenIndex[i] - 1];
        bstNum /= 32;
        bstNum++;
        len_bindex[i] = bstNum;

        pthread_mutex_lock(&readMutex);
        if(bstNum > max_len_bindex) {
            max_len_bindex = bstNum;
        }
        pthread_mutex_unlock(&readMutex);

        bindex[i] = new bitset<32>[bstNum];
        for(int j = 0; j < lenIndex[i]; j++) {
            int t = iindex[i][j];
            bindex[i][t / 32].set(t % 32);
        }
    }

    pthread_exit(NULL);
}

bool TryReadIndex()
{
    if (!indexFile)
    {
        cout << "Error opening file.";
        return false;
    }

    numIndex = 0;
    int num = 0;
    while (indexFile.read(reinterpret_cast<char *>(&num), sizeof(int)))
    {
        lenIndex[numIndex] = num;
        indexFile.read(reinterpret_cast<char *>(iindex[numIndex]), sizeof(int) * num);
        numIndex++;
    }

    len_bindex = new int[numIndex];
    bindex = new bitset<32> *[numIndex];

    // Try Pthread
    pthread_t rthread[NUM_THREADS];
    threadParm_t rthreadParm[NUM_THREADS];
    read_itv = numIndex / NUM_THREADS;
    int readStart = 0;
    for(int i = 0; i < NUM_THREADS; i++) {
        rthreadParm[i].sect = readStart;
        rthreadParm[i].threadID = i;

        pthread_create(&rthread[i], NULL, ReadIndex, (void*)&rthreadParm[i]);

        readStart += read_itv;
    }

    for(int i = 0; i < NUM_THREADS; i++) {
        pthread_join(rthread[i], NULL);
    }
    
    return true;
}

bool ReadQuery()
{ // Read A Row At a Time
    if (!queryFile)
    {
        cout << "Error opening file.";
        return false;
    }

    numQuery = 0;
    int x;
    string list;
    if (getline(queryFile, list))
    {
        istringstream islist(list);
        while (islist >> x)
        {
            query[numQuery++] = x;
        }
        return true;
    }
    else
    {
        return false;
    }
}

void ReleaseSpace() {
    // Delete bindex
    for(int i = 0; i < numIndex; i++) {
        delete[] bindex[i];
    }
    delete[] bindex;

    // Delete len_bindex
    delete[] len_bindex;

    // Delete result
    delete[] result;

    // Delete rst
    delete[] rst;
}

void* GetIntersectionAndAnswer(void *param) {
    threadParm_t *p = (threadParm_t*) param;
    int t_id = p->threadID;
    int ori_t_id = t_id;

    while(!EndReadQuery) {
        sem_wait(&sem_calcStart);

        // Get Intersection
        int sect_itv = rstLen / NUM_THREADS;
        int sect_start = t_id * sect_itv;
        int sect_end = sect_start + sect_itv;
        if(t_id == NUM_THREADS - 1) sect_end = rstLen;
        // printf("%d: [%d, %d]\n", t_id, sect_start, sect_end);
        // if(t_id != ori_t_id) printf("Error From %d: t_id Changed\n", ori_t_id);
        for(int sect = sect_start; sect < sect_end; sect++) {
            // Get Intersection
            rst[sect] = bindex[query[0]][sect];
            for(int i = 1; i < numQuery; i++) {
                rst[sect] &= bindex[query[i]][sect];
            }

            // GetAnswer
            if(!rst[sect].none()) {
                for(int i = 0; i < 32; i++) {
                    if(rst[sect].test(i)) {
                        int ans = sect * 32 + i;
                        pthread_mutex_lock(&amutex);
                        // printf("%d From %d\n", ans, t_id);
                        result[resultCount++] = ans;
                        pthread_mutex_unlock(&amutex);
                    }
                }
            }
        }

        sem_post(&sem_main);
        sem_wait(&sem_calcEnd);
    }

    pthread_exit(NULL);
}

void ShowResult() {
    cout << resultCount << ": ";
    for(int i = 0; i < resultCount; i++) {
        cout << result[i] << " ";
    }
    cout << endl;
}

void test_code() {
    for(int i = 0; i < numIndex; i++) {
        for(int j = 0; j < len_bindex[i]; j++) {
            cout << bindex[i][j] << " ";
        }
        cout << endl;
    }
}

int main() {
    bool testMode = true;

    MyTimer fileInTimer, computeTimer;

    fileInTimer.start();
    if(testMode) {
        indexFile.open(testIndexEnlargePath, ios::in | ios::binary);
        queryFile.open(testQueryPath, ios ::in);
    }
    else {
        queryFile.open(queryPath, ios ::in);
        indexFile.open(indexPath, ios::in | ios::binary);
    }

    if (!TryReadIndex())
    {
        cout << "Error: Fail to reading index" << endl;
        return 0;
    }

    fileInTimer.finish();
    printf("Time Used in File Reading: %8.4fms\n", fileInTimer.getTime());

    computeTimer.start();

    rst = new bitset <32> [max_len_bindex];
    result = new int[maxLenIndex];
    resultCount = 0;
    int readCount = 0;

    sem_init(&sem_main, 0, 0);
    sem_init(&sem_calcStart, 0, 0);
    sem_init(&sem_calcEnd, 0, 0);

    pthread_t cthread[NUM_THREADS];
    cthreadParm_t cthreadParm[NUM_THREADS];
    for(int i = 0; i < NUM_THREADS; i++) {
        cthreadParm[i].threadID = i;
        pthread_create(&cthread[i], NULL, GetIntersectionAndAnswer, (void*)&cthreadParm[i]);
    }
    EndReadQuery = false;

    ReadQuery();
    while(!EndReadQuery) {
        if(numQuery < 2) continue;

        rstLen = INF;
        for(int i = 0; i < numQuery; i++) {
            int t = query[i];
            if(len_bindex[t] < rstLen) {
                rstLen = len_bindex[t];
            }
        }

        for(int i = 0; i < NUM_THREADS; i++) {
            sem_post(&sem_calcStart);
        }

        for(int i = 0; i < NUM_THREADS; i++) {
            sem_wait(&sem_main);
        }

        if(testMode) ShowResult();
        resultCount = 0;

        if(!ReadQuery()) {
            EndReadQuery = true;
        }

        for(int i = 0; i < NUM_THREADS; i++) {
            sem_post(&sem_calcEnd);
        }
    }

    for(int i = 0; i < NUM_THREADS; i++) {
        pthread_join(cthread[i], NULL);
    }

    sem_destroy(&sem_main);
    sem_destroy(&sem_calcStart);
    sem_destroy(&sem_calcEnd);
    computeTimer.finish();
    printf("Time Used in Computing Intersection: %8.4fms\n", computeTimer.getTime());

    ReleaseSpace();
    queryFile.close();
    indexFile.close();

    cout << "End" << endl;
}