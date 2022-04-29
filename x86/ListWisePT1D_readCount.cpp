#include <iostream>
#include <cstdio>
#include <string>
#include <sstream>
#include <fstream>
#include <bitset>
#include "config.h"
#include <windows.h>
#include <pthread.h>
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

class MyTimer {
    private:
        long long start_time, finish_time, freq;
        double timeMS;

    public:
        MyTimer() {
            timeMS = 0;
            QueryPerformanceFrequency((LARGE_INTEGER*) &freq);
        }

        void start() {
            QueryPerformanceCounter((LARGE_INTEGER*) &start_time);
        }

        void finish() {
            QueryPerformanceCounter((LARGE_INTEGER*) &finish_time);
        }
        
        double getTime() {
            // ms
            double duration = (finish_time - start_time) * 1000.0 / freq;
            return duration;
        }
};

typedef struct {
    int threadID;
    int sect;
} threadParm_t;

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
    int sect_start = p->sect;
    sect_itv = rstLen / NUM_THREADS;
    int sect_end = sect_start + sect_itv;
    if(t_id == NUM_THREADS - 1) sect_end = rstLen;

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
                    int x = sect * 32 + i;
                    pthread_mutex_lock(&amutex);
                    result[resultCount++] = x;
                    pthread_mutex_unlock(&amutex);
                }
            }
        }
    }

    pthread_exit(NULL);
}   

void Calc() {
    if(numQuery < 2) return;

    rstLen = INF;
    for(int i = 0; i < numQuery; i++) {
        int t = query[i];
        if(len_bindex[t] < rstLen) {
            rstLen = len_bindex[t];
        }
    }

    pthread_t thread[NUM_THREADS];
    threadParm_t threadParm[NUM_THREADS];
    int numThreads;

    int sect = 0;
    sect_itv = rstLen / NUM_THREADS;
    for(int i = 0; i < NUM_THREADS; i++) {
        threadParm[i].sect = sect;
        threadParm[i].threadID = i;

        pthread_create(&thread[i], NULL, GetIntersectionAndAnswer, (void*)&threadParm[i]);

        sect += sect_itv;
    }

    for(int i = 0; i < NUM_THREADS; i++) {
        pthread_join(thread[i], NULL);
    }
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
    int readLimit[] = {1, 2, 3, 4, 5, 10, 15, 20, 25, 30, 40, 50, 60, 70, 80, 90, 
    100, 125, 150, 175, 200, 225, 250, 275, 300, 325, 350, 375, 400, 425, 450, 475, 500, 525, 
    550, 575, 600, 625, 650, 675, 700, 725, 750, 775, 800, 825, 850, 875, 900, 925, 950, 975, 1000};
    int readLimitSize = sizeof(readLimit) / sizeof(int);

    MyTimer fileInTimer, computeTimer;

    fileInTimer.start();
    queryFile.open(queryPath, ios ::in);
    indexFile.open(indexPath, ios::in | ios::binary);

    if (!TryReadIndex())
    {
        cout << "Error: Fail to reading index" << endl;
        return 0;
    }

    fileInTimer.finish();
    printf("Time Used in File Reading: %8.4fms\n", fileInTimer.getTime());

    rst = new bitset <32> [max_len_bindex];
    result = new int[maxLenIndex];
    resultCount = 0;
    
    printf("Time Used in Computing Intersection:\n");
    computeTimer.start();

    int counter = 0;
    int readPhase = 0;
    while(ReadQuery()) {
        Calc();
        resultCount = 0;

        counter++;
        if(counter == readLimit[readPhase]) {
            computeTimer.finish();
            printf("%d: %8.4fms\n", counter, computeTimer.getTime());
            readPhase++;
        }
    }
    
    computeTimer.finish();
    printf("Time Used in Computing Intersection: %8.4fms\n", computeTimer.getTime());

    ReleaseSpace();
    queryFile.close();
    indexFile.close();

    cout << "End" << endl;
}