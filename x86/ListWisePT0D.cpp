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

int max_len_bindex = -1;

pthread_mutex_t amutex = PTHREAD_MUTEX_INITIALIZER;

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

bool ReadIndex()
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
    for(int i = 0; i < numIndex; i++) {
        int bstNum = iindex[i][lenIndex[i] - 1];
        bstNum /= 32;
        bstNum++;
        len_bindex[i] = bstNum;

        if(bstNum > max_len_bindex) {
            max_len_bindex = bstNum;
        }

        bindex[i] = new bitset<32>[bstNum];
        for(int j = 0; j < lenIndex[i]; j++) {
            int t = iindex[i][j];
            bindex[i][t / 32].set(t % 32);
        }
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
    int sect = p->sect;
    int t_id = p->threadID;

    // Get Intersection
    rst[sect] = bindex[query[0]][sect];
    for(int i = 1; i < numQuery; i++) {
        if(rst[sect].none()) break;
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

    pthread_exit(NULL);
}   

void Calc() {
    if(numQuery < 2) return;

    int rstLen = INF;
    for(int i = 0; i < numQuery; i++) {
        int t = query[i];
        if(len_bindex[t] < rstLen) {
            rstLen = len_bindex[t];
        }
    }

    pthread_t thread[NUM_THREADS];
    threadParm_t threadParm[NUM_THREADS];
    int numThreads;

    for(int sect = 0; sect < rstLen; sect += NUM_THREADS) {
        if(sect + NUM_THREADS <= rstLen) {
            numThreads = NUM_THREADS;
        } else {
            numThreads = rstLen - sect;
        }

        for(int i = 0; i < numThreads; i++) {
            threadParm[i].sect = sect + i;
            threadParm[i].threadID = i;
            pthread_create(&thread[i], NULL, GetIntersectionAndAnswer, (void*)&threadParm[i]);
        }

        for(int i = 0; i < numThreads; i++) {
            pthread_join(thread[i], NULL);
        }
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
    bool testMode = true;
    bool perfMode = false;

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

    if (!ReadIndex())
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
    if(testMode) {
        while(ReadQuery()) {
            Calc();

            cout << "Result: " << endl;
            ShowResult();
            resultCount = 0;
        }
    }
    else {
        if(perfMode) {
            while(ReadQuery()) {
                Calc();
                resultCount = 0;
            }
        }
        else {
            cout << "Start Computing" << endl;

            while(ReadQuery() && readCount++ < 1000) {
                Calc();
                printf("Finish: (%d/1000)\n", readCount);
                resultCount = 0;
            }
        }
    }
    computeTimer.finish();
    printf("Time Used in Computing Intersection: %8.4fms\n", computeTimer.getTime());

    ReleaseSpace();
    queryFile.close();
    indexFile.close();

    cout << "End" << endl;
}