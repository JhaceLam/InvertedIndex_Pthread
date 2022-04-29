#include <iostream>
#include <cstdio>
#include <string>
#include <sstream>
#include <fstream>
#include <bitset>
#include "config.h"
#include <windows.h>
#include <pthread.h>
#include <pmmintrin.h>
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

int sect_itv;

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

bool AlignmentTest(void * addr) {
    long long address = reinterpret_cast<long long>(addr);
    if (address % 16LL) {
        return false;
    } else {
        return true;
    }
}

__m128i zero_m128i = _mm_setzero_si128();
char andOp[4] = {1, 2, 4, 8};

void* GetIntersectionAndAnswer(void *param) {
    threadParm_t *p = (threadParm_t*) param;
    int t_id = p->threadID;
    int sect_start = p->sect;
    int sect_end = sect_start + sect_itv;
    if(t_id == NUM_THREADS - 1) sect_end = rstLen;

    /*for(int i = 0; i < numQuery; i++) {
        if(!AlignmentTest(&bindex[query[i]][sect_start])) {
            cout << "ERROR" << endl;
        }
    }
    if(!AlignmentTest(&rst[sect_start])) {
        cout << "ERROR" << endl;
    }*/

    int sect = 0;
    __m128i x1, x2;
    __m128 cmp;
    char r;
    for(sect = sect_start; sect + 4 < sect_end; sect += 4) {
        // Get Intersection
        x1 = _mm_load_si128(reinterpret_cast<const __m128i*>(bindex[query[0]] + sect));
        for(int i = 1; i < numQuery; i++) {
            x2 = _mm_load_si128(reinterpret_cast<const __m128i*>(bindex[query[i]] + sect));
            x1 = _mm_and_si128(x1, x2);
        }

        _mm_store_si128(reinterpret_cast<__m128i*>(rst + sect), x1);

        // GetAnswer     
        cmp = reinterpret_cast<__m128>( _mm_cmpeq_epi32(x1, zero_m128i) );
        r = _mm_movemask_ps(cmp);
        if(r == 0xF) continue;
        
        for(int k = 0; k < 4; k++) {
            if( !(r & andOp[k]) ) { // [sect + k]
                for(int i = 0; i < 32; i++) {
                    if(rst[sect + k].test(i)) {
                        int ans = (sect + k) * 32 + i;
                        pthread_mutex_lock(&amutex);
                        result[resultCount++] = ans;
                        pthread_mutex_unlock(&amutex);
                    }
                }
            }
        }
        
    }
    // Remaining section
    for(; sect < sect_end; sect++) {
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
    sect_itv = rstLen / NUM_THREADS / 16 * 16;
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
    bool testMode = false;
    bool perfMode = true;

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