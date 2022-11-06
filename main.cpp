#define _GNU_SOURCE
#include <sched.h>
#include <iostream>
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <cmath>
#include <thread>
#include <atomic>
#include <numa.h>
#include <sys/mman.h>
#include <unistd.h>
#include <random>
#include <fcntl.h>
#include <time.h>

#define HUGE_PAGE_SIZE (1UL << 21UL)  // 2MB
#define ONE_NODE_MEM_SIZE (1UL << 33UL)  // 8G
#define DATA_SIZE (1UL << 12UL) // 4K
#define ONE_NODE_CPU_SETS 8 // todo: need to change?

using namespace std;

double hot_ratio_;
uint64_t *local_hot_buf, *local_cold_buf, *local_huge_buf;
uint64_t *remote_hot_buf, *remote_cold_buf, *remote_huge_buf;
int small_pages, small_cold_pages, small_hot_pages;
int local_hot_pages, remote_hot_pages, local_cold_pages, remote_cold_pages; // all referencing to 4K pages
int huge_pages, local_huge_pages, remote_huge_pages;
int hot_pages, cold_pages;
int hot_first, hot_second, hot_third, hot_fourth;
long local_huge_to_small_hot;
long remote_huge_to_small_hot;

static inline void BUG_ON(bool cond) {
    if (cond) {
        raise(SIGABRT);
    }
}

void mem_alloc_set_numa(uint64_t *ptr, long num_page, int node=0, bool huge=false) {
    uint64_t page_size = huge ? HUGE_PAGE_SIZE : DATA_SIZE;
    size_t size = num_page * page_size;
    ptr = static_cast<uint64_t *>(aligned_alloc(page_size, size));
    BUG_ON(ptr == NULL);
    if (huge) {
        int ret = madvise(ptr, size, MADV_HUGEPAGE);
        BUG_ON(ret != 0);
    }
    memset(ptr, 0, size);
    numa_tonode_memory(ptr, size, node);
}

void inline access_page(volatile uint64_t &tmp, uint64_t *start) {
    for (uint64_t k = 0; k < DATA_SIZE / sizeof(uint64_t); ++k) {
        tmp += start[(DATA_SIZE / sizeof(uint64_t)) + k];
    }
    // todo: definition of a single op?
}

uint64_t *pick_start_hot() {
    int start_page = rand() % hot_pages;
    if (start_page < hot_first) {
        int cur_start_page = rand() % local_hot_pages;
        return local_hot_buf + cur_start_page * DATA_SIZE;
    } else if (start_page < hot_second) {
        int cur_start_page = rand() % local_huge_to_small_hot;
        int granularity = HUGE_PAGE_SIZE * hot_ratio_ / DATA_SIZE; // hot 4k pages per huge page
        int start_index = cur_start_page / granularity * HUGE_PAGE_SIZE + cur_start_page % granularity * DATA_SIZE;
        return local_huge_buf + start_index;
    } else if (start_page < hot_third) {
        int cur_start_page = rand() % remote_hot_pages;
        return remote_hot_buf + cur_start_page * DATA_SIZE;
    } else {
        int cur_start_page = rand() % remote_huge_to_small_hot;
        int granularity = HUGE_PAGE_SIZE * hot_ratio_ / DATA_SIZE; // hot 4k pages per huge page
        int start_index = cur_start_page / granularity * HUGE_PAGE_SIZE + cur_start_page % granularity * DATA_SIZE;
        return remote_huge_buf + start_index;
    }
}

void thread_fn(int thread_index, long num_op,
               volatile bool *terminate, double *result_arr) {
    int ret;
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(1 + thread_index, &set);
    ret = sched_setaffinity(0, sizeof(set), &set);
    BUG_ON(ret != 0);

    long cur_num_op = 0;
    srand((unsigned)time(NULL));
    chrono::time_point<chrono::steady_clock> start_time = chrono::steady_clock::now();
    while (!(*terminate) && cur_num_op < num_op) {
        uint64_t next_index;
        volatile uint64_t tmp = 0;
        access_page(tmp, pick_start_hot());
        cur_num_op += 1;
    }
    chrono::time_point<chrono::steady_clock> end_time = chrono::steady_clock::now();
    *terminate = true;

    result_arr[thread_index] = (double) cur_num_op
                               / (((double) chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count())
                                  / 1e9);
}

void calculate_pages(double split_ratio) {
    small_pages = (split_ratio * ONE_NODE_MEM_SIZE * 2) / DATA_SIZE;
    small_hot_pages = small_pages * hot_ratio_;
    small_cold_pages = small_pages - small_hot_pages;

    uint64_t local_hot_mem = min(ONE_NODE_MEM_SIZE, small_hot_pages * DATA_SIZE);
    local_hot_pages = local_hot_mem / DATA_SIZE;
    remote_hot_pages = max(small_hot_pages - local_hot_pages, 0);

    huge_pages = ((1 - split_ratio) * ONE_NODE_MEM_SIZE * 2) / HUGE_PAGE_SIZE;

    local_huge_pages = (ONE_NODE_MEM_SIZE - local_hot_mem) / HUGE_PAGE_SIZE;
    local_cold_pages = (ONE_NODE_MEM_SIZE - local_hot_mem - local_huge_pages * HUGE_PAGE_SIZE) / DATA_SIZE;
    remote_cold_pages = small_cold_pages - local_cold_pages;
    remote_huge_pages = huge_pages - local_huge_pages;

    local_huge_to_small_hot = local_huge_pages * HUGE_PAGE_SIZE * hot_ratio_ / DATA_SIZE;
    remote_huge_to_small_hot = remote_huge_pages * HUGE_PAGE_SIZE * hot_ratio_ / DATA_SIZE;

    hot_pages = local_hot_pages + local_huge_to_small_hot + remote_hot_pages + remote_huge_to_small_hot;
    hot_first = local_hot_pages;
    hot_second = hot_first + local_huge_to_small_hot;
    hot_third = hot_second + remote_hot_pages;
    hot_fourth = hot_third + remote_huge_to_small_hot;
}

void main_experiment(long num_op, long num_thread, double split_ratio, double hot_ratio) {

    hot_ratio_ = hot_ratio;
    printf("num_op is %ld, num_thread is %ld, split_ratio is %lf, hot_ratio is %lf\n",
           num_op, num_thread, split_ratio, hot_ratio);

    // todo: asser num_thread < cores on single CPU

    int ret;
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(0, &set);
    ret = sched_setaffinity(0, sizeof(set), &set);
    BUG_ON(ret != 0);

    thread *thread_arr = new thread[num_thread];
    double *result_arr = new double[num_thread];
    volatile bool terminate = false;

//    printf("allocating buffer...\n");
//    uint64_t *buffer;

    calculate_pages(split_ratio);

    // todo: not considering access cold pages right now

    mem_alloc_set_numa(local_hot_buf, local_hot_pages, 0, false);
    mem_alloc_set_numa(local_cold_buf, local_cold_pages, 0, false);
    mem_alloc_set_numa(local_huge_buf, local_huge_pages, 0, true);
    mem_alloc_set_numa(remote_hot_buf, remote_hot_pages, 1, false);
    mem_alloc_set_numa(remote_cold_buf, remote_cold_pages, 1, false);
    mem_alloc_set_numa(remote_huge_buf, remote_huge_pages, 1, true);

    printf("start measurement\n");

    fflush(stdout);

    for (int thread_index = 0; thread_index < num_thread; ++thread_index) {
        thread_arr[thread_index] = thread(thread_fn, thread_index, num_op,
                                          &terminate, result_arr);
    }
    for (int thread_index = 0; thread_index < num_thread; ++thread_index) {
        thread_arr[thread_index].join();
    }

    double sum = 0;
    for (int thread_index = 0; thread_index < num_thread; ++thread_index) {
        printf("thread %d: %f\n", thread_index, result_arr[thread_index]);
        sum += result_arr[thread_index];
    }

    printf("overall throughput: %f GB/s\n", (sum * DATA_SIZE) / (1L << 30L));
    fflush(stdout);

    free(local_hot_buf);
    free(local_cold_buf);
    free(local_huge_buf);
    free(remote_hot_buf);
    free(remote_cold_buf);
    free(remote_huge_buf);
}


int main(int argc, char *argv[]) {

    double split_ratios[] = {};
    long hot_ratios[] = {};
    long num_threads[] = {};

    long num_op, num_thread;
    double split_ratio, hot_ratio; // the maximum is 1

    printf("maximum CPU setsize is %d", CPU_SETSIZE);

    main_experiment(num_op, num_thread, split_ratio, hot_ratio);

    return 0;
}