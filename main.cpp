#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <sched.h>
#include <cassert>
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
#include <fstream>

#define HUGE_PAGE_SIZE (1UL << 21UL)  // 2MB
#define ONE_NODE_MEM_SIZE (1UL << 33UL)  // 8G
#define DATA_SIZE (1UL << 12UL) // 4K
#define ONE_NODE_CPU_SETS 10
#define SKIP_SIZE 1

using namespace std;

double hot_ratio_;
uint64_t *local_hot_buf, *local_cold_buf, *local_huge_buf;
uint64_t *remote_hot_buf, *remote_cold_buf, *remote_huge_buf;
uint64_t small_pages, small_cold_pages, small_hot_pages;
uint64_t local_hot_pages, remote_hot_pages, local_cold_pages, remote_cold_pages; // all referencing to 4K pages
uint64_t huge_pages, local_huge_pages, remote_huge_pages;
uint64_t hot_pages[4], cold_pages[4];
uint64_t hot_prefix_sum[4], cold_prefix_sum[4];
uint64_t local_huge_to_small_hot, local_huge_to_small_cold;
uint64_t remote_huge_to_small_hot, remote_huge_to_small_cold;
ofstream logfile;

static inline void BUG_ON(bool cond) {
    if (cond) {
        raise(SIGABRT);
    }
}

uint64_t *mem_alloc_set_numa(long num_page, int node, bool huge) {
    uint64_t page_size = huge ? HUGE_PAGE_SIZE : DATA_SIZE;
    size_t size = num_page * page_size;
    uint64_t *ptr = static_cast<uint64_t *>(aligned_alloc(page_size, size));
    BUG_ON(ptr == NULL);
    if (huge) {
        int ret = madvise(ptr, size, MADV_HUGEPAGE);
        BUG_ON(ret != 0);
    }
    numa_tonode_memory(ptr, size, node);
    memset(ptr, 0, size);
    return ptr;
}

void inline access_page(volatile uint64_t &tmp, uint64_t *start, long &cur_num_op) {
    if (!start)
        return;
    // every op includes 4KB/64b/SKIP_SIZE times addition: currently it is 512 times for skip_size = 1
    for (uint64_t k = 0; k < DATA_SIZE / sizeof(uint64_t); k+=SKIP_SIZE) {
        tmp += start[k];
    }
    cur_num_op += 1;
}

uint64_t calculate_start_index(uint64_t cur_start_page, bool hot) {
    // cur_start_page is the unit of 4K pages

    uint64_t granularity = long(HUGE_PAGE_SIZE * hot_ratio_) / DATA_SIZE; // hot 4k pages per huge page
    // printf("granularity %ld\n", granularity);
    uint64_t start_index;
    if (hot) {
        if (granularity)
            start_index = cur_start_page / granularity * HUGE_PAGE_SIZE + 
                    cur_start_page % granularity * DATA_SIZE;
        else
            start_index = -1 * sizeof(uint64_t); // less than zero means no hot pages
    } else {
        if (granularity)
            start_index = cur_start_page / granularity * HUGE_PAGE_SIZE + uint64_t(HUGE_PAGE_SIZE * hot_ratio_)
                        + cur_start_page % granularity * DATA_SIZE;
        else
            start_index = cur_start_page * DATA_SIZE; // 4K is a unit
    }
    
    return start_index / sizeof(uint64_t);
}

uint64_t *pick_start(bool hot) {
    uint64_t *pages, *prefix_sum;
    uint64_t *local_small_buf, *remote_small_buf;
    if (hot) {
        pages = hot_pages;
        prefix_sum = hot_prefix_sum;
        local_small_buf = local_hot_buf;
        remote_small_buf = remote_hot_buf;
    }
    else {
        pages = cold_pages;
        prefix_sum = cold_prefix_sum;
        local_small_buf = local_cold_buf;
        remote_small_buf = remote_cold_buf;
    }
    uint64_t start_page = rand() % prefix_sum[3];
    if (start_page < prefix_sum[0]) {
        uint64_t cur_start_page = rand() % pages[0];
 return local_small_buf + cur_start_page * DATA_SIZE / sizeof(local_small_buf);
    } else if (start_page < prefix_sum[1]) {
        uint64_t cur_start_page = rand() % pages[1]; // the cur_start_page is with unit 4K already
        uint64_t start_index = calculate_start_index(cur_start_page, hot);
        if (start_index > ONE_NODE_MEM_SIZE / sizeof(uint64_t))
            return NULL;
        return local_huge_buf + start_index;
    } else if (start_page < prefix_sum[2]) {
        uint64_t cur_start_page = rand() % pages[2];
return remote_small_buf + cur_start_page * DATA_SIZE / sizeof(remote_small_buf);
    } else {
        uint64_t cur_start_page = rand() % pages[3];
        uint64_t start_index = calculate_start_index(cur_start_page, hot);
        if (start_index > ONE_NODE_MEM_SIZE / sizeof(uint64_t))
            return NULL;
        return remote_huge_buf + start_index;
    }
}

void thread_fn(int thread_index, long num_op,
               volatile bool *terminate, double *result_arr) {
    int ret;
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(1 + thread_index, &set);
    assert(1 + thread_index < ONE_NODE_CPU_SETS);
    ret = sched_setaffinity(0, sizeof(set), &set);
    BUG_ON(ret != 0);

    long cur_num_op = 0;
    srand((unsigned)time(NULL));
    chrono::time_point<chrono::steady_clock> start_time = chrono::steady_clock::now();
    if (hot_ratio_ >= 0.1 && hot_ratio_ <= 0.9) {
        while (!(*terminate) && cur_num_op < num_op) {
            bool hot = (rand() % 10 < 9); // 0-8 hot, 9 cold
            volatile uint64_t tmp = 0;
            access_page(tmp, pick_start(hot), cur_num_op);
        }
    } else if (hot_ratio_ > 0.9) {
        while (!(*terminate) && cur_num_op < num_op) {
            bool hot = (rand() % 10 >= 0);
            volatile uint64_t tmp = 0;
            access_page(tmp, pick_start(hot), cur_num_op);
        }
    }
    else {
        while (!(*terminate) && cur_num_op < num_op) {
            bool hot = (rand() % 10 < 0); //always = false when there is no hot pages
            volatile uint64_t tmp = 0;
            access_page(tmp, pick_start(hot), cur_num_op);
        }
    }

    // printf("loop ended\n");
    chrono::time_point<chrono::steady_clock> end_time = chrono::steady_clock::now();
    *terminate = true;

    result_arr[thread_index] = (double) cur_num_op
                               / (((double) chrono::duration_cast<std::chrono::nanoseconds>(end_time - start_time).count())
                                  / 1e9);
}

void update_hot_cold_pages(void) {
    hot_pages[0] = local_hot_pages;
    hot_pages[1] = local_huge_to_small_hot;
    hot_pages[2] = remote_hot_pages;
    hot_pages[3] = remote_huge_to_small_hot;

    cold_pages[0] = local_cold_pages;
    cold_pages[1] = local_huge_to_small_cold;
    cold_pages[2] = remote_cold_pages;
    cold_pages[3] = remote_huge_to_small_cold;
    
    for (int i = 0; i < 4; ++i) {
        hot_prefix_sum[i] = hot_pages[i];
        cold_prefix_sum[i] = cold_pages[i];
        if (i) {
            hot_prefix_sum[i] += hot_prefix_sum[i-1];
            cold_prefix_sum[i] += cold_prefix_sum[i-1];
        }
    }

    // logfile << "hot pages: ";
    // for (int i = 0; i < 4; ++i)
    //     logfile << hot_pages[i] << " ";
    // logfile << endl;

    // logfile << "hot prefix: ";
    // for (int i = 0; i < 4; ++i)
    //     logfile << hot_prefix_sum[i] << " ";
    // logfile << endl;

    // logfile << "cold pages: ";
    // for (int i = 0; i < 4; ++i)
    //     logfile << cold_pages[i] << " ";
    // logfile << endl;

    // logfile << "cold prefix: ";
    // for (int i = 0; i < 4; ++i)
    //     logfile << cold_prefix_sum[i] << " ";
    // logfile << endl;

    // logfile << endl;
}

void calculate_pages(double split_ratio) {
    small_pages = (split_ratio * ONE_NODE_MEM_SIZE * 2) / DATA_SIZE;
    small_hot_pages = small_pages * hot_ratio_;
    small_cold_pages = small_pages - small_hot_pages;

    uint64_t local_hot_mem = min(ONE_NODE_MEM_SIZE, small_hot_pages * DATA_SIZE);
    local_hot_pages = local_hot_mem / DATA_SIZE;
    remote_hot_pages = max(small_hot_pages - local_hot_pages, uint64_t(0));

    huge_pages = ((1 - split_ratio) * ONE_NODE_MEM_SIZE * 2) / HUGE_PAGE_SIZE;

    local_huge_pages = min((ONE_NODE_MEM_SIZE - local_hot_mem) / HUGE_PAGE_SIZE, huge_pages);
    local_cold_pages = min((ONE_NODE_MEM_SIZE - local_hot_mem - local_huge_pages * HUGE_PAGE_SIZE) / DATA_SIZE, small_cold_pages);
    remote_cold_pages = small_cold_pages - local_cold_pages;
    remote_huge_pages = huge_pages - local_huge_pages;

    local_huge_to_small_hot = local_huge_pages * HUGE_PAGE_SIZE * hot_ratio_ / DATA_SIZE;
    remote_huge_to_small_hot = remote_huge_pages * HUGE_PAGE_SIZE * hot_ratio_ / DATA_SIZE;

    local_huge_to_small_cold = local_huge_pages * HUGE_PAGE_SIZE * (1 - hot_ratio_) / DATA_SIZE;
    remote_huge_to_small_cold = remote_huge_pages * HUGE_PAGE_SIZE * (1 - hot_ratio_) / DATA_SIZE;

    // logfile << "small_pages: " <<
    //     small_pages << " small_hot: " << small_hot_pages << " small_cold:" << small_cold_pages << "\nlocal_hot: " 
    //     << local_hot_pages << " remote_hot: " << remote_hot_pages << " local_cold: " << local_cold_pages << " remote_cold: " << remote_cold_pages << "\nhuge_pages: " << huge_pages << " local_huge: "
    //     << local_huge_pages << " remote_huge: " <<remote_huge_pages << "\nlocal_huge_to_small_hot: " << local_huge_to_small_hot
    //     << " local_huge_to_small_cold: " << local_huge_to_small_cold << "\nremote_huge_to_small_hot: " 
    //     <<remote_huge_to_small_hot << " remote_huge_to_small_cold: " << remote_huge_to_small_cold << endl << endl;
    
    assert(small_pages * DATA_SIZE + huge_pages * HUGE_PAGE_SIZE <= ONE_NODE_MEM_SIZE * 2);
    assert(small_pages * DATA_SIZE + huge_pages * HUGE_PAGE_SIZE >= ONE_NODE_MEM_SIZE * 2 - HUGE_PAGE_SIZE * 2 - DATA_SIZE * 2);
    assert(local_hot_pages * DATA_SIZE + local_huge_pages * HUGE_PAGE_SIZE + local_cold_pages * DATA_SIZE <= ONE_NODE_MEM_SIZE);
    assert(local_hot_pages * DATA_SIZE + local_huge_pages * HUGE_PAGE_SIZE + local_cold_pages * DATA_SIZE >= ONE_NODE_MEM_SIZE - HUGE_PAGE_SIZE);
    assert(remote_hot_pages * DATA_SIZE + remote_huge_pages * HUGE_PAGE_SIZE + remote_cold_pages * DATA_SIZE <= ONE_NODE_MEM_SIZE);
    assert(remote_hot_pages * DATA_SIZE + remote_huge_pages * HUGE_PAGE_SIZE + remote_cold_pages * DATA_SIZE >= ONE_NODE_MEM_SIZE - HUGE_PAGE_SIZE);
    
    update_hot_cold_pages();
}

void main_experiment(long num_op, long num_thread, double split_ratio, double hot_ratio) {

    hot_ratio_ = hot_ratio;
    // printf("num_op is %ld, num_thread is %ld, split_ratio is %lf, hot_ratio is %lf\n",
    //        num_op, num_thread, split_ratio, hot_ratio);
    printf("%ld | %lf | %lf ", num_op, split_ratio, hot_ratio_);


    int ret;
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(0, &set);
    ret = sched_setaffinity(0, sizeof(set), &set);
    BUG_ON(ret != 0);

    thread *thread_arr = new thread[num_thread];
    double *result_arr = new double[num_thread];
    volatile bool terminate = false;

    calculate_pages(split_ratio);

    local_hot_buf = mem_alloc_set_numa(local_hot_pages, 0, false);
    local_cold_buf = mem_alloc_set_numa(local_cold_pages, 0, false);
    local_huge_buf = mem_alloc_set_numa(local_huge_pages, 0, true);
    remote_hot_buf = mem_alloc_set_numa(remote_hot_pages, 1, false);
    remote_cold_buf = mem_alloc_set_numa(remote_cold_pages, 1, false);
    remote_huge_buf = mem_alloc_set_numa(remote_huge_pages, 1, true);

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
        // printf("thread %d: %f\n", thread_index, result_arr[thread_index]);
        sum += result_arr[thread_index];
    }

    // printf("overall throughput: %f GB/s\n", (sum * DATA_SIZE) / (1L << 30L));
    printf("| %f\n", (sum * DATA_SIZE) / (1L << 30L));
    fflush(stdout);

    free(local_hot_buf);
    free(local_cold_buf);
    free(local_huge_buf);
    free(remote_hot_buf);
    free(remote_cold_buf);
    free(remote_huge_buf);
}


int main(int argc, char *argv[]) {

    double split_ratios[] = {0.0 ,0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};
    double hot_ratios[] = {0.0 ,0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0};
    long num_threads[] = {};
    // long num_ops[] = {10000000, 20000000, 40000000, 80000000, 100000000};

    BUG_ON(argc != 3);

    int split_idx= atoi(argv[1]), hot_idx = atoi(argv[2]);
    // int op_idx= 0, hot_idx = atoi(argv[2]);
    // cout << split_idx << hot_idx << endl;

    long num_op, num_thread;
    double split_ratio, hot_ratio; // the maximum is 1

    logfile.open("log.txt");

    num_op = 10000000, num_thread = 8, split_ratio = split_ratios[split_idx], hot_ratio = hot_ratios[hot_idx];
        // printf("%ld\n", ONE_NODE_MEM_SIZE / sizeof(uint64_t));
    main_experiment(num_op, num_thread, split_ratio, hot_ratio);

    // for (int i = 0; i < 11; i += 2) {
    //     for (int j = 0; j < 11; j += 2) {
    //         num_op = 1000000, num_thread = 8, split_ratio = split_ratios[i], hot_ratio = hot_ratios[j];
    //         main_experiment(num_op, num_thread, split_ratio, hot_ratio);
    //     }
    // }

    // for (int i = 0; i < 30; ++i) {
    //         num_op = 1000000, num_thread = 8, split_ratio = 0.1, hot_ratio = 0.1;
    //     // printf("%ld\n", ONE_NODE_MEM_SIZE / sizeof(uint64_t));
    //         main_experiment(num_op, num_thread, split_ratio, hot_ratio);
    // }

    logfile.close();


    return 0;
}
