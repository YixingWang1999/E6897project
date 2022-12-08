#ifndef _GNU_SOURCE
#define int64_t _GNU_SOURCE
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

const int64_t HUGE_PAGE_SIZE = 1L << 21L; // 2MB
const int64_t ONE_NODE_MEM_SIZE = 1L << 33L;  // 8G
const int64_t DATA_SIZE = 1L << 12L; // 4K
const int64_t ONE_NODE_CPU_SETS = 10L;
const int64_t SKIP_SIZE = 1L;

// #define HUGE_PAGE_SIZE (1UL << 21UL) // 2MB
// #define ONE_NODE_MEM_SIZE (1UL << 33UL)  // 8G
// #define DATA_SIZE (1UL << 12UL) // 4K
// #define ONE_NODE_CPU_SETS 10
// #define SKIP_SIZE 11

// const int64_t HUGE_PAGE_SIZE (1UL << 21UL)  // 2MB
// const int64_t ONE_NODE_MEM_SIZE (1UL << 33UL)  // 8G
// const int64_t DATA_SIZE (1UL << 12UL) // 4K
// const int64_t ONE_NODE_CPU_SETS 10
// const int64_t SKIP_SIZE 1

using namespace std;

double hot_ratio_;
unsigned int *seeds;
int64_t *local_hot_buf, *local_cold_buf, *local_huge_buf;
int64_t *remote_hot_buf, *remote_cold_buf, *remote_huge_buf;
int64_t small_pages, small_cold_pages, small_hot_pages;
int64_t local_hot_pages, remote_hot_pages, local_cold_pages, remote_cold_pages; // all referencing to 4K pages
int64_t huge_pages, local_huge_pages, remote_huge_pages;
int64_t hot_pages[4], cold_pages[4];
int64_t hot_prefix_sum[4], cold_prefix_sum[4];
int64_t local_huge_to_small_hot, local_huge_to_small_cold;
int64_t remote_huge_to_small_hot, remote_huge_to_small_cold;
ofstream logfile;

static inline void BUG_ON(bool cond) {
    if (cond) {
        raise(SIGABRT);
    }
}

int64_t *mem_alloc_set_numa(long num_page, int node, bool huge) {
    int64_t page_size = huge ? HUGE_PAGE_SIZE : DATA_SIZE;
    size_t size = num_page * page_size;
    int64_t *ptr = static_cast<int64_t *>(aligned_alloc(page_size, size));
    BUG_ON(ptr == NULL);
    if (huge) {
        int ret = madvise(ptr, size, MADV_HUGEPAGE);
        BUG_ON(ret != 0);
    }
    numa_tonode_memory(ptr, size, node);
    memset(ptr, 0, size);
    // printf("\nsizes: %ld, %ld\n", size / DATA_SIZE, int64_t(size / DATA_SIZE * hot_ratio_));
    return ptr;
}

void inline access_page(volatile int64_t &tmp, int64_t *start, long &cur_num_op) {
    if (!start)
        return;
    // every op includes 4KB/64b/SKIP_SIZE times addition: currently it is 512 times for skip_size = 1
    for (int64_t k = 0; k < DATA_SIZE / sizeof(tmp); k+=SKIP_SIZE) {
        tmp += start[k];
    }
    cur_num_op += 1;
}

int64_t calculate_start_index(int64_t cur_start_page, bool hot) {
    // cur_start_page is the unit of 4K pages
    // we want to find the huge page number given the cur_start_pag

    
    
    // printf("\ngranularity %ld\n", granularity);
    int64_t start_index;
    // volatile int64_t complexer;
    // printf("hot: %d, cur_start_page: %ld\n", int(hot), cur_start_page);
    if (hot) {
        int64_t granularity = ceil(HUGE_PAGE_SIZE / DATA_SIZE * hot_ratio_); // hot 4k pages per huge page
        if (granularity) {
            start_index = cur_start_page / granularity * HUGE_PAGE_SIZE + 
                    cur_start_page % granularity * DATA_SIZE;
            // printf("\n1: \n");
        } else {
            // less than zero means no hot pages
            // we complex the computation here to be more fair to no hot pages
            // complexer = -1000 % sizeof(int64_t) * DATA_SIZE - 1000 / sizeof(int64_t) * HUGE_PAGE_SIZE; 
            start_index = -1 * sizeof(int64_t);
            // printf("\n2\n");
        }
    } else {
        int64_t cold_granularity = ceil(HUGE_PAGE_SIZE / DATA_SIZE * (1 - hot_ratio_)); // cold 4k pages per huge page
        if (cold_granularity) {
            // todo: this is where the bug is!!!
            // start_index = cur_start_page / granularity * HUGE_PAGE_SIZE + int64_t(HUGE_PAGE_SIZE * hot_ratio_)
            //             + cur_start_page % granularity * DATA_SIZE;
            // todo: needs to rebuild here
            start_index = cur_start_page / cold_granularity * HUGE_PAGE_SIZE
                        + int64_t(HUGE_PAGE_SIZE * hot_ratio_) // skip the hot area
                        + cur_start_page % cold_granularity * DATA_SIZE;
            // printf("\n3\n");
        } else {
            // complexer = -1000 % sizeof(int64_t) * DATA_SIZE - 1000 / sizeof(int64_t) * HUGE_PAGE_SIZE; 
            start_index = cur_start_page * DATA_SIZE; // 4K is a unit
            // printf("\n4\n");
        }
    }
    
    return start_index;
}

int64_t *pick_start_hot(int thread_idx) {
    int64_t start_page = rand_r(seeds + thread_idx) % hot_prefix_sum[3];
    if (start_page < hot_prefix_sum[0]) {
        // int64_t cur_start_page = rand() % pages[0];
        int64_t cur_start_page = start_page - 0;
        return local_hot_buf + cur_start_page * DATA_SIZE / sizeof(local_hot_buf);
    } else if (start_page < hot_prefix_sum[1]) {
        // int64_t cur_start_page = rand() % pages[1]; // the cur_start_page is with unit 4K already
        int64_t cur_start_page = start_page - hot_prefix_sum[0];
        int64_t start_index = calculate_start_index(cur_start_page, true);
        if (start_index < 0)
            return NULL;
        // cout<< endl << "hh" << start_index / DATA_SIZE << endl;
        // if (start_index > ONE_NODE_MEM_SIZE / sizeof(int64_t))
        //     return NULL;
        return local_huge_buf + start_index / sizeof(local_huge_buf);
    } else if (start_page < hot_prefix_sum[2]) {
        // int64_t cur_start_page = rand() % pages[2];
        int64_t cur_start_page = start_page - hot_prefix_sum[1];
        return remote_hot_buf + cur_start_page * DATA_SIZE / sizeof(remote_hot_buf);
    } else {
        // int64_t cur_start_page = rand() % pages[3];
        int64_t cur_start_page = start_page - hot_prefix_sum[2];
        int64_t start_index = calculate_start_index(cur_start_page, true);
        if (start_index < 0)
            return NULL;
        // cout<< endl << "ll" << start_index / DATA_SIZE << endl;
        // if (start_index > ONE_NODE_MEM_SIZE / sizeof(int64_t))
        //     return NULL;
        return remote_huge_buf + start_index / sizeof(remote_huge_buf);
    }
}


int64_t *pick_start_cold(int thread_idx) {
    int64_t start_page = rand_r(seeds + thread_idx) % cold_prefix_sum[3];
    if (start_page < cold_prefix_sum[0]) {
        // int64_t cur_start_page = rand() % pages[0];
        int64_t cur_start_page = start_page - 0;
        return local_cold_buf + cur_start_page * DATA_SIZE / sizeof(local_cold_buf);
    } else if (start_page < cold_prefix_sum[1]) {
        // int64_t cur_start_page = rand() % pages[1]; // the cur_start_page is with unit 4K already
        int64_t cur_start_page = start_page - cold_prefix_sum[0];
        int64_t start_index = calculate_start_index(cur_start_page, false);
        if (start_index < 0)
            return NULL;
        // cout<< endl << "hh" << start_index / DATA_SIZE << endl;
        // if (start_index > ONE_NODE_MEM_SIZE / sizeof(int64_t))
        //     return NULL;
        return local_huge_buf + start_index / sizeof(local_huge_buf);
    } else if (start_page < cold_prefix_sum[2]) {
        // int64_t cur_start_page = rand() % pages[2];
        int64_t cur_start_page = start_page - cold_prefix_sum[1];
        return remote_cold_buf + cur_start_page * DATA_SIZE / sizeof(remote_cold_buf);
    } else {
        // int64_t cur_start_page = rand() % pages[3];
        int64_t cur_start_page = start_page - cold_prefix_sum[2];
        int64_t start_index = calculate_start_index(cur_start_page, false);
        if (start_index < 0)
            return NULL;
        // cout<< endl << "ll" << start_index / DATA_SIZE << endl;
        // if (start_index > ONE_NODE_MEM_SIZE / sizeof(int64_t))
        //     return NULL;
        return remote_huge_buf + start_index / sizeof(remote_huge_buf);
    }
}

int64_t *pick_start(bool hot, int thread_index) {
    if (hot)
        return pick_start_hot(thread_index);
    else
        return pick_start_cold(thread_index);
}

// int64_t *pick_start(bool hot) {
//     int64_t *pages, *prefix_sum;
//     int64_t *local_small_buf, *remote_small_buf;
//     if (hot) {
//         pages = hot_pages;
//         prefix_sum = hot_prefix_sum;
//         local_small_buf = local_hot_buf;
//         remote_small_buf = remote_hot_buf;
//     }
//     else {
//         pages = cold_pages;
//         prefix_sum = cold_prefix_sum;
//         local_small_buf = local_cold_buf;
//         remote_small_buf = remote_cold_buf;
//     }
//     int64_t start_page = rand() % prefix_sum[3];
//     if (start_page < prefix_sum[0]) {
//         // int64_t cur_start_page = rand() % pages[0];
//         int64_t cur_start_page = start_page - 0;
//         return local_small_buf + cur_start_page * DATA_SIZE / sizeof(local_small_buf);
//     } else if (start_page < prefix_sum[1]) {
//         // int64_t cur_start_page = rand() % pages[1]; // the cur_start_page is with unit 4K already
//         int64_t cur_start_page = start_page - prefix_sum[0];
//         int64_t start_index = calculate_start_index(cur_start_page, hot);
//         if (start_index < 0)
//             return NULL;
//         // cout<< endl << "hh" << start_index / DATA_SIZE << endl;
//         // if (start_index > ONE_NODE_MEM_SIZE / sizeof(int64_t))
//         //     return NULL;
//         return local_huge_buf + start_index / sizeof(local_huge_buf);
//     } else if (start_page < prefix_sum[2]) {
//         // int64_t cur_start_page = rand() % pages[2];
//         int64_t cur_start_page = start_page - prefix_sum[1];
//         return remote_small_buf + cur_start_page * DATA_SIZE / sizeof(remote_small_buf);
//     } else {
//         // int64_t cur_start_page = rand() % pages[3];
//         int64_t cur_start_page = start_page - prefix_sum[2];
//         int64_t start_index = calculate_start_index(cur_start_page, hot);
//         if (start_index < 0)
//             return NULL;
//         // cout<< endl << "ll" << start_index / DATA_SIZE << endl;
//         // if (start_index > ONE_NODE_MEM_SIZE / sizeof(int64_t))
//         //     return NULL;
//         return remote_huge_buf + start_index / sizeof(remote_huge_buf);
//     }
// }

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
    volatile bool hot;
    volatile int64_t tmp = 0;
    chrono::time_point<chrono::steady_clock> start_time = chrono::steady_clock::now();
    if (hot_ratio_ >= 0.1 && hot_ratio_ <= 0.9) {
        while (!(*terminate) && cur_num_op < num_op) {
            // bool hot = (rand() % 10 < 9); // 0-8 hot, 9 cold
            hot = (cur_num_op % 10 < 9); 
            // volatile int8_t tmp = 0;
            access_page(tmp, pick_start(hot, thread_index), cur_num_op);
        }
    } else if (hot_ratio_ > 0.9) {
        while (!(*terminate) && cur_num_op < num_op) {
            // bool hot = (rand() % 10 >= 0);
            hot = (cur_num_op % 10 >= 0);
            // volatile int8_t tmp = 0;
            access_page(tmp, pick_start(hot, thread_index), cur_num_op);
        }
    }
    else { // hot_ratio is 0
        while (!(*terminate) && cur_num_op < num_op) {
            hot = (cur_num_op % 10 < 0); //always = false when there is no hot pages
            // volatile int8_t tmp = 0;
            access_page(tmp, pick_start(hot, thread_index), cur_num_op);
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

    logfile << "hot pages: ";
    for (int i = 0; i < 4; ++i)
        logfile << hot_pages[i] << " ";
    logfile << endl;

    logfile << "hot prefix: ";
    for (int i = 0; i < 4; ++i)
        logfile << hot_prefix_sum[i] << " ";
    logfile << endl;

    logfile << "cold pages: ";
    for (int i = 0; i < 4; ++i)
        logfile << cold_pages[i] << " ";
    logfile << endl;

    logfile << "cold prefix: ";
    for (int i = 0; i < 4; ++i)
        logfile << cold_prefix_sum[i] << " ";
    logfile << endl;

    logfile << endl;
}

void calculate_pages_local_remote(double split_ratio, int64_t local_size=ONE_NODE_MEM_SIZE, int64_t remote_size=ONE_NODE_MEM_SIZE) {
    int64_t whole_size = local_size + remote_size;

    small_pages = whole_size / DATA_SIZE * split_ratio;
    small_hot_pages = small_pages * hot_ratio_;
    small_cold_pages = small_pages - small_hot_pages;

    int64_t local_hot_mem = min(local_size, small_hot_pages * DATA_SIZE);
    local_hot_pages = local_hot_mem / DATA_SIZE;
    remote_hot_pages = max(int64_t(small_hot_pages - local_hot_pages), int64_t(0));

    huge_pages = ((1 - split_ratio) * whole_size) / HUGE_PAGE_SIZE;

    local_huge_pages = min((local_size - local_hot_mem) / HUGE_PAGE_SIZE, huge_pages);
    local_cold_pages = min((local_size - local_hot_mem - local_huge_pages * HUGE_PAGE_SIZE) / DATA_SIZE, small_cold_pages);
    remote_cold_pages = small_cold_pages - local_cold_pages;
    remote_huge_pages = huge_pages - local_huge_pages;

    local_huge_to_small_hot = HUGE_PAGE_SIZE / DATA_SIZE * hot_ratio_ * local_huge_pages;
    remote_huge_to_small_hot = HUGE_PAGE_SIZE / DATA_SIZE * hot_ratio_ * remote_huge_pages;
    local_huge_to_small_cold = HUGE_PAGE_SIZE / DATA_SIZE * (1 - hot_ratio_) * local_huge_pages;
    remote_huge_to_small_cold = HUGE_PAGE_SIZE / DATA_SIZE  * (1 - hot_ratio_) * remote_huge_pages;

    logfile << "small_pages: " <<
        small_pages << " small_hot: " << small_hot_pages << " small_cold:" << small_cold_pages << "\nlocal_hot: " 
        << local_hot_pages << " remote_hot: " << remote_hot_pages << " local_cold: " << local_cold_pages << " remote_cold: " << remote_cold_pages << "\nhuge_pages: " << huge_pages << " local_huge: "
        << local_huge_pages << " remote_huge: " <<remote_huge_pages << "\nlocal_huge_to_small_hot: " << local_huge_to_small_hot
        << " local_huge_to_small_cold: " << local_huge_to_small_cold << "\nremote_huge_to_small_hot: " 
        <<remote_huge_to_small_hot << " remote_huge_to_small_cold: " << remote_huge_to_small_cold << endl << endl;

    assert(small_pages * DATA_SIZE + huge_pages * HUGE_PAGE_SIZE <= whole_size);
    assert(small_pages * DATA_SIZE + huge_pages * HUGE_PAGE_SIZE >= int64_t(whole_size - (HUGE_PAGE_SIZE * 2 + DATA_SIZE * 2)));
    assert(local_hot_pages * DATA_SIZE + local_huge_pages * HUGE_PAGE_SIZE + local_cold_pages * DATA_SIZE <= local_size);
    assert(local_hot_pages * DATA_SIZE + local_huge_pages * HUGE_PAGE_SIZE + local_cold_pages * DATA_SIZE >= int64_t(local_size - HUGE_PAGE_SIZE));
    assert(remote_hot_pages * DATA_SIZE + remote_huge_pages * HUGE_PAGE_SIZE + remote_cold_pages * DATA_SIZE <= remote_size);
    // cout << int64_t(remote_size - HUGE_PAGE_SIZE) << endl;
    assert(remote_hot_pages * DATA_SIZE + remote_huge_pages * HUGE_PAGE_SIZE + remote_cold_pages * DATA_SIZE >= int64_t(remote_size - HUGE_PAGE_SIZE));
    update_hot_cold_pages();
}

// void calculate_pages(double split_ratio) {
//     small_pages = (split_ratio * ONE_NODE_MEM_SIZE * 2) / DATA_SIZE;
//     small_hot_pages = small_pages * hot_ratio_;
//     small_cold_pages = small_pages - small_hot_pages;

//     int64_t local_hot_mem = min(ONE_NODE_MEM_SIZE, small_hot_pages * DATA_SIZE);
//     local_hot_pages = local_hot_mem / DATA_SIZE;
//     remote_hot_pages = max(small_hot_pages - local_hot_pages, int64_t(0));

//     huge_pages = ((1 - split_ratio) * ONE_NODE_MEM_SIZE * 2) / HUGE_PAGE_SIZE;

//     local_huge_pages = min((ONE_NODE_MEM_SIZE - local_hot_mem) / HUGE_PAGE_SIZE, huge_pages);
//     local_cold_pages = min((ONE_NODE_MEM_SIZE - local_hot_mem - local_huge_pages * HUGE_PAGE_SIZE) / DATA_SIZE, small_cold_pages);
//     remote_cold_pages = small_cold_pages - local_cold_pages;
//     remote_huge_pages = huge_pages - local_huge_pages;

//     local_huge_to_small_hot = local_huge_pages * HUGE_PAGE_SIZE * hot_ratio_ / DATA_SIZE;
//     remote_huge_to_small_hot = remote_huge_pages * HUGE_PAGE_SIZE * hot_ratio_ / DATA_SIZE;

//     local_huge_to_small_cold = local_huge_pages * HUGE_PAGE_SIZE * (1 - hot_ratio_) / DATA_SIZE;
//     remote_huge_to_small_cold = remote_huge_pages * HUGE_PAGE_SIZE * (1 - hot_ratio_) / DATA_SIZE;

//     // logfile << "small_pages: " <<
//     //     small_pages << " small_hot: " << small_hot_pages << " small_cold:" << small_cold_pages << "\nlocal_hot: " 
//     //     << local_hot_pages << " remote_hot: " << remote_hot_pages << " local_cold: " << local_cold_pages << " remote_cold: " << remote_cold_pages << "\nhuge_pages: " << huge_pages << " local_huge: "
//     //     << local_huge_pages << " remote_huge: " <<remote_huge_pages << "\nlocal_huge_to_small_hot: " << local_huge_to_small_hot
//     //     << " local_huge_to_small_cold: " << local_huge_to_small_cold << "\nremote_huge_to_small_hot: " 
//     //     <<remote_huge_to_small_hot << " remote_huge_to_small_cold: " << remote_huge_to_small_cold << endl << endl;
    
//     assert(small_pages * DATA_SIZE + huge_pages * HUGE_PAGE_SIZE <= ONE_NODE_MEM_SIZE * 2);
//     assert(small_pages * DATA_SIZE + huge_pages * HUGE_PAGE_SIZE >= ONE_NODE_MEM_SIZE * 2 - HUGE_PAGE_SIZE * 2 - DATA_SIZE * 2);
//     assert(local_hot_pages * DATA_SIZE + local_huge_pages * HUGE_PAGE_SIZE + local_cold_pages * DATA_SIZE <= ONE_NODE_MEM_SIZE);
//     assert(local_hot_pages * DATA_SIZE + local_huge_pages * HUGE_PAGE_SIZE + local_cold_pages * DATA_SIZE >= ONE_NODE_MEM_SIZE - HUGE_PAGE_SIZE);
//     assert(remote_hot_pages * DATA_SIZE + remote_huge_pages * HUGE_PAGE_SIZE + remote_cold_pages * DATA_SIZE <= ONE_NODE_MEM_SIZE);
//     assert(remote_hot_pages * DATA_SIZE + remote_huge_pages * HUGE_PAGE_SIZE + remote_cold_pages * DATA_SIZE >= ONE_NODE_MEM_SIZE - HUGE_PAGE_SIZE);
    
//     update_hot_cold_pages();
// }

void main_experiment(long num_op, long num_thread, double split_ratio, double hot_ratio, int mode=0) {

    hot_ratio_ = hot_ratio;
    // printf("num_op is %ld, num_thread is %ld, split_ratio is %lf, hot_ratio is %lf\n",
    //        num_op, num_thread, split_ratio, hot_ratio);
    printf("%ld | %lf | %lf | %d ", num_op, split_ratio, hot_ratio_, mode);


    int ret;
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(0, &set);
    ret = sched_setaffinity(0, sizeof(set), &set);
    BUG_ON(ret != 0);

    // cout << int64_t(0) - int64_t(2) << " hhh" << endl; 

    thread *thread_arr = new thread[num_thread];
    double *result_arr = new double[num_thread];
    volatile bool terminate = false;

    if (mode == 2)
        calculate_pages_local_remote(split_ratio, ONE_NODE_MEM_SIZE, ONE_NODE_MEM_SIZE);
    else if (mode == 0)
        calculate_pages_local_remote(split_ratio, ONE_NODE_MEM_SIZE * 2, int64_t(0));
    else if(mode == 1)
        calculate_pages_local_remote(split_ratio, int64_t(0), ONE_NODE_MEM_SIZE * 2);
    
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
    // long num_ops[] = {500000, 1000000, 2000000, 2500000, 3000000, 3500000};
    // bool flag = (unsigned int)(0) <= (unsigned int)(2);
    // cout << flag << endl;
    
    BUG_ON(argc != 4);

    unsigned int split_idx= atoi(argv[1]), hot_idx = atoi(argv[2]), mode = atoi(argv[3]);
    // int op_idx= 0, hot_idx = atoi(argv[2]);
    // cout << split_idx << hot_idx << endl;
    // cout << "hello" << endl;
    BUG_ON(mode > 2);

    // cout << HUGE_PAGE_SIZE << " " << ONE_NODE_MEM_SIZE << " " << DATA_SIZE << " " << endl;
    long num_op, num_thread;
    double split_ratio, hot_ratio; // the maximum is 1

    num_op = 2000000, num_thread = 8, split_ratio = split_ratios[split_idx], hot_ratio = hot_ratios[hot_idx];
        // printf("%ld\n", ONE_NODE_MEM_SIZE / sizeof(int64_t));

    seeds = (unsigned int *)malloc(num_thread * sizeof(unsigned int));
    for (int i = 0; i < num_thread; ++i)
        seeds[i] = i + 1;

    logfile.open("log.txt");
    logfile << "mode: " << mode << " | " << "split_ratio: " << split_ratio 
        << " | " << "hot_ratio: " << hot_ratio << "\n"; 

    // cout << "start" << endl;

    main_experiment(num_op, num_thread, split_ratio, hot_ratio, mode);


    // for (int i = 0; i < 11; i += 2) {
    //     for (int j = 0; j < 11; j += 2) {
    //         num_op = 1000000, num_thread = 8, split_ratio = split_ratios[i], hot_ratio = hot_ratios[j];
    //         main_experiment(num_op, num_thread, split_ratio, hot_ratio);
    //     }
    // }

    // for (int i = 0; i < 30; ++i) {
    //         num_op = 1000000, num_thread = 8, split_ratio = 0.1, hot_ratio = 0.1;
    //     // printf("%ld\n", ONE_NODE_MEM_SIZE / sizeof(int64_t));
    //         main_experiment(num_op, num_thread, split_ratio, hot_ratio);
    // }

    logfile.close();


    return 0;
}
