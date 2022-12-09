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
#include <stdio.h>
#include <stdlib.h>
#include <sys/wait.h>

const int64_t HUGE_PAGE_SIZE = 1L << 21L; // 2MB
const int64_t ONE_NODE_MEM_SIZE = 1L << 33L;  // 8G
const int64_t DATA_SIZE = 1L << 12L; // 4K
const int64_t ONE_NODE_CPU_SETS = 10L;
const int64_t SKIP_SIZE = 1L;
const int HOT_COLD_ACCESS_RATIO = 100;
const double EPS = 1e-3;

using namespace std;

double hot_ratio_;
unsigned int *seeds;
int64_t *local_hot_buf, *local_cold_buf, *local_huge_buf;
int64_t *remote_hot_buf, *remote_cold_buf, *remote_huge_buf;
int64_t hot_4ks_in_huge, cold_4ks_in_huge;
int64_t small_pages, small_cold_pages, small_hot_pages;
int64_t local_hot_pages, remote_hot_pages, local_cold_pages, remote_cold_pages; // all referencing to 4K pages
int64_t huge_pages, local_huge_pages, remote_huge_pages;
int64_t hot_pages[4], cold_pages[4];
int64_t hot_prefix_sum[4], cold_prefix_sum[4];
int64_t local_huge_to_small_hot, local_huge_to_small_cold;
int64_t remote_huge_to_small_hot, remote_huge_to_small_cold;
int64_t *hot_buf_concat[ONE_NODE_MEM_SIZE * 2 / DATA_SIZE] = {NULL};
int64_t *cold_buf_concat[ONE_NODE_MEM_SIZE * 2 / DATA_SIZE] = {NULL};
double suggested_local_remote_access_ratio;
ofstream perf_stat;
// ofstream logfile;

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
    return ptr;
}

void inline access_page(volatile int64_t &tmp, int64_t *start, long &cur_num_op) {
    if (!start)
        return;
    // cout << "1" << endl;
    tmp += start[0];
    // cout << tmp << endl;
    // cout << "2" << endl;
    cur_num_op += 1;
}


inline int64_t *pick_start(bool hot, int thread_index, long num_op) {
    if ((hot && hot_ratio_ > EPS) || hot_ratio_ > 1 - EPS){ // in case hot_ratio_ is 0 or hot_ratio_ is 1
        return hot_buf_concat[num_op % hot_prefix_sum[3]];
    } else if((!hot && hot_ratio_ < 1 - EPS) || hot_ratio_ < EPS) {
        // in case hot_ratio_ == 0 or hot_ratio_ = 1
        return cold_buf_concat[num_op % cold_prefix_sum[3]];
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
    bool hot;
    volatile int64_t tmp = 0;
    int64_t *start = NULL; // in case it is opimized
    chrono::time_point<chrono::steady_clock> start_time = chrono::steady_clock::now();
    while (!(*terminate) && cur_num_op < num_op) {
            hot = (cur_num_op % (HOT_COLD_ACCESS_RATIO + 1) < HOT_COLD_ACCESS_RATIO);
            start = pick_start(hot, thread_index, cur_num_op);
            access_page(tmp, start, cur_num_op);
    }
    // if (hot_ratio_ >= 0.1 && hot_ratio_ <= 0.9) {
    //     while (!(*terminate) && cur_num_op < num_op) {
    //         // bool hot = (rand() % 10 < 9); // 0-8 hot, 9 cold
    //         hot = (cur_num_op % 20 < 19);
    //         // volatile int8_t tmp = 0;
    //         start = pick_start(hot, thread_index, cur_num_op);
    //         access_page(tmp, start, cur_num_op);
    //     }
    // } else if (hot_ratio_ > 0.9) {
    //     while (!(*terminate) && cur_num_op < num_op) {
    //         // bool hot = (rand() % 10 >= 0);
    //         hot = (cur_num_op % 20 >= 0);
    //         // volatile int8_t tmp = 0;
    //         start = pick_start(hot, thread_index, cur_num_op);
    //         access_page(tmp, start, cur_num_op);
    //     }
    // }
    // else { // hot_ratio is 0
    //     while (!(*terminate) && cur_num_op < num_op) {
    //         hot = (cur_num_op % 20 < 0); //always = false when there is no hot pages
    //         // volatile int8_t tmp = 0;
    //         start = pick_start(hot, thread_index, cur_num_op);
    //         access_page(tmp, start, cur_num_op);
    //     }
    // }

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

    long local_accesses, remote_accesses;
    local_accesses = hot_prefix_sum[1] * HOT_COLD_ACCESS_RATIO + cold_prefix_sum[1];
    remote_accesses = (hot_prefix_sum[3] - hot_prefix_sum[1]) * HOT_COLD_ACCESS_RATIO
                        + cold_prefix_sum[3] - cold_prefix_sum[1];
    // cout << local_accesses << remote_accesses;
    suggested_local_remote_access_ratio = double(local_accesses) / double(remote_accesses);
    perf_stat.open("./perf_stat.txt", ios_base::app);
    perf_stat << "suggested ratio: " << suggested_local_remote_access_ratio << endl;
    perf_stat.close();
    //     logfile << "hot pages: ";
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

    hot_4ks_in_huge = HUGE_PAGE_SIZE / DATA_SIZE * hot_ratio_;
    cold_4ks_in_huge = HUGE_PAGE_SIZE / DATA_SIZE - hot_4ks_in_huge;

    local_huge_to_small_hot = hot_4ks_in_huge * local_huge_pages;
    remote_huge_to_small_hot = hot_4ks_in_huge * remote_huge_pages;
    local_huge_to_small_cold = cold_4ks_in_huge * local_huge_pages;
    remote_huge_to_small_cold = cold_4ks_in_huge * remote_huge_pages;

    // logfile << "small_pages: " <<
    //     small_pages << " small_hot: " << small_hot_pages << " small_cold:" << small_cold_pages << "\nlocal_hot: " 
    //     << local_hot_pages << " remote_hot: " << remote_hot_pages << " local_cold: " << local_cold_pages << " remote_cold: " << remote_cold_pages << "\nhuge_pages: " << huge_pages << " local_huge: "
    //     << local_huge_pages << " remote_huge: " <<remote_huge_pages << "\nlocal_huge_to_small_hot: " << local_huge_to_small_hot
    //     << " local_huge_to_small_cold: " << local_huge_to_small_cold << "\nremote_huge_to_small_hot: " 
    //     <<remote_huge_to_small_hot << " remote_huge_to_small_cold: " << remote_huge_to_small_cold << endl << endl;

    assert(small_pages * DATA_SIZE + huge_pages * HUGE_PAGE_SIZE <= whole_size);
    assert(small_pages * DATA_SIZE + huge_pages * HUGE_PAGE_SIZE >= int64_t(whole_size - (HUGE_PAGE_SIZE * 2 + DATA_SIZE * 2)));
    assert(local_hot_pages * DATA_SIZE + local_huge_pages * HUGE_PAGE_SIZE + local_cold_pages * DATA_SIZE <= local_size);
    assert(local_hot_pages * DATA_SIZE + local_huge_pages * HUGE_PAGE_SIZE + local_cold_pages * DATA_SIZE >= int64_t(local_size - HUGE_PAGE_SIZE));
    assert(remote_hot_pages * DATA_SIZE + remote_huge_pages * HUGE_PAGE_SIZE + remote_cold_pages * DATA_SIZE <= remote_size);
    // cout << int64_t(remote_size - HUGE_PAGE_SIZE) << endl;
    assert(remote_hot_pages * DATA_SIZE + remote_huge_pages * HUGE_PAGE_SIZE + remote_cold_pages * DATA_SIZE >= int64_t(remote_size - HUGE_PAGE_SIZE));
    update_hot_cold_pages();
}

void buffer_concatenation(bool hot) {
    size_t size = hot ? hot_prefix_sum[3] : cold_prefix_sum[3];
    // int64_t *concated_buffer[size] = NULL;

    int i = 0;
    int64_t j;
    int64_t hot_region_size = hot_4ks_in_huge * DATA_SIZE;
    // cout << "hot_region_size: " << hot_region_size << endl;
    // hot_region_size += DATA_SIZE - hot_region_size % DATA_SIZE;
    if (hot) {
        // hot_buf_concat_size = size;
        for (j = 0; i < hot_prefix_sum[0] && j < local_hot_pages * DATA_SIZE; ++i, j+=DATA_SIZE) // local 4k hot
            hot_buf_concat[i] = &local_hot_buf[j/sizeof(int64_t)];
        // logfile << "diff1" << i - hot_prefix_sum[0] << endl;
        // i = hot_prefix_sum[0];
        // cout << i << " " << j << endl;
        for (j = 0; i < hot_prefix_sum[1] && j < local_huge_pages * HUGE_PAGE_SIZE; ++i, j+=DATA_SIZE) {// local 2M hot part
            if ((j % HUGE_PAGE_SIZE) >= hot_region_size) {// if it is beyond hot region
                // jump to the start of next huge page
                j += HUGE_PAGE_SIZE - j % HUGE_PAGE_SIZE - DATA_SIZE;
                i -= 1;
                continue;
            }
            hot_buf_concat[i] = &local_huge_buf[j/sizeof(int64_t)];
        }
        // logfile << "diff2" << i - hot_prefix_sum[1] << endl;
        // i = hot_prefix_sum[1];
        // cout << i << " " << j << endl;
        for (j=0; i < hot_prefix_sum[2] && j < remote_hot_pages * DATA_SIZE; ++i, j+=DATA_SIZE)
            hot_buf_concat[i] = &remote_hot_buf[j/sizeof(int64_t)];
        // logfile << "diff3" << i - hot_prefix_sum[2] << endl;
        // i = hot_prefix_sum[2];
        // cout << i << " " << j << endl;
        for (j = 0; i < hot_prefix_sum[3] && j < remote_huge_pages * HUGE_PAGE_SIZE; ++i, j+=DATA_SIZE) {// remote 2M hot part
            if ((j % HUGE_PAGE_SIZE) >= hot_region_size) {// if it is beyond hot region
                // jump to the start of next huge page
                j += HUGE_PAGE_SIZE - j % HUGE_PAGE_SIZE - DATA_SIZE;
                i -= 1;
                continue;
            }
            hot_buf_concat[i] = &remote_huge_buf[j/sizeof(int64_t)];
        }
        // logfile << "diff4" << i - hot_prefix_sum[3] << endl;
        hot_prefix_sum[3] = i;
    } else {
        // cold_buf_concat_size = size;
        for (j = 0; i < cold_prefix_sum[0] && j < local_cold_pages * DATA_SIZE; ++i, j+=DATA_SIZE) // local 4k hot
            cold_buf_concat[i] = &local_cold_buf[j/sizeof(int64_t)];
        // cout << i << " " << j << endl;
        // logfile << "diff1: " << i - cold_prefix_sum[0] << endl;
        // i = cold_prefix_sum[0];
        for (j = hot_region_size; i < cold_prefix_sum[1] && j < local_huge_pages * HUGE_PAGE_SIZE; ++i, j+=DATA_SIZE) {// local 2M hot part
            if (hot_region_size != 0 && (j % HUGE_PAGE_SIZE) == 0) {// if it is beyond cold region
                j += hot_region_size - DATA_SIZE; // jump to the start of next huge page's cold region
                i -= 1;
                continue;
            }
            cold_buf_concat[i] = &local_huge_buf[j/sizeof(int64_t)];
        }
        // cout << i << " " << j << endl;
        // logfile << "diff2: " << i - cold_prefix_sum[1] << endl;
        // i = cold_prefix_sum[1];
        for (j=0; i < cold_prefix_sum[2] && j < remote_cold_pages * DATA_SIZE; ++i, j+=DATA_SIZE)
            cold_buf_concat[i] = &remote_cold_buf[j/sizeof(int64_t)];
        // cout << i << " " << j << endl;
        // logfile << "diff3: " << i - cold_prefix_sum[2] << endl;
        // i = cold_prefix_sum[2];
        for (j = hot_region_size; i < cold_prefix_sum[3] && j < remote_huge_pages * HUGE_PAGE_SIZE; ++i, j+=DATA_SIZE) {// remote 2M hot part
            if (hot_region_size != 0 && (j % HUGE_PAGE_SIZE) == 0) {// if it is beyond hot region
                j += hot_region_size - DATA_SIZE; // jump to the start of next huge page
                i -= 1;
                continue;
            }
            cold_buf_concat[i] = &remote_huge_buf[j/sizeof(int64_t)];
        }
        // logfile << "diff4: " << i - cold_prefix_sum[3] << endl;
        cold_prefix_sum[3] = i;
    }
}

void inspect_page_table_size(bool before_alloc, double split_ratio, double hot_ratio, int mode) {
    if (before_alloc) {
        perf_stat.open("./perf_stat.txt", ios_base::app);
        perf_stat << split_ratio << " | " << hot_ratio \
            << " | " << mode << endl;
    }
    char command1[100];
    char command3[] = "sudo grep AnonHugePages /proc/meminfo >> ./perf_stat.txt ";
    char command2[] = "sudo cat /proc/meminfo | grep PageTables >> ./perf_stat.txt";
    sprintf(command1, "sudo cat /proc/%ld/status | grep VmPTE >> ./perf_stat.txt", long(getpid()));
    // sprintf(command2, "sudo cat /proc/%ld/status | grep VmPTE >> ./perf_stat.txt", long(getpid()));

    pid_t pid1 = fork();
    if (pid1 < 0) {
            printf("fork error\n");
            return;
    } else if (pid1 == 0) {
            system(command2);
            // sleep(3);
            if (!before_alloc) {
                sleep(1);
                system(command3);
                sleep(1);
                system(command1);
                // sleep(3);
            }
            _exit(0);
    } else {
            wait(NULL);
            // kill(pid1, SIGKILL);
    }

    if (!before_alloc)
        perf_stat.close();
}

void main_experiment(long num_op, long num_thread, double split_ratio, double hot_ratio, int mode=0) {

    hot_ratio_ = hot_ratio;
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
    
    // inspect_page_table_size(true, split_ratio, hot_ratio, mode);

    local_hot_buf = mem_alloc_set_numa(local_hot_pages, 0, false);
    local_cold_buf = mem_alloc_set_numa(local_cold_pages, 0, false);
    local_huge_buf = mem_alloc_set_numa(local_huge_pages, 0, true);
    remote_hot_buf = mem_alloc_set_numa(remote_hot_pages, 1, false);
    remote_cold_buf = mem_alloc_set_numa(remote_cold_pages, 1, false);
    remote_huge_buf = mem_alloc_set_numa(remote_huge_pages, 1, true);

    // inspect_page_table_size(false, split_ratio, hot_ratio, mode);
    
    buffer_concatenation(true);
    buffer_concatenation(false);

    // logfile << local_hot_buf <<"\t" << local_huge_buf << "\t"<< remote_hot_buf << "\t"<< remote_huge_buf << "\t"<< endl;

    // for (int i = 0; i < hot_prefix_sum[3]; ++i)
    //     logfile << hot_buf_concat[i] << " ";
    // logfile << endl;
    // int i = 0;
    // logfile << "hh" << endl;
    // for (; i < hot_prefix_sum[0]; ++i)
    //     logfile << hot_buf_concat[i] << " ";
    // logfile << endl;

    // logfile << "hh" << endl;
    // for (; i < hot_prefix_sum[1]; ++i)
    //     logfile << hot_buf_concat[i] << " ";
    // logfile << endl;

    // logfile << "hh" << endl;
    // for (; i < hot_prefix_sum[2]; ++i)
    //     logfile << hot_buf_concat[i] << " ";
    // logfile << endl;

    // logfile << "hh" << endl;
    // for (; i < hot_prefix_sum[3]; ++i)
    //     logfile << hot_buf_concat[i] << " ";
    // logfile << endl;
    // logfile << local_cold_buf <<"\t" << local_huge_buf << "\t"<< remote_cold_buf << "\t"<< remote_huge_buf << "\t"<< endl;

    // for (int i = 0; i < hot_prefix_sum[3]; ++i)
    //     logfile << hot_buf_concat[i] << " ";
    // logfile << endl;
    // int i = 0;
    // logfile << "hh" << endl;
    // for (; i < cold_prefix_sum[0]; ++i)
    //     logfile << cold_buf_concat[i] << " ";
    // logfile << endl;

    // logfile << "hh" << endl;
    // for (; i < cold_prefix_sum[1]; ++i)
    //     logfile << cold_buf_concat[i] << " ";
    // logfile << endl;

    // logfile << "hh" << endl;
    // for (; i < cold_prefix_sum[2]; ++i)
    //     logfile << cold_buf_concat[i] << " ";
    // logfile << endl;

    // logfile << "hh" << endl;
    // for (; i < cold_prefix_sum[3]; ++i)
    //     logfile << cold_buf_concat[i] << " ";
    // logfile << endl;

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
    // inspect_page_table_size(false, split_ratio, hot_ratio, mode);
    // printf("overall throughput: %f GB/s\n", (sum * DATA_SIZE) / (1L << 30L));
    perf_stat.open("./perf_stat.txt", ios_base::app);
    perf_stat << DATA_SIZE / sizeof(int64_t) << endl;
    perf_stat.close();
    printf("| %f\n", (sum * sizeof(int64_t) * 8) / (1L << 30L));
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
    // long num_ops[] = {5000000, 10000000, 20000000, 25000000, 30000000, 35000000};
    
    BUG_ON(argc != 4);

    unsigned int split_idx= atoi(argv[1]), hot_idx = atoi(argv[2]), mode = atoi(argv[3]);
    BUG_ON(mode > 2);

    long num_op, num_thread;
    double split_ratio, hot_ratio; // the maximum is 1

    num_op = 50000000, num_thread = 8, split_ratio = split_ratios[split_idx], hot_ratio = hot_ratios[hot_idx] / 8;

    seeds = (unsigned int *)malloc(num_thread * sizeof(unsigned int));
    for (int i = 0; i < num_thread; ++i)
        seeds[i] = i + 1;

    // logfile.open("log.txt");
    // logfile << "mode: " << mode << " | " << "split_ratio: " << split_ratio 
    //     << " | " << "hot_ratio: " << hot_ratio << "\n"; 
    main_experiment(num_op, num_thread, split_ratio, hot_ratio, mode);

    // logfile.close();


    return 0;
}
