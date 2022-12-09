make

touch perf_stat_tem.txt
>perf_stat.txt

for j in {2..10..2}
do
        for i in {5..10..2}
        do
                # sudo cat /proc/meminfo | grep PageTables >> perf_stat.txt
                sudo perf stat -o perf_stat_tem.txt \
                        -e mem_load_uops_l3_miss_retired.local_dram,mem_load_uops_l3_miss_retired.remote_dram,dTLB-load-misses,dTLB-store-misses,page-faults,dTLB-prefetch-misses\
                                ./test $i $j 2
                cat perf_stat_tem.txt | grep -E '_|-' >> perf_stat.txt
                echo -e "\n" >> perf_stat.txt
                # sudo perf stat -o perf_stat_tem.txt \
                #         -e MEM_LOAD_L3_MISS_RETIRED.REMOTE_DRAM,MEM_LOAD_L3_MISS_RETIRED.LOCAL_DRAM\
                #                 ./test $i 3 2
                # cat perf_stat_tem.txt >> perf_stat.txt
                # echo -e "\n" >> perf_stat.txt
                # done
        done
done