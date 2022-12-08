make

touch perf_stat_tem.txt
>perf_stat.txt

mul=0.100000

for i in {0..10..1}
do
        # for j in {0..10..1}
        # do
                # a=$((mul*i))
                # b=$((mul*j))
                # echo "$i | $j " >> perf_stat.txt
        sudo perf stat -o perf_stat_tem.txt \
                -e dTLB-load-misses,dTLB-store-misses,page-faults,major-faults,minor-faults\
                        ./test $i 3 0
        cat perf_stat_tem.txt >> perf_stat.txt
        # done
done