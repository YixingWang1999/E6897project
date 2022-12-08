make

> output.txt
# for m in {0..5..1}
# do
#         ./test 0 8 0 >> output.txt
# # done
# for m in {0..5..1}
# do
#         ./test 1 1 2 $m >> output.txt
# done

# for j in {0..10..1}
# do
#         for i in {0..10..1}
#         do
#                 ./test $i $j 2 >> output.txt
#         done
# done
for k in {0..2..1}
do
        for j in {0..10..1}
        do
                for i in {0..10..1}
                do
                        ./test $i $j $k >> output.txt
                done
        done
done