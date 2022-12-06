make

> output.txt
# for m in {0..5..1}
# do
#         ./test 0 8 0 >> output.txt
# done
# # ./test $0 $0 $0 >> output.txt
for k in {0..2..1}
do
        for i in {0..10..2}
        do
                for j in {0..10..2}
                do
                        ./test $i $j $k >> output.txt
                done
        done
done