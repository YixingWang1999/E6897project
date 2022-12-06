make

> output.txt

# ./test 0 0 1 >> output.txt
# # ./test $0 $0 $0 >> output.txt
for k in {0..2..1}
do
        for i in {0..1..1}
        do
                for j in {0..1..1}
                do
                        ./test $i $j $k >> output.txt
                done
        done
done