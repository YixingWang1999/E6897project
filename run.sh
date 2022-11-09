make

> output.txt

for i in {0..10..2}
do
        for j in {0..10..2}
        do
        ./test $i $j >> output.txt
        done
done