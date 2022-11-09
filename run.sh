make

> output.txt

for i in {0..10..1}
do
        for j in {0..10..1}
        do
        ./test $i $j >> output.txt
        done
done