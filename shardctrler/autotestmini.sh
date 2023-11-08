rm ./logs/test.log

for i in {1..50}
do
    echo "第 $i 次循环" >> ./logs/test.log

    go test
    if [ $? -ne 0 ]; then
        echo "第 $i 次循环，不通过，退出" >> ./logs/test.log
        break
    fi

    echo "第 $i 次循环，通过" >> ./logs/test.log
    echo "" >> ./logs/test.log

    sleep 0
done