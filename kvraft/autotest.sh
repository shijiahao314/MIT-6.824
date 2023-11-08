for i in {1..100}
do
    echo "第 $i 次循环" >> ./logs/3B.log

    go test -run 3B >> ./logs/3B.log
    if [ $? -ne 0 ]; then
        echo "3错误"
        break
    fi

    sleep 1
done