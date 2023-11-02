for i in {1..50}
do
    echo "第 $i 次循环" >> ./logs/3A.log

    go test -run 3A >> ./logs/3A.log
    if [ $? -ne 0 ]; then
        echo "3A错误"
        break
    fi

    sleep 1
done