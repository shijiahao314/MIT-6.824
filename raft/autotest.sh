for i in {1..50}
do
    echo "第 $i 次循环"

    echo "Test2A: $i " >> ./logs/2A.log
    go test -run 2A >> ./logs/2A.log
    if [ $? -ne 0 ]; then
        echo "2A错误"
        break
    fi
    echo "" >> ./logs/2A.log

    echo "Test2B: $i " >> ./logs/2B.log
    go test -run 2B >> ./logs/2B.log
    if [ $? -ne 0 ]; then
        echo "2B错误"
        break
    fi
    echo "" >> ./logs/2B.log

    echo "Test2C: $i " >> ./logs/2C.log
    go test -run 2C >> ./logs/2C.log
    if [ $? -ne 0 ]; then
        echo "2C错误"
        break
    fi
    echo "" >> ./logs/2C.log

    echo "Test2D: $i " >> ./logs/2D.log
    go test -run 2D >> ./logs/2D.log
    if [ $? -ne 0 ]; then
        echo "2D错误"
        break
    fi
    echo "" >> ./logs/2D.log

    sleep 10
done