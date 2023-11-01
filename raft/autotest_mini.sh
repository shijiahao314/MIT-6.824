for i in {1..50}
do
    echo "第 $i 次循环"

    go test -run TestRPCBytes2B > ./logs/tmp.log
    if [ $? -ne 0 ]; then
        echo "TestRPCBytes2B错误"
        break
    fi

    sleep 1
done