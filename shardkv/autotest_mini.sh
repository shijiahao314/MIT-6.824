# test shardkv
for i in {1..100}
do
    echo "第 $i 次循环" > ./logs/tmp.log
    go test -run TestUnreliable2 >> ./logs/tmp.log
    if [ $? -ne 0 ]; then
        echo "错误"
        break
    fi
    sleep 1
done
cd ..

# test others