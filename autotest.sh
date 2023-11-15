# test shardkv
cd ./shardkv
rm ../logs/shardkv.log
for i in {1..100}
do
    echo "第 $i 次循环" >> ../logs/shardkv.log
    go test >> ../logs/shardkv.log
    if [ $? -ne 0 ]; then
        echo "错误"
        break
    fi
    echo "--------------------" >> ../logs/shardkv.log
    sleep 1
done
cd ..

# test others