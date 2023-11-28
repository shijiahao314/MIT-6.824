# test mapreduce
cd ./mapreduce/main
bash test-mr.sh > ../../logs/mapreduce.log 2>&1
cd ../../

cd ./shardkv
# test shardkv
if [ -f ../logs/shardkv.log ]; then
    echo "文件存在"
    rm ../logs/shardkv.log
fi
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