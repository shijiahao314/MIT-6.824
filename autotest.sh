# var
absolute_path=$(realpath .)
logs_dir="$absolute_path/logs"
mapreduce_log="$logs_dir/mapreduce"
raft_log="$logs_dir/raft"
kvraft_log="$logs_dir/kvraft"
shardkv_log="$logs_dir/shardkv"
# lab 2/3/4 测试次数
test_n=100

# # 1.mapreduce
# cd "$absolute_path/mapreduce/main"
# mr_filename="$mapreduce_log.log"
# if [ -f "$mr_filename" ]; then
#     rm "$mr_filename"
# fi
# bash test-mr.sh > "$mr_filename" 2>&1
# if [ $? -eq 0 ]; then
#     # 成功则修改文件名
#     mv "$mr_filename" "${mapreduce_log}-pass.log"
# else
#     # 失败则删除文件
#     rm "$mr_filename"
# fi

# func for test lab 2/3/4
lab_test() {
    path=$1
    log_name=$2
    n=$3
    filename="${log_name}.log"
    # cd
    cd $path
    # 日志文件存在则删除
    if [ -f $filename ]; then
        rm $filename
    fi
    # 循环测试
    for i in $(seq 1 $n); do
        echo "第 $i 次循环" >> $filename
        go test >> $filename
        # 测试未通过
        if [ $? -ne 0 ]; then
            echo "错误" >> $filename
            break
        fi
        echo "--------------------" >> $filename
        # 判断是否完成
        if [ $i -eq $n ]; then
            mv $filename "${log_name}-pass${n}.log"
        fi
    done
    if [ -f $filename ]; then
        rm $filename
    fi
}

# 2.raft
lab_test "${absolute_path}/raft" $raft_log $test_n

# 3.kvraft
lab_test "${absolute_path}/raft" $kvraft_log $test_n

# 4.shardkv
lab_test "${absolute_path}/shardkv" $shardkv_log $test_n

# test finish
echo "All tests finish!"