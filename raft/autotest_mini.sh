for i in {1..50}
do
    echo "第 $i 次循环"

    go test -run TestSnapshotInstallUnreliable2D > ./logs/tmp.log
    if [ $? -ne 0 ]; then
        echo "TestSnapshotInstallUnreliable2D错误"
        break
    fi

    sleep 1
done