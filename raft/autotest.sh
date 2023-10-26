while true; do
    # 运行你的命令
    go test -run 2C > ./logs/tmp.log

    # 检查返回值
    if [ $? -ne 0 ]; then
        break
    fi

    # 可选：等待一段时间再继续
    sleep 1
done
