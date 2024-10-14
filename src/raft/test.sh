#!/bin/bash

# 初始化计数器
count=1

# 持续运行 go test，直到返回值为 1
while true
do
    # 打印当前执行次数
    echo "第 $count 次执行"

    # 执行测试并获取返回值
    go test
    result=$?

    # 增加执行次数计数器
    count=$((count+1))

    # 如果返回值为 1，则退出循环
    if [ $result -eq 1 ]; then
        echo "测试失败，退出。共执行 $count 次。"
        break
    fi
done
