#!/bin/bash

# 路径设置
export PYTHONPATH=${ROOT_DIR}
# 本地路径设置
DATA_DIR="${ROOT_DIR}/data"
MODEL_DIR="${ROOT_DIR}/model"
TMP_DIR="${ROOT_DIR}/tmp"
LOG_DIR="${ROOT_DIR}/logs/dynamic_rank"

if [ ! -d "$DATA_DIR" ]; then
    mkdir -p "$DATA_DIR"
    echo "Created directory: $DATA_DIR"
fi

if [ ! -d "$MODEL_DIR" ]; then
    mkdir -p "$MODEL_DIR"
    echo "Created directory: $MODEL_DIR"
fi

if [ ! -d "$TMP_DIR" ]; then
    mkdir -p "$TMP_DIR"
    echo "Created directory: $TMP_DIR"
fi

if [ ! -d "$LOG_DIR" ]; then
    mkdir -p "$LOG_DIR"
    echo "Created directory: $LOG_DIR"
fi

export HADOOP_ROOT_LOGGER=ERROR,console
HDFS_BASE_DIR="hdfs:/usr/hive/warehouse/yyz_rec_prd.db/algo_reco_rank"
HDFS_PYTHON_PATH="hdfs:/usr/hive/warehouse/yyz_rec_prd.db/algo_reco_rank/algo_reco_rank.tar.gz"
HDFS_UDF_DIR="hdfs:/usr/hive/warehouse/yyz_rec_prd.db/algo_reco_rank/udf"

#本地调试使用
#HDFS_BASE_DIR="file:/home/zhangquan/algo_reco_rank/src/samples"

