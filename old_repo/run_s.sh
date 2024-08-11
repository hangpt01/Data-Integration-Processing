spark-submit \
    --archives myenv.tar.gz#myenv \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./myenv/bin/python \
    --conf spark.executorEnv.PYSPARK_PYTHON=./myenv/bin/python \
    your_script.py