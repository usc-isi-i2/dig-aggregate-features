To run:

Running the Histogram Aggregator
--------------------------------
zip -r reducers.zip reducers

Then submit the spark job:

./bin/spark-submit --py-files ~/github/dig-aggregate-features/reducers.zip ~/github/dig-aggregate-features/histogram-features.py hdfs://memex-nn1:8020/user/worker/process/atf/dev01/evolution-forums/user/part-r-00000 hdfs://memex-nn1:8020/user/worker/test/dev05 fromUser

The program to run is: histogram-reducers.py. It takes as argument the location of the data and comma separated list of feature names that should be aggregated
