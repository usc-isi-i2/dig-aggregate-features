Running the Aggregator
--------------------------------
{code}
zip -r reducers.zip reducers features utils
{code}

The program to run is: featureReducer.py. It takes as argument the location of the data and aggregator:featureName

{code}
./bin/spark-submit \
    --master local[*] \
    --py-files ~/github/dig-aggregate-features/reducers.zip \
    ~/github/dig-aggregate-features/featureReducer.py \
    hdfs://memex-nn1:8020/user/worker/process/atf/dev04/evolution-forums/user/part-r-00020 \
    hdfs://memex-nn1:8020/user/worker/aggregate-features/histogram_date_02 \
    histogram:fromUser date:enrollment_date
{code}

Available aggregators:

  - histogram: aggregated value is each value from the post with a count of how many times it appears
  - date: aggregated value is the date range as the featureValue