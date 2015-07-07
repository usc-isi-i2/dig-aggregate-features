Running the Aggregator
--------------------------------
```
zip -r reducers.zip reducers features utils
```

The program to run is: <code>featureReducer.py</code>. 

```
./bin/spark-submit --py-files reducers.zip featureReducer.py \
      <input folder> <output folder> <child attribute name> [<aggregation>:<featureName> ...]
```

Example Invocation:

```
./bin/spark-submit \
    --master local[*] \
    --py-files ~/github/dig-aggregate-features/reducers.zip \
    ~/github/dig-aggregate-features/featureReducer.py \
    hdfs://memex-nn1:8020/user/worker/process/atf/dev11/evolution-forums/main/part-r-00000 \
    hdfs://memex-nn1:8020/user/worker/aggregate-features/v2_4 \
    hasPost date:dateCreated histogram:author.name
```

Available aggregators:

  - histogram: aggregated value is each value from the post with a count of how many times it appears
  - date: aggregated value is the date range as the featureValue
  
  
It also sorts all Posts under a Thread in the order created (sort by dateCreated ASC)
