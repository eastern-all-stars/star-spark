# star-spark

This project is greatly inspired by [airstream of Airbnb](https://www.slideshare.net/databricks/building-data-product-based-on-apache-spark-at-airbnb-with-jingwei-lu-and-liyin-tang) and [S2Job](https://github.com/apache/incubator-s2graph/tree/master/s2jobs).

You can define a Spark job without writing even one line of Java/Scala codes. What you only need to do is just to write a job description file(json format).

## QuickStart

### Step 1. write a job description file.

**Example**

hive_source_example.json

```json

{
    "name": "sampleJob",
    "source": [
        {
            "name": "hiveSource",
            "inputs": [],
            "type": "hive",
            "options": {
            	"sql": "select * from hive_user"
            }
        }
    ],
    "process": [
        {
            "name": "transform",
            "inputs": ["hiveSource"],
            "type": "sql",
            "options": {
                "sql": "select * from hiveSource WHERE id = 1"
            }
        }
    ],
    "sink": [
        {
            "name": "hdfs_sink",
            "inputs": ["transform"],
            "type": "hdfs",
            "options": {
                "path": "/data/test",
                "format": "parquet"
            }
        }
    ]
}

```

### Step 2. run it with spark

```shell
spark-submit --class com.easternallstars.star.spark.job.JobLauncher --master yarn --deploy-mode client star-spark.jar hive_source_example.json
```

Have a cup of coffee and wait util the job is over.

You can also upload job description file and jar to hdfs, and let Oozie schedule the spark job.





