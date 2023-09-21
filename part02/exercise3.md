# Exercise 3: the streaming dataset with a Master Dataset enrichment

Enrich the job created before with a Master Dataset stored as JSON files under `/tmp/wfc/workshop/master/` directory. 

The keys responsible for the join are respectively:

* `value` for the Apache Kafka dataset
* `nr` for the Master Dataset

The enrichment process should keep the rows when they don't have matching values in the Master Dataset.

Besides, you can reduce the processing time trigger to `30 seconds` so that the join happens more often.

<details>
<summary>Hints - Master Dataset definition</summary>

It's a static dataset, so we use the Spark SQL API here:
```
sparkSession.read.schema("nr STRING, label STRING").json("/tmp/wfc/workshop/master")
```

Remember, for any kind of semi-structured data sources (JSON, CSV), it's important to explicitly define the schema. Otherwise Apache Spark will
sample the dataset to infer the schema which in the end can be wrong and the operation can be costly (dataset processed twice, for the schema resolution
and processing logic)
</details>

## Setup instructions
1. Run the following command to prepare the master dataset:
```
DATASET_DIR=/tmp/wfc/workshop/master/
mkdir -p $DATASET_DIR
echo '{"nr": "1", "label": "Number one"}
{"nr": "2", "label": "Number two"}' > "$DATASET_DIR/1_2.json"


echo '{"nr": "3", "label": "Number three"}
{"nr": "4", "label": "Number four"}' > "$DATASET_DIR/3_4.json"
```
2. Implement the job.
3. Start the data generator.
4. Start the job.
5. Add a new file to the Master Dataset:
```

echo '{"nr": "5", "label": "Number three"}
{"nr": "6", "label": "Number four"}' > "$DATASET_DIR/5_6.json"
```

6. Wait for the records number 5 and 6 to be joined and check the results of the job. You should see no matched rows because of the files index cache.
7. Rewrite the existing files of the job:
```
echo '{"nr": "1", "label": "ONE"}
{"nr": "2", "label": "TWO"}' > "$DATASET_DIR/1_2.json"

echo '{"nr": "3", "label": "THREE"}
{"nr": "4", "label": "FOUR"}' > "$DATASET_DIR/3_4.json"
```

7. Wait for the joins for 1, 2, 3, or 4 happening. You should see the rows joined with the updated JSONs.

*The refresh issue is present only for the raw data sources. Modern table file formats like Delta Lake don't have it.*

# Well done! 
⏭️ [start the next exercise](exercise4.md)
