# 20210624_GOJE
Restaurant - Data Pipeline

#### 20210624_GOJE - Restaurant - Data Pipeline

Data Pipeline for processing restaurant analytics like user and restaurant information, interaction details etc.

###### **Tech Stack:**

* Spark
* Scala

###### **Usage:**

Below parameters should be configured based on Job
* **spark.app.name:** Set this property to Job Name Provide job name to run
* **spark.driver.memory**
* **spark.executor.cores**
* **spark.executor.instances**
* **spark.executor.memory**
* **spark.executor.memoryOverhead**
* **spark.sql.shuffle.partitions**: Set this property based on executor instances and executor cores. 
                                        As best practice, set this property to (executor_cores*executor_instances)

Below are Application configuration properties
* **user.details.fileName**: Path for userDetail.json file
* **place.info.fileName**: Path for placeDetails.json file
* **spark.yarn.queue**: Set queue name here 

**Spark Command:**
```bash
spark-submit \
--conf "spark.executor.extraJavaOptions=-XX:ReservedCodeCacheSize=100M -XX:CompressedClassSpaceSize=256m -XX:MaxDirectMemorySize=4096m -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=30 -XX:+ScavengeBeforeFullGC -XX:+CMSScavengeBeforeRemark" \
--conf "spark.driver.extraJavaOptions=-XX:ReservedCodeCacheSize=100M -XX:MaxMetaspaceSize=256m -XX:CompressedClassSpaceSize=256m -XX:MaxDirectMemorySize=4096m -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+UseCMSInitiatingOccupancyOnly -XX:CMSInitiatingOccupancyFraction=30 -XX:+ScavengeBeforeFullGC -XX:+CMSScavengeBeforeRemark" \
--class com.playstation.test.batch.driver.Driver \
--master yarn \
--conf spark.app.name=run_entire_flow \
--conf spark.checkpoint.compress=true \
--conf spark.driver.memory=4G \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.dynamicAllocation.minExecutors=10 \
--conf spark.dynamicAllocation.initialExecutors=10 \
--conf spark.executor.cores=4 \
--conf spark.executor.memory=1G \
--conf spark.executor.memoryOverhead=1024 \
--conf spark.hadoop.orc.overwrite.output.file=true \
--conf spark.io.compression.codec=zstd \
--conf spark.kryoserializer.buffer.max=2047m \
--conf spark.kryoserializer.buffer=1024k \
--conf spark.locality.wait=1s \
--conf spark.memory.offHeap.enabled=true \
--conf spark.memory.offHeap.size=1G \
--conf spark.rdd.compress=true \
--conf spark.reducer.maxReqsInFlight=1 \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.shuffle.compress=true \
--conf spark.speculation=true \
--conf spark.sql.orc.enableVectorizedReader=true \
--conf spark.sql.orc.filterPushdown=true \
--conf spark.sql.orc.impl=native \
--conf spark.sql.shuffle.partitions=400 \
--conf spark.hadoop.orc.overwrite.output.file=true \
--conf spark.ui.killEnabled=true \
--conf spark.ui.showConsoleProgress=true \
--conf spark.yarn.preserve.staging.files=false \
--conf spark.yarn.queue=<queue-name> \
--driver-class-path <jar-name>
```

###### ETL Pipeline Overview:
There are multiple jobs in pipeline. These job may run in parallel. Below are the jobs
* **run_pipeline:** This job will trigger entire etl workflow in sequence

* **user_details_ingestion:** This job will read given json file(userDetails.json) and ingest data into Datalake 
* **place_details_ingestion:** This job will read given json file(placeDetails.json) and ingest data into Datalake 
* **user_details_transform:** This job will process user details ingestion table and create final data set good for analytic needs
* **restaurant_info_transform:** This job will process place details ingestion table and create final dataset for analytic needs
* **restaurant_intr_transform:** This job will process restaurant & user interaction information and create partitioned table
* **question_1:** This job will provide top 3 restaurants for each cuisine type within given date range
* **question_2:** This job will provide top nth record for each cuisine type based on sale_amt
* **question_3:** This job will return avg visit time for a user_id for a given date


###### Contact:
For more information, please reach out to Krishna Goje
_**krishna19.gk@gmail.com**_

###### **Contributors:** 

* Krishna Goje