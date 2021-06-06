# Copyright (2020) The Delta Lake Project Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from typing import List
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, to_timestamp, date_format, from_unixtime, unix_timestamp
from pyspark.sql.types import *
from pyspark.sql import Window
import pyspark
import logging
import json
import datetime
import sys
import os
from pyspark import since
from pyspark.sql import Column, DataFrame, functions


class DeltaLakeProcessing():
    """
    This class process data from Debezium with Kafa to S3 
    and store this table in DeltaLake format

    Data has 4 main format

    - after: State after transaction
    - before: Statete before transaction
    - op: Operation of data change
        - r: fullload
        - c: insert
        - u: update
        - d: delete
    - ts_ms: timestamp of when de data was set to kafka topic
    """

    def __init__(
        self,
        processing_prefix_path: str,
        staged_prefix_path: str,
        curated_path: str,
        glue_database: str,
        topic_name: str,
        primary_keys: List[str],
        timestamp_cols: List[str],
        ref_time_column: str,
        partition: str,
        big_table: bool
    ):
        self.processing_prefix_path = processing_prefix_path
        self.staged_prefix_path = staged_prefix_path
        self.curated_path = curated_path
        self.glue_database = glue_database

        self.df_upsert = None
        self.df_delete = None

        self.topic_name = topic_name
        self.primary_keys = primary_keys
        self.timestamp_cols = timestamp_cols

        self.ref_time_column = ref_time_column
        self.partition = partition
        self.big_table = big_table

        self.spark = SparkSession.builder \
            .appName("ProcessingCDCDeltaLake") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
            .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED") \
            .config("spark.eventLog.enabled", "false") \
            .enableHiveSupport() \
            .getOrCreate()

        
        # DEV MODE
        # self.spark.sparkContext.setLogLevel('WARN')

        self.sqlContext = SQLContext(self.spark.sparkContext)
        self.logger = self.spark.sparkContext._jvm.org.apache.log4j.LogManager.getLogger(
            __name__)


    def read_from_processing_stage(self, format_file: str = 'parquet'):
        """Will read data from prefix kafka/cdc/{schema}/{topic_name}.

        Args:
            format_file (str, optional): Can be avro, csv, json, etc. Defaults to 'parquet'.

        Raises:
            pyspark.sql.utils.AnalysisException: If don't have data in CDC to process

        Returns:
            bool: True if data of Debezium is in DeltaLake. 
            False if data of Debezium is in DeltaLake annd jump to next step of Amazon EMR
        """

      
        try:
            path = self.processing_prefix_path + '/' +  self.topic_name.split('.')[1] + '/' + self.topic_name
            df_cdc = self.spark.read.format(format_file).load(path)

            df_upsert = df_cdc.filter(col("op").isin(
                ['r', 'c', 'u'])).select("after.*", "op", "ts_ms")
            df_delete = df_cdc.filter(col("op").isin(
                ['d'])).select("before.*", "op", "ts_ms")

            w = Window.partitionBy(self.primary_keys)

            # Get ts_ms with max timestamp using primary keys as reference
            df_cdc = df_upsert.union(df_delete).withColumn('tmp_ts_ms_max', max('ts_ms').over(w))\
                .where(col('ts_ms') == col('tmp_ts_ms_max'))\
                .drop('tmp_ts_ms_max')


            self.df_upsert = df_cdc.filter(
                col("op").isin(['r', 'c', 'u'])).drop("op")
            self.df_delete = df_cdc.filter(col("op").isin(['d'])).drop("op")
            self.logger.info("Debezium's CDC files loaded.")

        # If table doesn't exists in Debizium pass to next step in ENR.
        except pyspark.sql.utils.AnalysisException as error:
            if 'Unable to infer schema for Parquet' in error.args[0] or 'Path does not exist' in error.args[0]:
                self.logger.info(f"There is no data in {path}.")
                return False

            else:
                raise error

        return True

    

        


    def process_upsert_and_delete(self):
        """Process data from cdc to tables deltalake in bukect staged

        Raises:
            error:If table doesn't exists at staged buekct in format Deltalake create a new one.
        """

        path = f"{self.staged_prefix_path}/{self.topic_name.split('.')[1]}/{self.topic_name}"
        table_name = topic_maped

        string_exp = ""
        for pk in self.primary_keys:
            string_exp += f"`{table_name}`.{pk} = stored.{pk}" if string_exp == "" else f" AND `{table_name}`.{pk} = stored.{pk}"

        try:
            df_test = self.spark.read.format("delta").load(path)
            df_test.unpersist()

            # https://databricks.com/blog/2020/05/19/schema-evolution-in-merge-operations-and-operational-metrics-in-delta-lake.html
            DeltaTable.forPath(sparkSession=self.spark, path=path).alias(table_name).merge(
                self.df_upsert.alias("stored"),
                string_exp
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            self.logger.info(f"Upsert processed in {path}.")

            
            DeltaTable.forPath(sparkSession=self.spark, path=path).alias(table_name).merge(
                self.df_delete.alias("stored"),
                string_exp
            ).whenMatchedDelete().execute()
            self.logger.info(f"Delete processed in {path}.")

            self.df_upsert.unpersist()
            self.df_delete.unpersist()

        except pyspark.sql.utils.AnalysisException as error:
            if 'is not a Delta table' in error.args[0]:
                self.df_upsert.write.format(
                    "delta").mode("overwrite").save(path)
                self.logger.info(f"Create deltaTable in {path}")
            else:
                raise error

    def show_history(self):
        "Get DeltaLake table in path and show history"
        path = f"{self.staged_prefix_path}/{self.topic_name.split('.')[1]}/{self.topic_name}"
        deltaTable = DeltaTable.forPath(sparkSession=self.spark, path=path)
        deltaTable.history().show()
        self.logger.info(f"Finished show history")

    def process_cdc_data(self):
        """True if data of Debezium is in DeltaLake. 
            False if data of Debezium is not in DeltaLake and jump to next step of Amazon EMR
        """        
        if self.read_from_processing_stage():
            self.process_upsert_and_delete()
            self.logger.info(f"Show history")
            delta_lake.show_history()
        else:
            self.logger.info(f"Jump Upsert and delete step.")

    def process_curated(self):
        """Get data from staged path, If table exist overwrite table, else create a new one

        Raises:
            error: If isn't a DeltaLake table or data doesn't exist in path 
        """        
        path = f"{self.staged_prefix_path}/{self.topic_name.split('.')[1]}/{self.topic_name}"

        def getTablesOnGlue(glue_database: str):
            """Get tables of Glue database created with Terraform

            Args:
                glue_database (str): Glue Database of file in staged path

            Returns:
                list: List with tabels of Glue database 
            """            
            response = []
            for x in self.sqlContext.sql(f"show tables in {glue_database}").rdd.collect():
                response.append(x.tableName)
            return response

        try:
            df = self.spark.read.format("delta").load(path)
            glue_table_name = self.topic_name.replace('.', '_')

            if glue_table_name in getTablesOnGlue(self.glue_database):
                self.logger.info(f"writing data of {path}.")
                df.write.mode("overwrite").insertInto(
                    f'{self.glue_database}.{glue_table_name}')
            else:
                if self.big_table:
                    self.logger.info(f"writing data of {path}.")
                    df.write.partitionBy(self.primary_keys).saveAsTable(f'{self.glue_database}.{glue_table_name}')
                else:
                    self.logger.info(f"writing data of {path}.")
                    df.write.saveAsTable(f'{self.glue_database}.{glue_table_name}')
            

            compute_statistics(self.spark, self.glue_database , glue_table_name)

        except pyspark.sql.utils.AnalysisException as error:
            if 'is not a Delta table' in error.args[0]:
                self.logger.info(f"There is no data in {path}")
            else:
                raise error
        
        

if __name__ == '__main__':
    if len(sys.argv) <= 1:
        raise Exception("There are no argments")
    # Get parameters of Lambda
    params = sys.argv[1]
    params = json.loads(params)
    print(params)

    topic_list = params.get('topic_name')

    for topic in topic_list:  
        delta_lake = DeltaLakeProcessing(
            processing_prefix_path=params.get("processing_prefix_path"),
            staged_prefix_path=params.get("staged_prefix_path"),
            curated_path=params.get("curated_path"),
            glue_database=params.get("glue_database"),
            topic_name=topic,
            primary_keys=params.get("primary_keys"),
            timestamp_cols=params.get("timestamp_cols"),
            ref_time_column=params.get("ref_time_column"),
            partition=params.get("partition"),
            big_table=params.get("big_table")
        )
        delta_lake.process_cdc_data()
        
    delta_lake.process_curated()



