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

class DeltaTable(object):
    """
        Main class for programmatically interacting with Delta tables.
        You can create DeltaTable instances using the path of the Delta table.::

            deltaTable = DeltaTable.forPath(spark, "/path/to/table")

        In addition, you can convert an existing Parquet table in place into a Delta table.::

            deltaTable = DeltaTable.convertToDelta(spark, "parquet.`/path/to/table`")

        .. versionadded:: 0.4

        .. note:: Evolving
    """

    def __init__(self, spark, jdt):
        self._spark = spark
        self._jdt = jdt

    @since(0.4)
    def toDF(self):
        """
        Get a DataFrame representation of this Delta table.

        .. note:: Evolving
        """
        return DataFrame(self._jdt.toDF(), self._spark._wrapped)

    @since(0.4)
    def alias(self, aliasName):
        """
        Apply an alias to the Delta table.

        .. note:: Evolving
        """
        jdt = self._jdt.alias(aliasName)
        return DeltaTable(self._spark, jdt)

    @since(0.5)
    def generate(self, mode):
        """
        Generate manifest files for the given delta table.

        :param mode: mode for the type of manifest file to be generated
                     The valid modes are as follows (not case sensitive):

                     - "symlink_format_manifest": This will generate manifests in symlink format
                                                  for Presto and Athena read support.

                     See the online documentation for more information.

        .. note:: Evolving
        """
        self._jdt.generate(mode)

    @since(0.4)
    def delete(self, condition=None):
        """
        Delete data from the table that match the given ``condition``.

        Example::

            deltaTable.delete("date < '2017-01-01'")        # predicate using SQL formatted string

            deltaTable.delete(col("date") < "2017-01-01")   # predicate using Spark SQL functions

        :param condition: condition of the update
        :type condition: str or pyspark.sql.Column

        .. note:: Evolving
        """
        if condition is None:
            self._jdt.delete()
        else:
            self._jdt.delete(self._condition_to_jcolumn(condition))

    @since(0.4)
    def update(self, condition=None, set=None):
        """
        Update data from the table on the rows that match the given ``condition``,
        which performs the rules defined by ``set``.

        Example::

            # condition using SQL formatted string
            deltaTable.update(
                condition = "eventType = 'clck'",
                set = { "eventType": "'click'" } )

            # condition using Spark SQL functions
            deltaTable.update(
                condition = col("eventType") == "clck",
                set = { "eventType": lit("click") } )

        :param condition: Optional condition of the update
        :type condition: str or pyspark.sql.Column
        :param set: Defines the rules of setting the values of columns that need to be updated.
                    *Note: This param is required.* Default value None is present to allow
                    positional args in same order across languages.
        :type set: dict with str as keys and str or pyspark.sql.Column as values

        .. note:: Evolving
        """
        jmap = self._dict_to_jmap(self._spark, set, "'set'")
        jcolumn = self._condition_to_jcolumn(condition)
        if condition is None:
            self._jdt.update(jmap)
        else:
            self._jdt.update(jcolumn, jmap)

    @since(0.4)
    def merge(self, source, condition):
        """
        Merge data from the `source` DataFrame based on the given merge `condition`. This returns
        a :class:`DeltaMergeBuilder` object that can be used to specify the update, delete, or
        insert actions to be performed on rows based on whether the rows matched the condition or
        not. See :class:`DeltaMergeBuilder` for a full description of this operation and what
        combinations of update, delete and insert operations are allowed.

        Example 1 with conditions and update expressions as SQL formatted string::

            deltaTable.alias("events").merge(
                source = updatesDF.alias("updates"),
                condition = "events.eventId = updates.eventId"
              ).whenMatchedUpdate(set =
                {
                  "data": "updates.data",
                  "count": "events.count + 1"
                }
              ).whenNotMatchedInsert(values =
                {
                  "date": "updates.date",
                  "eventId": "updates.eventId",
                  "data": "updates.data",
                  "count": "1"
                }
              ).execute()

        Example 2 with conditions and update expressions as Spark SQL functions::

            from pyspark.sql.functions import *

            deltaTable.alias("events").merge(
                source = updatesDF.alias("updates"),
                condition = expr("events.eventId = updates.eventId")
              ).whenMatchedUpdate(set =
                {
                  "data" : col("updates.data"),
                  "count": col("events.count") + 1
                }
              ).whenNotMatchedInsert(values =
                {
                  "date": col("updates.date"),
                  "eventId": col("updates.eventId"),
                  "data": col("updates.data"),
                  "count": lit("1")
                }
              ).execute()

        :param source: Source DataFrame
        :type source: pyspark.sql.DataFrame
        :param condition: Condition to match sources rows with the Delta table rows.
        :type condition: str or pyspark.sql.Column

        :return: builder object to specify whether to update, delete or insert rows based on
                 whether the condition matched or not
        :rtype: :py:class:`delta.tables.DeltaMergeBuilder`

        .. note:: Evolving
        """
        if source is None:
            raise ValueError("'source' in merge cannot be None")
        elif type(source) is not DataFrame:
            raise TypeError("Type of 'source' in merge must be DataFrame.")
        if condition is None:
            raise ValueError("'condition' in merge cannot be None")

        jbuilder = self._jdt.merge(
            source._jdf, self._condition_to_jcolumn(condition))
        return DeltaMergeBuilder(self._spark, jbuilder)

    @since(0.4)
    def vacuum(self, retentionHours=None):
        """
        Recursively delete files and directories in the table that are not needed by the table for
        maintaining older versions up to the given retention threshold. This method will return an
        empty DataFrame on successful completion.

        Example::

            deltaTable.vacuum()     # vacuum files not required by versions more than 7 days old

            deltaTable.vacuum(100)  # vacuum files not required by versions more than 100 hours old

        :param retentionHours: Optional number of hours retain history. If not specified, then the
                               default retention period of 168 hours (7 days) will be used.

        .. note:: Evolving
        """
        jdt = self._jdt
        if retentionHours is None:
            return DataFrame(jdt.vacuum(), self._spark._wrapped)
        else:
            return DataFrame(jdt.vacuum(float(retentionHours)), self._spark._wrapped)

    @since(0.4)
    def history(self, limit=None):
        """
        Get the information of the latest `limit` commits on this table as a Spark DataFrame.
        The information is in reverse chronological order.

        Example::

            fullHistoryDF = deltaTable.history()    # get the full history of the table

            lastOperationDF = deltaTable.history(1) # get the last operation

        :param limit: Optional, number of latest commits to returns in the history.
        :return: Table's commit history. See the online Delta Lake documentation for more details.
        :rtype: pyspark.sql.DataFrame

        .. note:: Evolving
        """
        jdt = self._jdt
        if limit is None:
            return DataFrame(jdt.history(), self._spark._wrapped)
        else:
            return DataFrame(jdt.history(limit), self._spark._wrapped)

    @classmethod
    @since(0.4)
    def convertToDelta(cls, sparkSession, identifier, partitionSchema=None):
        """
        Create a DeltaTable from the given parquet table. Takes an existing parquet table and
        constructs a delta transaction log in the base path of the table.
        Note: Any changes to the table during the conversion process may not result in a consistent
        state at the end of the conversion. Users should stop any changes to the table before the
        conversion is started.

        Example::

            # Convert unpartitioned parquet table at path 'path/to/table'
            deltaTable = DeltaTable.convertToDelta(
                spark, "parquet.`path/to/table`")

            # Convert partitioned parquet table at path 'path/to/table' and partitioned by
            # integer column named 'part'
            partitionedDeltaTable = DeltaTable.convertToDelta(
                spark, "parquet.`path/to/table`", "part int")

        :param sparkSession: SparkSession to use for the conversion
        :type sparkSession: pyspark.sql.SparkSession
        :param identifier: Parquet table identifier formatted as "parquet.`path`"
        :type identifier: str
        :param partitionSchema: Hive DDL formatted string, or pyspark.sql.types.StructType
        :return: DeltaTable representing the converted Delta table
        :rtype: :py:class:`~delta.tables.DeltaTable`

        .. note:: Evolving
        """
        assert sparkSession is not None
        if partitionSchema is None:
            jdt = sparkSession._sc._jvm.io.delta.tables.DeltaTable.convertToDelta(
                sparkSession._jsparkSession, identifier)
        else:
            if not isinstance(partitionSchema, str):
                partitionSchema = sparkSession._jsparkSession.parseDataType(
                    partitionSchema.json())
            jdt = sparkSession._sc._jvm.io.delta.tables.DeltaTable.convertToDelta(
                sparkSession._jsparkSession, identifier,
                partitionSchema)
        return jdt

    @classmethod
    @since(0.4)
    def forPath(cls, sparkSession, path):
        """
        Create a DeltaTable for the data at the given `path` using the given SparkSession.

        :param sparkSession: SparkSession to use for loading the table
        :type sparkSession: pyspark.sql.SparkSession
        :return: loaded Delta table
        :rtype: :py:class:`~delta.tables.DeltaTable`

        Example::

            deltaTable = DeltaTable.forPath(spark, "/path/to/table")

        .. note:: Evolving
        """
        assert sparkSession is not None
        jdt = sparkSession._sc._jvm.io.delta.tables.DeltaTable.forPath(
            sparkSession._jsparkSession, path)
        return DeltaTable(sparkSession, jdt)

    @classmethod
    @since(0.7)
    def forName(cls, sparkSession, tableOrViewName):
        """
        Create a DeltaTable using the given table or view name using the given SparkSession.

        :param sparkSession: SparkSession to use for loading the table
        :param tableOrViewName: name of the table or view
        :return: loaded Delta table
        :rtype: :py:class:`~delta.tables.DeltaTable`

        Example::

            deltaTable = DeltaTable.forName(spark, "tblName")

        .. note:: Evolving
        """
        assert sparkSession is not None
        jdt = sparkSession._sc._jvm.io.delta.tables.DeltaTable.forName(
            sparkSession._jsparkSession, tableOrViewName)
        return DeltaTable(sparkSession, jdt)

    @classmethod
    @since(0.4)
    def isDeltaTable(cls, sparkSession, identifier):
        """
        Check if the provided `identifier` string, in this case a file path,
        is the root of a Delta table using the given SparkSession.

        :param sparkSession: SparkSession to use to perform the check
        :param path: location of the table
        :return: If the table is a delta table or not
        :rtype: bool

        Example::

            DeltaTable.isDeltaTable(spark, "/path/to/table")

        .. note:: Evolving
        """
        assert sparkSession is not None
        return sparkSession._sc._jvm.io.delta.tables.DeltaTable.isDeltaTable(
            sparkSession._jsparkSession, identifier)

    @since(0.8)
    def upgradeTableProtocol(self, readerVersion, writerVersion):
        """
        Updates the protocol version of the table to leverage new features. Upgrading the reader
        version will prevent all clients that have an older version of Delta Lake from accessing
        this table. Upgrading the writer version will prevent older versions of Delta Lake to write
        to this table. The reader or writer version cannot be downgraded.

        See online documentation and Delta's protocol specification at PROTOCOL.md for more details.

        .. note:: Evolving
        """
        jdt = self._jdt
        if not isinstance(readerVersion, int):
            raise ValueError("The readerVersion needs to be an integer but got '%s'." %
                             type(readerVersion))
        if not isinstance(writerVersion, int):
            raise ValueError("The writerVersion needs to be an integer but got '%s'." %
                             type(writerVersion))
        jdt.upgradeTableProtocol(readerVersion, writerVersion)

    @classmethod
    def _dict_to_jmap(cls, sparkSession, pydict, argname):
        """
        convert dict<str, pColumn/str> to Map<str, jColumn>
        """
        # Get the Java map for pydict
        if pydict is None:
            raise ValueError("%s cannot be None" % argname)
        elif type(pydict) is not dict:
            e = "%s must be a dict, found to be %s" % (
                argname, str(type(pydict)))
            raise TypeError(e)

        jmap = sparkSession._sc._jvm.java.util.HashMap()
        for col, expr in pydict.items():
            if type(col) is not str:
                e = ("Keys of dict in %s must contain only strings with column names" % argname) + \
                    (", found '%s' of type '%s" % (str(col), str(type(col))))
                raise TypeError(e)
            if type(expr) is Column:
                jmap.put(col, expr._jc)
            elif type(expr) is str:
                jmap.put(col, functions.expr(expr)._jc)
            else:
                e = ("Values of dict in %s must contain only Spark SQL Columns " % argname) + \
                    "or strings (expressions in SQL syntax) as values, " + \
                    ("found '%s' of type '%s'" % (str(expr), str(type(expr))))
                raise TypeError(e)
        return jmap

    @classmethod
    def _condition_to_jcolumn(cls, condition, argname="'condition'"):
        if condition is None:
            jcondition = None
        elif type(condition) is Column:
            jcondition = condition._jc
        elif type(condition) is str:
            jcondition = functions.expr(condition)._jc
        else:
            e = ("%s must be a Spark SQL Column or a string (expression in SQL syntax)" % argname) \
                + ", found to be of type %s" % str(type(condition))
            raise TypeError(e)
        return jcondition


class DeltaMergeBuilder(object):
    """
    Builder to specify how to merge data from source DataFrame into the target Delta table.
    Use :py:meth:`delta.tables.DeltaTable.merge` to create an object of this class.
    Using this builder, you can specify 1, 2 or 3 ``when`` clauses of which there can be at most
    2 ``whenMatched`` clauses and at most 1 ``whenNotMatched`` clause.
    Here are the constraints on these clauses.

    - Constraints in the ``whenMatched`` clauses:

      - There can be at most one ``update`` action and one ``delete`` action in `whenMatched`
        clauses.

      - Each ``whenMatched`` clause can have an optional condition. However, if there are two
        ``whenMatched`` clauses, then the first one must have a condition.

      - When there are two ``whenMatched`` clauses and there are conditions (or the lack of)
        such that a row matches both clauses, then the first clause/action is executed.
        In other words, the order of the ``whenMatched`` clauses matter.

      - If none of the ``whenMatched`` clauses match a source-target row pair that satisfy
        the merge condition, then the target rows will not be updated or deleted.

      - If you want to update all the columns of the target Delta table with the
        corresponding column of the source DataFrame, then you can use the
        ``whenMatchedUpdateAll()``. This is equivalent to::

            whenMatchedUpdate(set = {
              "col1": "source.col1",
              "col2": "source.col2",
              ...    # for all columns in the delta table
            })

    - Constraints in the ``whenNotMatched`` clauses:

      - This clause can have only an ``insert`` action, which can have an optional condition.

      - If ``whenNotMatchedInsert`` is not present or if it is present but the non-matching
        source row does not satisfy the condition, then the source row is not inserted.

      - If you want to insert all the columns of the target Delta table with the
        corresponding column of the source DataFrame, then you can use
        ``whenNotMatchedInsertAll()``. This is equivalent to::

            whenMatchedInsert(values = {
              "col1": "source.col1",
              "col2": "source.col2",
              ...    # for all columns in the delta table
            })

    Example 1 with conditions and update expressions as SQL formatted string::

        deltaTable.alias("events").merge(
            source = updatesDF.alias("updates"),
            condition = "events.eventId = updates.eventId"
          ).whenMatchedUpdate(set =
            {
              "data": "updates.data",
              "count": "events.count + 1"
            }
          ).whenNotMatchedInsert(values =
            {
              "date": "updates.date",
              "eventId": "updates.eventId",
              "data": "updates.data",
              "count": "1"
            }
          ).execute()

    Example 2 with conditions and update expressions as Spark SQL functions::

        from pyspark.sql.functions import *

        deltaTable.alias("events").merge(
            source = updatesDF.alias("updates"),
            condition = expr("events.eventId = updates.eventId")
          ).whenMatchedUpdate(set =
            {
              "data" : col("updates.data"),
              "count": col("events.count") + 1
            }
          ).whenNotMatchedInsert(values =
            {
              "date": col("updates.date"),
              "eventId": col("updates.eventId"),
              "data": col("updates.data"),
              "count": lit("1")
            }
          ).execute()

    .. versionadded:: 0.4

    .. note:: Evolving
    """

    def __init__(self, spark, jbuilder):
        self._spark = spark
        self._jbuilder = jbuilder

    @since(0.4)
    def whenMatchedUpdate(self, condition=None, set=None):
        """
        Update a matched table row based on the rules defined by ``set``.
        If a ``condition`` is specified, then it must evaluate to true for the row to be updated.

        See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.

        :param condition: Optional condition of the update
        :type condition: str or pyspark.sql.Column
        :param set: Defines the rules of setting the values of columns that need to be updated.
                    *Note: This param is required.* Default value None is present to allow
                    positional args in same order across languages.
        :type set: dict with str as keys and str or pyspark.sql.Column as values
        :return: this builder

        .. note:: Evolving
        """
        jset = DeltaTable._dict_to_jmap(
            self._spark, set, "'set' in whenMatchedUpdate")
        new_jbuilder = self.__getMatchedBuilder(condition).update(jset)
        return DeltaMergeBuilder(self._spark, new_jbuilder)

    @since(0.4)
    def whenMatchedUpdateAll(self, condition=None):
        """
        Update all the columns of the matched table row with the values of the  corresponding
        columns in the source row. If a ``condition`` is specified, then it must be
        true for the new row to be updated.

        See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.

        :param condition: Optional condition of the insert
        :type condition: str or pyspark.sql.Column
        :return: this builder

        .. note:: Evolving
        """
        new_jbuilder = self.__getMatchedBuilder(condition).updateAll()
        return DeltaMergeBuilder(self._spark, new_jbuilder)

    @since(0.4)
    def whenMatchedDelete(self, condition=None):
        """
        Delete a matched row from the table only if the given ``condition`` (if specified) is
        true for the matched row.

        See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.

        :param condition: Optional condition of the delete
        :type condition: str or pyspark.sql.Column
        :return: this builder

        .. note:: Evolving
        """
        new_jbuilder = self.__getMatchedBuilder(condition).delete()
        return DeltaMergeBuilder(self._spark, new_jbuilder)

    @since(0.4)
    def whenNotMatchedInsert(self, condition=None, values=None):
        """
        Insert a new row to the target table based on the rules defined by ``values``. If a
        ``condition`` is specified, then it must evaluate to true for the new row to be inserted.

        See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.

        :param condition: Optional condition of the insert
        :type condition: str or pyspark.sql.Column
        :param values: Defines the rules of setting the values of columns that need to be updated.
                       *Note: This param is required.* Default value None is present to allow
                       positional args in same order across languages.
        :type values: dict with str as keys and str or pyspark.sql.Column as values
        :return: this builder

        .. note:: Evolving
        """
        jvalues = DeltaTable._dict_to_jmap(
            self._spark, values, "'values' in whenNotMatchedInsert")
        new_jbuilder = self.__getNotMatchedBuilder(condition).insert(jvalues)
        return DeltaMergeBuilder(self._spark, new_jbuilder)

    @since(0.4)
    def whenNotMatchedInsertAll(self, condition=None):
        """
        Insert a new target Delta table row by assigning the target columns to the values of the
        corresponding columns in the source row. If a ``condition`` is specified, then it must
        evaluate to true for the new row to be inserted.

        See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.

        :param condition: Optional condition of the insert
        :type condition: str or pyspark.sql.Column
        :return: this builder

        .. note:: Evolving
        """
        new_jbuilder = self.__getNotMatchedBuilder(condition).insertAll()
        return DeltaMergeBuilder(self._spark, new_jbuilder)

    @since(0.4)
    def execute(self):
        """
        Execute the merge operation based on the built matched and not matched actions.

        See :py:class:`~delta.tables.DeltaMergeBuilder` for complete usage details.

        .. note:: Evolving
        """
        self._jbuilder.execute()

    def __getMatchedBuilder(self, condition=None):
        if condition is None:
            return self._jbuilder.whenMatched()
        else:
            return self._jbuilder.whenMatched(DeltaTable._condition_to_jcolumn(condition))

    def __getNotMatchedBuilder(self, condition=None):
        if condition is None:
            return self._jbuilder.whenNotMatched()
        else:
            return self._jbuilder.whenNotMatched(DeltaTable._condition_to_jcolumn(condition))



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
        table_name = self.topic_name.split('.')[2]

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

    topic = params.get('topic_name')

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



