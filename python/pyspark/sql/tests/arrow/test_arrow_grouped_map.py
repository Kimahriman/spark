#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import inspect
import os
import time
from typing import Iterator
import unittest

from pyspark.errors import PythonException
from pyspark.sql import Row
from pyspark.sql.functions import array, col, explode, lit, mean, stddev
from pyspark.sql.window import Window
from pyspark.testing.sqlutils import (
    ReusedSQLTestCase,
    have_pyarrow,
    pyarrow_requirement_message,
)
from pyspark.testing.utils import QuietTest


if have_pyarrow:
    import pyarrow as pa
    import pyarrow.compute as pc


def function_variations(func):
    yield func
    num_args = len(inspect.getfullargspec(func).args)
    if num_args == 1:

        def iter_func(batches):
            yield from func(pa.Table.from_batches(batches)).to_batches()

        yield iter_func
    else:

        def iter_keys_func(keys, batches):
            yield from func(keys, pa.Table.from_batches(batches)).to_batches()

        yield iter_keys_func


@unittest.skipIf(
    not have_pyarrow,
    pyarrow_requirement_message,  # type: ignore[arg-type]
)
class GroupedMapInArrowTests(ReusedSQLTestCase):
    @property
    def data(self):
        return (
            self.spark.range(10)
            .toDF("id")
            .withColumn("vs", array([lit(i) for i in range(20, 30)]))
            .withColumn("v", explode(col("vs")))
            .drop("vs")
        )

    @classmethod
    def setUpClass(cls):
        ReusedSQLTestCase.setUpClass()

        # Synchronize default timezone between Python and Java
        cls.tz_prev = os.environ.get("TZ", None)  # save current tz if set
        tz = "America/Los_Angeles"
        os.environ["TZ"] = tz
        time.tzset()

        cls.sc.environment["TZ"] = tz
        cls.spark.conf.set("spark.sql.session.timeZone", tz)

    @classmethod
    def tearDownClass(cls):
        del os.environ["TZ"]
        if cls.tz_prev is not None:
            os.environ["TZ"] = cls.tz_prev
        time.tzset()
        ReusedSQLTestCase.tearDownClass()

    def test_apply_in_arrow(self):
        def func(group):
            assert isinstance(group, pa.Table)
            assert group.schema.names == ["id", "value"]
            return group

        df = self.spark.range(10).withColumn("value", col("id") * 10)
        grouped_df = df.groupBy((col("id") / 4).cast("int"))
        expected = df.collect()

        for func_variation in function_variations(func):
            actual = grouped_df.applyInArrow(func_variation, "id long, value long").collect()
            self.assertEqual(actual, expected)

    def test_apply_in_arrow_with_key(self):
        def func(key, group):
            assert isinstance(key, tuple)
            assert all(isinstance(scalar, pa.Scalar) for scalar in key)
            assert isinstance(group, pa.Table)
            assert group.schema.names == ["id", "value"]
            assert all(
                (pc.divide(k, pa.scalar(4)).cast(pa.int32()),) == key for k in group.column("id")
            )
            return group

        df = self.spark.range(10).withColumn("value", col("id") * 10)
        grouped_df = df.groupBy((col("id") / 4).cast("int"))
        expected = df.collect()

        for func_variation in function_variations(func):
            actual2 = grouped_df.applyInArrow(func_variation, "id long, value long").collect()
            self.assertEqual(actual2, expected)

    def test_apply_in_arrow_empty_groupby(self):
        df = self.data

        def normalize(table):
            v = table.column("v")
            return table.set_column(
                1, "v", pc.divide(pc.subtract(v, pc.mean(v)), pc.stddev(v, ddof=1))
            )

        for func_variation in function_variations(normalize):
            # casting doubles to floats to get rid of numerical precision issues
            # when comparing Arrow and Spark values
            actual = (
                df.groupby()
                .applyInArrow(func_variation, "id long, v double")
                .withColumn("v", col("v").cast("float"))
                .sort("id", "v")
            )
            windowSpec = Window.partitionBy()
            expected = df.withColumn(
                "v",
                ((df.v - mean(df.v).over(windowSpec)) / stddev(df.v).over(windowSpec)).cast(
                    "float"
                ),
            )
            self.assertEqual(actual.collect(), expected.collect())

    def test_apply_in_arrow_not_returning_arrow_table(self):
        df = self.data

        def stats(key, _):
            return key

        def stats_iter(key, _):
            yield key

        with QuietTest(self.sc):
            with self.assertRaisesRegex(
                PythonException,
                "Return type of the user-defined function should be pyarrow.Table, but is tuple",
            ):
                df.groupby("id").applyInArrow(stats, schema="id long, m double").collect()

            with self.assertRaisesRegex(
                PythonException,
                "Return type of the user-defined function should be pyarrow.RecordBatch, but is "
                + "tuple",
            ):
                df.groupby("id").applyInArrow(stats_iter, schema="id long, m double").collect()

    def test_apply_in_arrow_returning_wrong_types(self):
        df = self.data

        for schema, expected in [
            ("id integer, v integer", "column 'id' \\(expected int32, actual int64\\)"),
            (
                "id integer, v long",
                "column 'id' \\(expected int32, actual int64\\), "
                "column 'v' \\(expected int64, actual int32\\)",
            ),
            ("id long, v long", "column 'v' \\(expected int64, actual int32\\)"),
            ("id long, v string", "column 'v' \\(expected string, actual int32\\)"),
        ]:
            with self.subTest(schema=schema):
                with QuietTest(self.sc):
                    for func_variation in function_variations(lambda table: table):
                        with self.assertRaisesRegex(
                            PythonException,
                            f"Columns do not match in their data type: {expected}",
                        ):
                            df.groupby("id").applyInArrow(func_variation, schema=schema).collect()

    def test_apply_in_arrow_returning_wrong_types_positional_assignment(self):
        df = self.data

        for schema, expected in [
            ("a integer, b integer", "column 'a' \\(expected int32, actual int64\\)"),
            (
                "a integer, b long",
                "column 'a' \\(expected int32, actual int64\\), "
                "column 'b' \\(expected int64, actual int32\\)",
            ),
            ("a long, b long", "column 'b' \\(expected int64, actual int32\\)"),
            ("a long, b string", "column 'b' \\(expected string, actual int32\\)"),
        ]:
            with self.subTest(schema=schema):
                with self.sql_conf(
                    {"spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName": False}
                ):
                    with QuietTest(self.sc):
                        for func_variation in function_variations(lambda table: table):
                            with self.assertRaisesRegex(
                                PythonException,
                                f"Columns do not match in their data type: {expected}",
                            ):
                                df.groupby("id").applyInArrow(
                                    func_variation, schema=schema
                                ).collect()

    def test_apply_in_arrow_returning_wrong_column_names(self):
        df = self.data

        def stats(key, table):
            # returning three columns
            return pa.Table.from_pydict(
                {
                    "id": [key[0].as_py()],
                    "v": [pc.mean(table.column("v")).as_py()],
                    "v2": [pc.stddev(table.column("v")).as_py()],
                }
            )

        with QuietTest(self.sc):
            for func_variation in function_variations(stats):
                with self.assertRaisesRegex(
                    PythonException,
                    "Column names of the returned pyarrow.Table do not match specified schema. "
                    "Missing: m. Unexpected: v, v2.\n",
                ):
                    # stats returns three columns while here we set schema with two columns
                    df.groupby("id").applyInArrow(
                        func_variation, schema="id long, m double"
                    ).collect()

    def test_apply_in_arrow_returning_empty_dataframe(self):
        df = self.data

        def odd_means(key, table):
            if key[0].as_py() % 2 == 0:
                return pa.table([])
            else:
                return pa.Table.from_pydict(
                    {"id": [key[0].as_py()], "m": [pc.mean(table.column("v")).as_py()]}
                )

        schema = "id long, m double"
        for func_variation in function_variations(odd_means):
            actual = (
                df.groupby("id").applyInArrow(func_variation, schema=schema).sort("id").collect()
            )
            expected = [Row(id=id, m=24.5) for id in range(1, 10, 2)]
            self.assertEqual(expected, actual)

    def test_apply_in_arrow_returning_empty_dataframe_and_wrong_column_names(self):
        df = self.data

        def odd_means(key, table):
            if key[0].as_py() % 2 == 0:
                return pa.table([[]], names=["id"])
            else:
                return pa.Table.from_pydict(
                    {"id": [key[0].as_py()], "m": [pc.mean(table.column("v")).as_py()]}
                )

        with QuietTest(self.sc):
            with self.assertRaisesRegex(
                PythonException,
                "Column names of the returned pyarrow.Table do not match specified schema. "
                "Missing: m.\n",
            ):
                # stats returns one column for even keys while here we set schema with two columns
                df.groupby("id").applyInArrow(odd_means, schema="id long, m double").collect()

    def test_apply_in_arrow_column_order(self):
        df = self.data
        grouped_df = df.groupby("id")
        expected = df.select(df.id, (df.v * 3).alias("u"), df.v).collect()

        # Function returns a table with required column names but different order
        def change_col_order(table):
            return table.append_column("u", pc.multiply(table.column("v"), 3))

        for func_variation in function_variations(change_col_order):
            # The result should assign columns by name from the table
            result = (
                grouped_df.applyInArrow(func_variation, "id long, u long, v int")
                .sort("id", "v")
                .select("id", "u", "v")
                .collect()
            )
            self.assertEqual(expected, result)

    def test_positional_assignment_conf(self):
        with self.sql_conf(
            {"spark.sql.legacy.execution.pandas.groupedMap.assignColumnsByName": False}
        ):

            def foo(_):
                return pa.Table.from_pydict({"x": ["hi"], "y": [1]})

            df = self.data
            for func_variation in function_variations(foo):
                result = (
                    df.groupBy("id")
                    .applyInArrow(func_variation, "a string, b long")
                    .select("a", "b")
                    .collect()
                )
                for r in result:
                    self.assertEqual(r.a, "hi")
                    self.assertEqual(r.b, 1)

    def test_apply_in_arrow_batching(self):
        with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": 2}):

            def func(group):
                assert isinstance(group, Iterator)
                batches = list(group)
                assert len(batches) == 2
                for batch in batches:
                    assert isinstance(batch, pa.RecordBatch)
                    assert batch.schema.names == ["id", "value"]
                yield from batches

            df = self.spark.range(12).withColumn("value", col("id") * 10)
            grouped_df = df.groupBy((col("id") / 4).cast("int"))

            actual = grouped_df.applyInArrow(func, "id long, value long").collect()
            self.assertEqual(actual, df.collect())

    def test_apply_in_arrow_partial_iteration(self):
        with self.sql_conf({"spark.sql.execution.arrow.maxRecordsPerBatch": 2}):

            def func(group: Iterator[pa.RecordBatch]):
                first = next(group)
                yield pa.RecordBatch.from_pylist(
                    [{"value": r.as_py() % 4} for r in first.column(0)]
                )

            df = self.spark.range(20)
            grouped_df = df.groupBy((col("id") % 4).cast("int"))

            # Should get two records for each group
            expected = [Row(value=x) for x in [0, 0, 1, 1, 2, 2, 3, 3]]

            actual = grouped_df.applyInArrow(func, "value long").collect()
            self.assertEqual(actual, expected)


if __name__ == "__main__":
    from pyspark.sql.tests.arrow.test_arrow_grouped_map import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
