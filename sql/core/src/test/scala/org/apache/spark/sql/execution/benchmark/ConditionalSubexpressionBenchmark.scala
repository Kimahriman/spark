/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// package org.apache.spark.sql.execution.benchmark

// import org.apache.spark.benchmark.Benchmark
// import org.apache.spark.sql.Dataset
// import org.apache.spark.sql.functions.{col, concat, lit, regexp_extract, when}
// import org.apache.spark.sql.internal.SQLConf
// import org.apache.spark.storage.StorageLevel

// /**
//  * Benchmark for measuring perf of subexpression elimination on conditional expressions.
//  *
//  * This benchmark toggles:
//  *   spark.sql.subexpressionElimination.conditionals.enabled
//  *
//  * and uses a repeated regular expression expression in a conditional projection.
//  *
//  * To run this benchmark:
//  * {{{
//  *   1. without sbt:
//  *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
//  *   2. build/sbt "sql/Test/runMain <this class>"
//  *   3. generate result:
//  *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
//  *      Results will be written to "benchmarks/ConditionalSubexpressionBenchmark-results.txt".
//  * }}}
//  */
// object ConditionalSubexpressionBenchmark extends SqlBasedBenchmark {
//   private val N = 5 * 1000 * 1000

//   private def runCaseWhenRegexBenchmark(groupName: String, df: Dataset[_]): Unit = {
//     val benchmark = new Benchmark(groupName, N, output = output)

//     val extracted = regexp_extract(col("s"), "(\\d+)", 1)
//     val conditionalRegexExpr = when(extracted =!= lit(""), extracted)

//     df.persist(StorageLevel.MEMORY_ONLY).count()

//     benchmark.addCase("conditionals disabled", 10) { _ =>
//       withSQLConf(SQLConf.SUBEXPRESSION_ELIMINATION_CONDITIONALS_ENABLED.key -> "false") {
//         df.select(conditionalRegexExpr.alias("value")).noop()
//       }
//     }

//     benchmark.addCase("conditionals enabled", 10) { _ =>
//       withSQLConf(SQLConf.SUBEXPRESSION_ELIMINATION_CONDITIONALS_ENABLED.key -> "true") {
//         df.select(conditionalRegexExpr.alias("value")).noop()
//       }
//     }

//     benchmark.run()

//     df.unpersist()
//   }

//   override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
//     val alwaysMatchDf = spark.range(N).select(
//       concat(lit("spark-"), (col("id") % 1000).cast("string"), lit("-sql")).alias("s"))
//     runCaseWhenRegexBenchmark(
//       "Conditional regex subexpression elimination (CASE always matches)",
//       alwaysMatchDf)

//     val neverMatchDf = spark.range(N).select(lit("spark-no-digits-sql").alias("s"))
//     runCaseWhenRegexBenchmark(
//       "Conditional regex subexpression elimination (CASE never matches)",
//       neverMatchDf)
//   }
// }
