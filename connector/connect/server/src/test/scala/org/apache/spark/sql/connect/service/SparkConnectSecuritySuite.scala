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

package org.apache.spark.sql.connect.service

import org.apache.spark.{SecurityManager, SparkConf, SparkEnv, SSLOptions}
import org.apache.spark.sql.connect.common.config.ConnectCommon
import org.apache.spark.sql.test.SharedSparkSession

class SparkConnectSecuritySuite extends SharedSparkSession {

  def withSparkConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val conf = SparkEnv.get.conf
    pairs.foreach { kv => conf.set(kv._1, kv._2) }
    try f
    finally {
      pairs.foreach { kv => conf.remove(kv._1) }
    }
  }

  private def sslEnabledConf(sslPort: Option[Int] = None): (SparkConf, SSLOptions) = {
    val keyStoreFilePath = getTestResourcePath("spark.keystore")
    val conf = new SparkConf()
      .set("spark.ssl.connect.enabled", "true")
      .set("spark.ssl.connect.keyStore", keyStoreFilePath)
      .set("spark.ssl.connect.keyStorePassword", "123456")
      .set("spark.ssl.connect.keyPassword", "123456")
    sslPort.foreach { p =>
      conf.set("spark.ssl.connect.port", p.toString)
    }
    val securityMgr = new SecurityManager(conf)
    (conf, securityMgr.getSSLOptions("connect"))
  }

  test("Start server with TLS enabled") {
    val (conf, sslContext) = sslEnabledConf()
    assert(sslContext.createNettySslContext().isDefined)
    withSparkConf(conf.getAll: _*) {
      SparkConnectService.start()
      assert(SparkConnectService.server.getPort() == ConnectCommon.CONNECT_GRPC_BINDING_PORT)
    }
  }

  test("Start server with TLS enabled with custom port") {
    val (conf, sslContext) = sslEnabledConf(Some(9999))
    assert(sslContext.createNettySslContext().isDefined)
    withSparkConf(conf.getAll: _*) {
      SparkConnectService.start()
      assert(SparkConnectService.server.getPort() == 9999)
    }
  }
}
