/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.polaris.spark;

import org.apache.spark.sql.SparkSessionExtensions;
import scala.runtime.BoxedUnit;

/**
 * Spark session extension that registers the PolarisSparkSqlParser to intercept transaction
 * commands (BEGIN, COMMIT, ROLLBACK, SET TRANSACTION ISOLATION LEVEL).
 *
 * <p>Register via spark-defaults.conf:
 *
 * <pre>
 * spark.sql.extensions = ...,org.apache.polaris.spark.PolarisSqlExtensions
 * spark.sql.extensions.polaris.catalog.name = polaris  (optional, defaults to "polaris")
 * </pre>
 */
public class PolarisSqlExtensions implements scala.Function1<SparkSessionExtensions, BoxedUnit> {

  static final String CATALOG_NAME_CONF = "spark.sql.extensions.polaris.catalog.name";
  static final String DEFAULT_CATALOG_NAME = "polaris";

  @Override
  public BoxedUnit apply(SparkSessionExtensions extensions) {
    extensions.injectParser(
        (scala.Function2<
                org.apache.spark.sql.SparkSession,
                org.apache.spark.sql.catalyst.parser.ParserInterface,
                org.apache.spark.sql.catalyst.parser.ParserInterface>)
            (session, delegate) -> {
              // session.conf() triggers sessionState initialization which itself calls
              // buildParser → circular StackOverflow in Spark 4.0+. Use the underlying
              // SparkConf (via SparkContext) which is always initialized before sessionState.
              String catalogName =
                  session.sparkContext().conf().get(CATALOG_NAME_CONF, DEFAULT_CATALOG_NAME);
              return new PolarisSparkSqlParser(delegate, catalogName);
            });
    return BoxedUnit.UNIT;
  }
}
