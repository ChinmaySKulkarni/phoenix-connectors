/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.jdbc

import java.util.Locale

import org.apache.phoenix.schema.types.PDataType
import org.apache.spark.sql.execution.datasources.SparkJdbcUtil
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, StringType}

private object PhoenixJdbcDialect  extends JdbcDialect {

  override def canHandle(url: String): Boolean = url.startsWith("jdbc:phoenix")

  /**
    * This is only called for ArrayType (see JdbcUtils.makeSetter)
    */
  // TODO: Fix this for arrays
  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case StringType => Some(JdbcType("VARCHAR", java.sql.Types.VARCHAR))
    case BinaryType => Some(JdbcType("BINARY(" + dt.defaultSize + ")", java.sql.Types.BINARY))
    case ByteType => Some(JdbcType("TINYINT", java.sql.Types.TINYINT))
    case BooleanType => Some(JdbcType("BOOLEAN", java.sql.Types.BOOLEAN))
    case ArrayType(et, _) => Some(getArrayType(et))
    case _ => None
  }

  def getArrayType(et: DataType): JdbcType = {
    val typeName = SparkJdbcUtil.getJdbcType(et, PhoenixJdbcDialect).databaseTypeDefinition
      .toLowerCase(Locale.ROOT).split("\\(")(0)
    val pDataType = PDataType.fromSqlTypeName(typeName)
    JdbcType(pDataType.getSqlTypeName + " ARRAY", PDataType.ARRAY_TYPE_BASE + pDataType.getSqlType)
  }

}
