// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import org.junit.Assert;

suite("test_ddl_function_auth","p0,auth_call") {
    String user = 'test_ddl_function_auth_user'
    String pwd = 'C123_567p'
    String dbName = 'test_ddl_function_auth_db'
    String functionName = 'test_ddl_function_auth_fct'

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user}""";
    }

    try_sql("DROP USER ${user}")
    try_sql """drop database if exists ${dbName}"""
    try_sql("""DROP FUNCTION ${dbName}.${functionName}(INT)""")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""
    sql """grant select_priv on regression_test to ${user}"""
    sql """create database ${dbName}"""
    sql """grant select_priv on ${dbName}.* to ${user}"""

    // ddl create,show,drop
    connect(user, "${pwd}", context.config.jdbcUrl) {
        test {
            sql """CREATE ALIAS FUNCTION ${dbName}.${functionName}(INT) WITH PARAMETER(id)  AS CONCAT(LEFT(id, 3), '****', RIGHT(id, 4));"""
            exception "denied"
        }

        sql """use ${dbName}"""
        def res = sql """show functions"""
        assertTrue(res.size() == 0)

        test {
            sql """DROP FUNCTION ${dbName}.${functionName}(INT)"""
            exception "denied"
        }
    }
    sql """grant admin_priv on *.*.* to ${user}"""
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql """CREATE ALIAS FUNCTION ${dbName}.${functionName}(INT) WITH PARAMETER(id)  AS CONCAT(LEFT(id, 3), '****', RIGHT(id, 4));"""
        sql """use ${dbName}"""
        def res = sql """show functions"""
        assertTrue(res.size() == 1)

        sql """select ${functionName}(1)"""
        sql """DROP FUNCTION ${dbName}.${functionName}(INT)"""
        res = sql """show functions"""
        assertTrue(res.size() == 0)
    }
    sql """revoke admin_priv on *.*.* from ${user}"""

    // show
    sql """CREATE ALIAS FUNCTION ${dbName}.${functionName}(INT) WITH PARAMETER(id)  AS CONCAT(LEFT(id, 3), '****', RIGHT(id, 4));"""
    sql """use ${dbName}"""
    def func_res = sql """show functions"""
    assertTrue(func_res.size() == 1)
    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql """use ${dbName}"""
        def res = sql """SHOW CREATE FUNCTION ${dbName}.${functionName}(INT)"""
        logger.info("res: " + res)
        assertTrue(res.size() == 1)
    }

    try_sql("""DROP FUNCTION ${dbName}.${functionName}(INT)""")
    sql """drop database if exists ${dbName}"""
    try_sql("DROP USER ${user}")
}
