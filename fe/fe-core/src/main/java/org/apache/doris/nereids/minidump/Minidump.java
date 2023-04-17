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

package org.apache.doris.nereids.minidump;

import org.apache.doris.catalog.Table;
import org.apache.doris.qe.SessionVariable;

import java.util.List;

/** Minidump for Nereids */
public class Minidump {
    // traceflags

    // optimizer configuration
    private SessionVariable sessionVariable;

    // original sql
    private String sql;

    // parsed plan in json format
    private String parsedPlanJson;

    // result plan in json format
    private String resultPlanJson;

    // metadata objects
    private List<Table> tables;

    public Minidump(String sql, SessionVariable sessionVariable,
                    String parsedPlanJson, String resultPlanJson, List<Table> tables) {
        this.sql = sql;
        this.sessionVariable = sessionVariable;
        this.parsedPlanJson = parsedPlanJson;
        this.resultPlanJson = resultPlanJson;
        this.tables = tables;
    }

    public Minidump() {

    }

    public SessionVariable getSessionVariable() {
        return sessionVariable;
    }

    public String getParsedPlanJson() {
        return parsedPlanJson;
    }

    public String getResultPlanJson() {
        return resultPlanJson;
    }

    public List<Table> getTables() {
        return tables;
    }

    public String getSql() {
        return sql;
    }
}
