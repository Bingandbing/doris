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

import org.apache.doris.catalog.ColocateTableIndex;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.ColumnStatistic;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    private String dbName;

    private String catalogName;

    // metadata objects
    private List<Table> tables;

    private Map<String, ColumnStatistic> totalColumnStatisticMap = new HashMap<>();

    private ColocateTableIndex colocateTableIndex;

    /** Minidump class used to save environment messages */
    public Minidump(String sql, SessionVariable sessionVariable,
                    String parsedPlanJson, String resultPlanJson, List<Table> tables,
                    String catalogName, String dbName, Map<String, ColumnStatistic> totalColumnStatisticMap,
                    ColocateTableIndex colocateTableIndex) {
        this.sql = sql;
        this.sessionVariable = sessionVariable;
        this.parsedPlanJson = parsedPlanJson;
        this.resultPlanJson = resultPlanJson;
        this.tables = tables;
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.totalColumnStatisticMap = totalColumnStatisticMap;
        this.colocateTableIndex = colocateTableIndex;
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

    public String getCatalogName() {
        return catalogName;
    }

    public String getDbName() {
        return dbName;
    }

    public ColocateTableIndex getColocateTableIndex() {
        return colocateTableIndex;
    }

    public Map<String, ColumnStatistic> getTotalColumnStatisticMap() {
        return totalColumnStatisticMap;
    }

    /** Nereids minidump entry, argument should be absolute address of minidump path */
    public static void main(String[] args) {
        assert (args.length == 1);
        Minidump minidump = null;
        try {
            minidump = MinidumpUtils.jsonMinidumpLoad(args[0]);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ConnectContext connectContext = new ConnectContext();
        connectContext.setSessionVariable(minidump.getSessionVariable());
        connectContext.setTables(minidump.getTables());
        connectContext.getSessionVariable().setDumpNereids(false);
        connectContext.setDatabase(minidump.getDbName());
        connectContext.getSessionVariable().setPlanNereidsDump(true);
        connectContext.getTotalColumnStatisticMap().putAll(minidump.getTotalColumnStatisticMap());
        Env.getCurrentEnv().setColocateTableIndex(minidump.getColocateTableIndex());

        LogicalPlan parsed = new NereidsParser().parseSingle(minidump.getSql());
        NereidsPlanner nereidsPlanner = new NereidsPlanner(
                new StatementContext(connectContext, new OriginStatement(minidump.getSql(), 0)));
        nereidsPlanner.plan(LogicalPlanAdapter.of(parsed));
        assert (nereidsPlanner.getOptimizedPlan().toJson().toString().equals(minidump.getResultPlanJson()));
        System.out.println("execute success");
    }
}
