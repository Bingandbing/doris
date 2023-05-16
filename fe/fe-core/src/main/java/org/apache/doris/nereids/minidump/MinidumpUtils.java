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
import org.apache.doris.catalog.SchemaTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Histogram;

import com.google.common.collect.ImmutableMap;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * Util for minidump
 */
public class MinidumpUtils {

    public static String DUMP_PATH = null;

    /**
     * Saving of minidump file to fe log path
     */
    public static void saveMinidumpString(JSONObject minidump, String dumpName) {
        String dumpPath = MinidumpUtils.DUMP_PATH + "/" + dumpName;
        File minidumpFileDir = new File(dumpPath);
        if (!minidumpFileDir.exists()) {
            minidumpFileDir.mkdirs();
        }
        String jsonMinidump = minidump.toString();
        try (FileWriter file = new FileWriter(dumpPath + "/" + "dumpFile.json")) {
            file.write(jsonMinidump);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Loading of minidump file
     */
    public static Minidump jsonMinidumpLoad(String dumpFilePath) throws IOException {
        // open file, read file, put them into minidump object
        try (FileInputStream inputStream = new FileInputStream(dumpFilePath + "/dumpFile.json")) {
            StringBuilder sb = new StringBuilder();
            int ch;
            while ((ch = inputStream.read()) != -1) {
                sb.append((char) ch);
            }
            String inputString = sb.toString();
            // Parse the JSON string back into a JSON object
            JSONObject inputJSON = new JSONObject(inputString);
            SessionVariable newSessionVariable = new SessionVariable();
            newSessionVariable.readFromJson(inputJSON.getString("SessionVariable"));
            String sql = inputJSON.getString("Sql");

            List<Table> tables = new ArrayList<>();
            String catalogName = inputJSON.getString("CatalogName");
            String dbName = inputJSON.getString("DbName");
            JSONArray tablesJson = (JSONArray) inputJSON.get("Tables");
            for (int i = 0; i < tablesJson.length(); i++) {
                String tablePath = dumpFilePath + (String) tablesJson.get(i);
                DataInputStream dis = new DataInputStream(new FileInputStream(tablePath));
                Table newTable = Table.read(dis);
                tables.add(newTable);
            }
            String colocateTableIndexPath = dumpFilePath + inputJSON.getString("ColocateTableIndex");
            DataInputStream dis = new DataInputStream(new FileInputStream(colocateTableIndexPath));
            ColocateTableIndex newColocateTableIndex = new ColocateTableIndex();
            newColocateTableIndex.readFields(dis);

            JSONArray columnStats = (JSONArray) inputJSON.get("ColumnStatistics");
            Map<String, ColumnStatistic> columnStatisticMap = new HashMap<>();
            for (int i = 0; i < columnStats.length(); i++) {
                JSONObject oneColumnStat = (JSONObject) columnStats.get(i);
                String colName = oneColumnStat.keys().next();
                String colStat = oneColumnStat.getString(colName);
                ColumnStatistic columnStatistic = ColumnStatistic.fromJson(colStat);
                columnStatisticMap.put(colName, columnStatistic);
            }
            JSONArray histogramJsonArray = (JSONArray) inputJSON.get("Histogram");
            Map<String, Histogram> histogramMap = new HashMap<>();
            for (int i = 0; i < histogramJsonArray.length(); i++) {
                JSONObject histogramJson = (JSONObject) histogramJsonArray.get(i);
                String colName = histogramJson.keys().next();
                String colHistogram = histogramJson.getString(colName);
                Histogram histogram = Histogram.deserializeFromJson(colHistogram);
                histogramMap.put(colName, histogram);
            }
            String parsedPlanJson = inputJSON.getString("ParsedPlan");
            String resultPlanJson = inputJSON.getString("ResultPlan");

            return new Minidump(sql, newSessionVariable, parsedPlanJson, resultPlanJson,
                    tables, catalogName, dbName, columnStatisticMap, histogramMap, newColocateTableIndex);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * serialize tables from Table in catalog to json format
     */
    public static JSONArray serializeTables(
            String minidumpFileDir, String dbAndCatalogName, List<Table> tables) throws IOException {
        JSONArray tablesJson = new JSONArray();
        for (Table table : tables) {
            if (table instanceof SchemaTable) {
                continue;
            }
            String tableFileName = dbAndCatalogName + table.getName();
            tablesJson.put(tableFileName);
            DataOutputStream dos = new DataOutputStream(new FileOutputStream(minidumpFileDir + tableFileName));
            table.write(dos);
            dos.flush();
            dos.close();
        }
        return tablesJson;
    }

    public static void serializeColocateTableIndex(
            String colocateTableIndexFile, ColocateTableIndex colocateTableIndex) throws IOException {
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(colocateTableIndexFile));
        colocateTableIndex.write(dos);
        dos.flush();
        dos.close();
    }

    /**
     * serialize column statistic and replace when loading to dumpfile and environment
     */
    public static JSONArray serializeColumnStatistic(Map<String, ColumnStatistic> totalColumnStatisticMap) {
        JSONArray columnStatistics = new JSONArray();
        for (Map.Entry<String, ColumnStatistic> entry : ImmutableMap.copyOf(totalColumnStatisticMap).entrySet()) {
            ColumnStatistic columnStatistic = entry.getValue();
            String colName = entry.getKey();
            JSONObject oneColumnStats = new JSONObject();
            oneColumnStats.put(colName, columnStatistic.toJson());
            columnStatistics.put(oneColumnStats);
        }
        return columnStatistics;
    }

    /**
     * serialize histogram and replace when loading to dumpfile and environment
     */
    public static JSONArray serializeHistogram(Map<String, Histogram> totalHistogramMap) {
        JSONArray histogramsJson = new JSONArray();
        for (Map.Entry<String, Histogram> entry : totalHistogramMap.entrySet()) {
            Histogram histogram = entry.getValue();
            String colName = entry.getKey();
            JSONObject oneHistogram = new JSONObject();
            oneHistogram.put(colName, Histogram.serializeToJson(histogram));
            histogramsJson.put(oneHistogram);
        }
        return histogramsJson;
    }

    public static List<String> compareJsonObjects(JSONObject json1, JSONObject json2, String path) {
        List<String> differences = new ArrayList<>();

        Iterator<String> keys = json1.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            String currentPath = (path.isEmpty()) ? key : path + "." + key;

            if (!json2.has(key)) {
                differences.add("Key '" + currentPath + "' not found in the second JSON object.");
                continue;
            }

            Object value1 = json1.get(key);
            Object value2 = json2.get(key);

            if (!value1.equals(value2)) {
                if (value1.getClass().equals(value2.getClass())) {
                    differences.add("Value for key '" + currentPath + "' is different: " + value1 + " != " + value2);
                } else if (!value1.toString().equals(value2.toString())) {
                    differences.add("Value for key '" + currentPath + "' is different: " + value1 + " != " + value2);
                } else {
                    differences.add("Value for key '" + currentPath + "' datatype is different: " + value1.getClass().getName() + " != " + value2.getClass().getName());
                }
            }

            if (value1 instanceof JSONObject && value2 instanceof JSONObject) {
                List<String> nestedDifferences = compareJsonObjects((JSONObject) value1, (JSONObject) value2, currentPath);
                differences.addAll(nestedDifferences);
            } else if (value1 instanceof JSONArray && value2 instanceof JSONArray) {
                List<String> nestedDifferences = compareJsonArrays((JSONArray) value1, (JSONArray) value2, currentPath);
                differences.addAll(nestedDifferences);
            }
        }

        keys = json2.keys();
        while (keys.hasNext()) {
            String key = keys.next();
            String currentPath = (path.isEmpty()) ? key : path + "." + key;

            if (!json1.has(key)) {
                differences.add("Key '" + currentPath + "' not found in the first JSON object.");
            }
        }

        return differences;
    }

    private static List<String> compareJsonArrays(JSONArray array1, JSONArray array2, String path) {
        List<String> differences = new ArrayList<>();

        if (array1.length() != array2.length()) {
            differences.add("Array length for key '" + path + "' is different: " + array1.length() + " != " + array2.length());
            return differences;
        }

        for (int i = 0; i < array1.length(); i++) {
            Object value1 = array1.get(i);
            Object value2 = array2.get(i);
            String currentPath = path + "[" + i + "]";

            if (value1 instanceof JSONObject && value2 instanceof JSONObject) {
                List<String> nestedDifferences = compareJsonObjects((JSONObject) value1, (JSONObject) value2, currentPath);
                differences.addAll(nestedDifferences);
            } else if (!value1.equals(value2)) {
                differences.add("Value for key '" + currentPath + "' is different: " + value1 + " != " + value2);
            }
        }

        return differences;
    }

    /**
     * init minidump utils before start to dump file, this will create a path
     */
    public static void init() {
        DUMP_PATH = Optional.ofNullable(DUMP_PATH).orElse(System.getenv("DORIS_HOME") + "/log/minidump");
        new File(DUMP_PATH).mkdirs();
    }
}
