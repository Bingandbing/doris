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

package org.apache.doris.nereids.metrics.consumer;

import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.nereids.metrics.Event;
import org.apache.doris.nereids.metrics.EventConsumer;

import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.sql.Time;

/**
 * log consumer
 */
public class LogConsumer extends EventConsumer {
    private final Logger logger;

    private static long startTime;

    private static final String logFile = "/Users/libinfeng/workspace/doris/fe/log/minidump/dumpDemo/metriclog";

    private static JSONObject totalTraces = new JSONObject();

    private static JSONArray enforcerEvent = new JSONArray();

    private static JSONArray costStateUpdateEvent = new JSONArray();

    private static JSONArray transformEvent = new JSONArray();

    public static void setStartTime(long startTime) {
        LogConsumer.startTime = startTime;
    }

    public static String getCurrentTime() {
//        return TimeUtils.getCurrentFormatMsTime();
        return String.valueOf(TimeUtils.getEstimatedTime(LogConsumer.startTime)/1000) + "us";
//        return TimeUtils.longToTimeStringWithms(TimeUtils.getEstimatedTime(LogConsumer.startTime));
    }

    public static JSONArray getEnforcerEvent() {
        return enforcerEvent;
    }

    public static JSONArray getCostStateUpdateEvent() {
        return costStateUpdateEvent;
    }

    public static JSONArray getTransformEvent() {
        return transformEvent;
    }

    public LogConsumer(Class<? extends Event> targetClass, Logger logger) {
        super(targetClass);
        this.logger = logger;
    }

    @Override
    public void consume(Event e) {
        return;
//        JSONArray jsonArray = totalTraces.getJSONArray(e.getClass().toString());
//        if (jsonArray == null) {
//            jsonArray = new JSONArray();
//            totalTraces.put(e.getClass().toString(), jsonArray);
//        }
//        jsonArray.put(e.toJson().toString());
    }

    public static void output()
    {
        totalTraces.put("TransformEvent", transformEvent);
        totalTraces.put("CostStateUpdateEvent", costStateUpdateEvent);
        totalTraces.put("EnforcerEvent", enforcerEvent);
        try {
            FileWriter fileWriter = new FileWriter(logFile, true);
            fileWriter.write(totalTraces.toString());
            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
