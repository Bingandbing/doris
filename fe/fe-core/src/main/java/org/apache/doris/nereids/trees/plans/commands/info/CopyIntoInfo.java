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

package org.apache.doris.nereids.trees.plans.commands.info;

import com.google.common.base.Strings;
import org.apache.doris.analysis.*;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.proto.Cloud.StagePB.StageType;
import org.apache.doris.cloud.storage.RemoteBase;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * copy into informations
 */
public class CopyIntoInfo {
    private static final Logger LOG = LogManager.getLogger(CopyStmt.class);

    private final List<String> nameParts;
    private CopyFromDesc copyFromDesc;
    private CopyIntoProperties copyIntoProperties;
    private Map<String, String> optHints;

    private LabelName label = null;
    private BrokerDesc brokerDesc = null;
    private DataDescription dataDescription = null;
    private final Map<String, String> brokerProperties = new HashMap<>();
    private Map<String, String> properties = new HashMap<>();

    private String stage;
    private String stageId;
    private StageType stageType;
    private String stagePrefix;
    private RemoteBase.ObjectInfo objectInfo;
    private String userName;

    /**
     * copy into informations
     */
    public CopyIntoInfo(List<String> nameParts, CopyFromDesc copyFromDesc,
                        Map<String, String> properties, Map<String, String> optHints) {
        this.nameParts = nameParts;
        this.copyFromDesc = copyFromDesc;
        this.properties = properties;
        this.optHints = optHints;
    }

    public void validate(ConnectContext ctx) throws DdlException {
        if (this.optHints != null && this.optHints.containsKey(SessionVariable.CLOUD_CLUSTER)) {
            ((CloudEnv) Env.getCurrentEnv()).checkCloudClusterPriv(this.optHints.get(SessionVariable.CLOUD_CLUSTER));
        }
        // generate a label
        String labelName = "copy_" + DebugUtil.printId(ctx.queryId()).replace("-", "_");
        String ctl = null;
        String db = null;
        String table = null;
        switch (nameParts.size()) {
            case 1: { // table
                ctl = ctx.getDefaultCatalog();
                if (Strings.isNullOrEmpty(ctl)) {
                    ctl = InternalCatalog.INTERNAL_CATALOG_NAME;
                }
                db = ctx.getDatabase();
                if (Strings.isNullOrEmpty(db)) {
                    new AnalysisException("asdf");
                }
                table = nameParts.get(0);
                break;
            }
            case 2:
                // db.table
                // Use database name from table name parts.
                break;
            case 3: {
                // catalog.db.table
                ctl = nameParts.get(0);
                db = nameParts.get(1);
                table = nameParts.get(2);
                break;
            }
            default:
                throw new IllegalStateException("Table name [" + nameParts + "] is invalid.");
        }
        TableName tableName = new TableName(ctl, db, table);
        label = new LabelName(tableName.getDb(), labelName);
    }

    public CopyStmt toLegacyStatement() {
        return new CopyStmt(null, null, null, null, null);
    }
}
