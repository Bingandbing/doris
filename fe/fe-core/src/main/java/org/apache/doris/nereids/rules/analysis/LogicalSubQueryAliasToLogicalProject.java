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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.hint.Hint;
import org.apache.doris.nereids.hint.LeadingHint;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.RelationId;

import com.google.common.collect.ImmutableList;

/**
 * Eliminate the logical sub query and alias node after analyze and before rewrite
 * If we match the alias node and return its child node, in the execute() of the job
 * <p>
 * TODO: refactor group merge strategy to support the feature above
 */
public class LogicalSubQueryAliasToLogicalProject extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return RuleType.LOGICAL_SUB_QUERY_ALIAS_TO_LOGICAL_PROJECT.build(
                logicalSubQueryAlias().thenApply(ctx -> {
                    LogicalProject project = new LogicalProject<>(
                            ImmutableList.copyOf(ctx.root.getOutput()), ctx.root.child());
                    if (ctx.cascadesContext.getStatementContext().getHintMap().get("Leading") != null) {
                        String aliasName = ctx.root.getAlias();
                        Hint leading = ctx.cascadesContext.getStatementContext().getHintMap().get("Leading");
                        RelationId id = ((LeadingHint) leading).findRelationIdAndTableName(aliasName);
                        if (id == null) {
                            id = StatementScopeIdGenerator.newRelationId();
                        }
                        ((LeadingHint) leading).putRelationIdAndTableName(Pair.of(id, aliasName));
                        ((LeadingHint) leading).getRelationIdToScanMap().put(id, project);
                    }
                    return project;
                })
        );
    }
}
