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

package org.apache.doris.nereids.rules.rewrite;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.hint.LeadingHint;
import org.apache.doris.nereids.jobs.joinorder.hypergraph.bitmap.LongBitmap;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * CollectJoinConstraint
 */
public class CollectJoinConstraint implements RewriteRuleFactory {

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            logicalOlapScan().thenApply(ctx -> {
                LeadingHint leading = (LeadingHint) ctx.cascadesContext.getStatementContext().getHintMap().get("Leading");
                if (leading == null) {
                    return ctx.root;
                }
                return ctx.root;
            }).toRule(RuleType.COLLECT_JOIN_CONSTRAINT),

            logicalJoin().thenApply(ctx -> {
                LeadingHint leading = (LeadingHint) ctx.cascadesContext.getStatementContext().getHintMap().get("Leading");
                if (leading == null) {
                    return ctx.root;
                }
                LogicalJoin join = ctx.root;
                List<Expression> expressions = join.getHashJoinConjuncts();
                for (Expression expression : expressions) {
                    Long filterBitMap = calFilterMap(leading, expression.getInputSlots());
                    leading.getFilters().add(Pair.of(filterBitMap, expression));
                }
                expressions = join.getOtherJoinConjuncts();
                for (Expression expression : expressions) {
                    Long filterBitMap = calFilterMap(leading, expression.getInputSlots());
                    leading.getFilters().add(Pair.of(filterBitMap, expression));
                }

                return ctx.root;
            }).toRule(RuleType.COLLECT_JOIN_CONSTRAINT),

            logicalFilter().thenApply(ctx -> {
                LeadingHint leading = (LeadingHint) ctx.cascadesContext.getStatementContext().getHintMap().get("Leading");
                if (leading == null) {
                    return ctx.root;
                }
                LogicalFilter filter = ctx.root;
                Set<Expression> expressions = filter.getConjuncts();
                for (Expression expression : expressions) {
                    Long filterBitMap = calFilterMap(leading, expression.getInputSlots());
                    leading.getFilters().add(Pair.of(filterBitMap, expression));
                }
                return ctx.root;
            }).toRule(RuleType.COLLECT_FILTER)
        );
    }

    private long calFilterMap(LeadingHint leading, Set<Slot> slots) {
        Preconditions.checkArgument(slots.size() != 0);
        long bitmap = LongBitmap.newBitmap();
        for (Slot slot : slots) {
            LogicalPlan scan = leading.getTableNameToScanMap().get(slot.getQualifier().get(1));
            bitmap = LongBitmap.or(bitmap, ((LogicalRelation) scan).getTable().getId());
        }
        return bitmap;
    }
}
