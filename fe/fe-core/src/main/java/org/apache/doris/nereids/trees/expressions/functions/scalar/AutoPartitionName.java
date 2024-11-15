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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * ScalarFunction 'auto_partition_name'. This class is not generated by
 * GenerateFunction.
 */
public class AutoPartitionName extends ScalarFunction
        implements ExplicitlyCastableSignature, AlwaysNotNullable {

    public static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(VarcharType.SYSTEM_DEFAULT).varArgs(VarcharType.SYSTEM_DEFAULT),
            FunctionSignature.ret(StringType.INSTANCE).varArgs(StringType.INSTANCE));

    /**
     * constructor with 2 or 3 arguments.
     */
    public AutoPartitionName(Expression arg, Expression... varArgs) {
        super("auto_partition_name", ExpressionUtils.mergeArguments(arg, varArgs));
    }

    /**
     * withChildren.
     */
    @Override
    public AutoPartitionName withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() >= 2);
        return new AutoPartitionName(children.get(0),
                children.subList(1, children.size()).toArray(new Expression[0]));
    }

    @Override
    public void checkLegalityAfterRewrite() {
        if (arity() < 2) {
            throw new AnalysisException("function auto_partition_name must contains at least two arguments");
        }
        if (!child(0).isLiteral()) {
            throw new AnalysisException("auto_partition_name must accept literal for 1st argument");
        }
        final String partition_type = ((VarcharLiteral) getArgument(0)).getStringValue().toLowerCase();
        if (!Lists.newArrayList("range", "list").contains(partition_type)) {
            throw new AnalysisException("function auto_partition_name must accept range|list for 1st argument");
        } else if (Lists.newArrayList("range").contains(partition_type)) {
            if (!child(1).isLiteral()) {
                throw new AnalysisException("auto_partition_name must accept literal for 2nd argument");
            } else {
                final String range_partition_type = ((VarcharLiteral) getArgument(1)).getStringValue()
                        .toLowerCase();
                if (arity() != 3) {
                    throw new AnalysisException("range auto_partition_name must contains three arguments");
                }
                if (!Lists.newArrayList("year", "month", "day", "hour", "minute", "second")
                        .contains(range_partition_type)) {
                    throw new AnalysisException(
                            "range auto_partition_name must accept year|month|day|hour|minute|second for 2nd argument");
                }
            }

        }
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        checkLegalityAfterRewrite();
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitAutoPartitionName(this, context);
    }
}
