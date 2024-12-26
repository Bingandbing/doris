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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.Monotonic;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullableOnDateLikeV2Args;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DateTimeType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DateType;
import org.apache.doris.nereids.types.DateV2Type;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * ScalarFunction 'to_monday'. This class is generated by GenerateFunction.
 */
public class ToMonday extends ScalarFunction
        implements UnaryExpression, ExplicitlyCastableSignature, PropagateNullableOnDateLikeV2Args, Monotonic {

    private static final List<FunctionSignature> SIGNATURES = ImmutableList.of(
            FunctionSignature.ret(DateV2Type.INSTANCE).args(DateV2Type.INSTANCE),
            FunctionSignature.ret(DateType.INSTANCE).args(DateType.INSTANCE),
            FunctionSignature.ret(DateV2Type.INSTANCE).args(DateTimeV2Type.SYSTEM_DEFAULT),
            FunctionSignature.ret(DateType.INSTANCE).args(DateTimeType.INSTANCE)
    );

    /**
     * constructor with 1 argument.
     */
    public ToMonday(Expression arg) {
        super("to_monday", arg);
    }

    /**
     * withChildren.
     */
    @Override
    public ToMonday withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new ToMonday(children.get(0));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitToMonday(this, context);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return SIGNATURES;
    }

    @Override
    public boolean isPositive() {
        return true;
    }

    @Override
    public int getMonotonicFunctionChildIndex() {
        return 0;
    }

    @Override
    public Expression withConstantArgs(Expression literal) {
        return new ToMonday(literal);
    }
}
