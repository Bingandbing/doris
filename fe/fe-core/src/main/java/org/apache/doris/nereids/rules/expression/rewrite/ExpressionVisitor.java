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

package org.apache.doris.nereids.rules.expression.rewrite;

import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Literal;
import org.apache.doris.nereids.trees.expressions.Not;

/**
 * Use the visitor pattern to iterate over all expressions for expression rewriting.
 */
public abstract class ExpressionVisitor<R, C> {

    public abstract R visitExpression(Expression expr, C context);

    public R visitNot(Not expr, C context) {
        return visitExpression(expr, context);
    }

    public R visitComparisonPredicate(ComparisonPredicate expr, C context) {
        return visitExpression(expr, context);
    }

    public R visitLiteral(Literal expr, C context) {
        return visitExpression(expr, context);
    }
}
