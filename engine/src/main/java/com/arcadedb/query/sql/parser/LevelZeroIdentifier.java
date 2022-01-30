/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */
/* Generated By:JJTree: Do not edit this line. OLevelZeroIdentifier.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_USERTYPE_VISIBILITY_PUBLIC=true */
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Record;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.AggregationContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultInternal;

import java.util.Map;
import java.util.Set;

public class LevelZeroIdentifier extends SimpleNode {
  protected FunctionCall functionCall;
  protected Boolean      self;
  protected PCollection  collection;

  public LevelZeroIdentifier(int id) {
    super(id);
  }

  public LevelZeroIdentifier(SqlParser p, int id) {
    super(p, id);
  }

  public void toString(Map<String, Object> params, StringBuilder builder) {
    if (functionCall != null) {
      functionCall.toString(params, builder);
    } else if (Boolean.TRUE.equals(self)) {
      builder.append("@this");
    } else if (collection != null) {
      collection.toString(params, builder);
    }
  }

  public Object execute(Record iCurrentRecord, CommandContext ctx) {
    if (functionCall != null) {
      return functionCall.execute(iCurrentRecord, ctx);
    }
    if (collection != null) {
      return collection.execute(iCurrentRecord, ctx);
    }
    if (Boolean.TRUE.equals(self)) {
      return iCurrentRecord;
    }
    throw new UnsupportedOperationException();
  }

  public Object execute(Result iCurrentRecord, CommandContext ctx) {
    if (functionCall != null) {
      return functionCall.execute(iCurrentRecord, ctx);
    }
    if (collection != null) {
      return collection.execute(iCurrentRecord, ctx);
    }
    if (Boolean.TRUE.equals(self)) {
      return iCurrentRecord;
    }
    throw new UnsupportedOperationException();
  }

  public boolean isIndexedFunctionCall() {
    if (functionCall != null) {
      return functionCall.isIndexedFunctionCall();
    }
    return false;
  }

  public long estimateIndexedFunction(FromClause target, CommandContext context, BinaryCompareOperator operator, Object right) {
    if (functionCall != null) {
      return functionCall.estimateIndexedFunction(target, context, operator, right);
    }

    return -1;
  }

  public Iterable<Record> executeIndexedFunction(FromClause target, CommandContext context, BinaryCompareOperator operator, Object right) {
    if (functionCall != null) {
      return functionCall.executeIndexedFunction(target, context, operator, right);
    }
    return null;
  }

  /**
   * tests if current expression is an indexed funciton AND that function can also be executed without using the index
   *
   * @param target   the query target
   * @param context  the execution context
   * @param operator
   * @param right
   *
   * @return true if current expression is an indexed funciton AND that function can also be executed without using the index, false otherwise
   */
  public boolean canExecuteIndexedFunctionWithoutIndex(FromClause target, CommandContext context, BinaryCompareOperator operator, Object right) {
    if (this.functionCall == null) {
      return false;
    }
    return functionCall.canExecuteIndexedFunctionWithoutIndex(target, context, operator, right);
  }

  /**
   * tests if current expression is an indexed function AND that function can be used on this target
   *
   * @param target   the query target
   * @param context  the execution context
   * @param operator
   * @param right
   *
   * @return true if current expression involves an indexed function AND that function can be used on this target, false otherwise
   */
  public boolean allowsIndexedFunctionExecutionOnTarget(FromClause target, CommandContext context, BinaryCompareOperator operator, Object right) {
    if (this.functionCall == null) {
      return false;
    }
    return functionCall.allowsIndexedFunctionExecutionOnTarget(target, context, operator, right);
  }

  /**
   * tests if current expression is an indexed function AND the function has also to be executed after the index search.
   * In some cases, the index search is accurate, so this condition can be excluded from further evaluation. In other cases
   * the result from the index is a superset of the expected result, so the function has to be executed anyway for further filtering
   *
   * @param target  the query target
   * @param context the execution context
   *
   * @return true if current expression is an indexed function AND the function has also to be executed after the index search.
   */
  public boolean executeIndexedFunctionAfterIndexSearch(FromClause target, CommandContext context, BinaryCompareOperator operator, Object right) {
    if (this.functionCall == null) {
      return false;
    }
    return functionCall.executeIndexedFunctionAfterIndexSearch(target, context, operator, right);
  }

  public boolean isExpand() {
    if (functionCall != null) {
      return functionCall.isExpand();
    }
    return false;
  }

  public Expression getExpandContent() {
    if (functionCall.getParams().size() != 1) {
      throw new CommandExecutionException("Invalid expand expression: " + functionCall.toString());
    }
    return functionCall.getParams().get(0);
  }

  public boolean needsAliases(Set<String> aliases) {
    if (functionCall != null && functionCall.needsAliases(aliases)) {
      return true;
    }
    return collection != null && collection.needsAliases(aliases);
  }

  public boolean isAggregate() {
    if (functionCall != null && functionCall.isAggregate()) {
      return true;
    }
    return collection != null && collection.isAggregate();
  }

  public boolean isCount() {
    return functionCall != null && functionCall.name.getStringValue().equalsIgnoreCase("count");
  }

  public boolean isEarlyCalculated() {
    if (functionCall != null && functionCall.isEarlyCalculated()) {
      return true;
    }
    if (Boolean.TRUE.equals(self)) {
      return false;
    }
    return collection != null && collection.isEarlyCalculated();
  }

  public SimpleNode splitForAggregation(AggregateProjectionSplit aggregateProj) {
    if (isAggregate()) {
      LevelZeroIdentifier result = new LevelZeroIdentifier(-1);
      if (functionCall != null) {
        SimpleNode node = functionCall.splitForAggregation(aggregateProj);
        if (node instanceof FunctionCall) {
          result.functionCall = (FunctionCall) node;
        } else {
          return node;
        }
      } else if (collection != null) {
        result.collection = collection.splitForAggregation(aggregateProj);
        return result;
      } else {
        throw new IllegalStateException();
      }
      return result;
    } else {
      return this;
    }
  }

  public AggregationContext getAggregationContext(CommandContext ctx) {
    if (isAggregate()) {
      if (functionCall != null) {
        return functionCall.getAggregationContext(ctx);
      }
    }
    throw new CommandExecutionException("cannot aggregate on " + this);
  }

  public LevelZeroIdentifier copy() {
    final LevelZeroIdentifier result = new LevelZeroIdentifier(-1);
    result.functionCall = functionCall == null ? null : functionCall.copy();
    result.self = self;
    result.collection = collection == null ? null : collection.copy();
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final LevelZeroIdentifier that = (LevelZeroIdentifier) o;

    if (functionCall != null ? !functionCall.equals(that.functionCall) : that.functionCall != null)
      return false;
    if (self != null ? !self.equals(that.self) : that.self != null)
      return false;
    return collection != null ? collection.equals(that.collection) : that.collection == null;
  }

  @Override
  public int hashCode() {
    int result = functionCall != null ? functionCall.hashCode() : 0;
    result = 31 * result + (self != null ? self.hashCode() : 0);
    result = 31 * result + (collection != null ? collection.hashCode() : 0);
    return result;
  }

  public void setCollection(PCollection collection) {
    this.collection = collection;
  }

  public boolean refersToParent() {
    if (functionCall != null && functionCall.refersToParent()) {
      return true;
    }
    return collection != null && collection.refersToParent();
  }

  public FunctionCall getFunctionCall() {
    return functionCall;
  }

  public Boolean getSelf() {
    return self;
  }

  public PCollection getCollection() {
    return collection;
  }

  public Result serialize() {
    final ResultInternal result = new ResultInternal();
    if (functionCall != null) {
      result.setProperty("functionCall", functionCall.serialize());
    }
    result.setProperty("self", self);
    if (collection != null) {
      result.setProperty("collection", collection.serialize());
    }
    return result;
  }

  public void deserialize(Result fromResult) {
    if (fromResult.getProperty("functionCall") != null) {
      functionCall = new FunctionCall(parser, -1);
      functionCall.deserialize(fromResult.getProperty("functionCall"));
    }
    self = fromResult.getProperty("self");
    if (fromResult.getProperty("collection") != null) {
      collection = new PCollection(-1);
      collection.deserialize(fromResult.getProperty("collection"));
    }
  }

  public void extractSubQueries(Identifier letAlias, SubQueryCollector collector) {
    if (this.functionCall != null) {
      this.functionCall.extractSubQueries(letAlias, collector);
    }
  }

  public void extractSubQueries(SubQueryCollector collector) {
    if (this.functionCall != null) {
      this.functionCall.extractSubQueries(collector);
    }
  }

  public boolean isCacheable() {
    if (functionCall != null) {
      return functionCall.isCacheable();
    }
    if (collection != null) {
      return collection.isCacheable();
    }
    return false;
  }
}
/* JavaCC - OriginalChecksum=0305fcf120ba9395b4c975f85cdade72 (do not edit this line) */
