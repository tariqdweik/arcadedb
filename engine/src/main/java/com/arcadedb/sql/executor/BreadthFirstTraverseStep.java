/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.arcadedb.sql.executor;

import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.sql.parser.PInteger;
import com.arcadedb.sql.parser.TraverseProjectionItem;
import com.arcadedb.sql.parser.WhereClause;

import java.util.*;

/**
 * Created by luigidellaquila on 26/10/16.
 */
public class BreadthFirstTraverseStep extends AbstractTraverseStep {

  public BreadthFirstTraverseStep(List<TraverseProjectionItem> projections, WhereClause whileClause, PInteger maxDepth,
      CommandContext ctx, boolean profilingEnabled) {
    super(projections, whileClause, maxDepth, ctx, profilingEnabled);
  }

  @Override
  protected void fetchNextEntryPoints(CommandContext ctx, int nRecords) {
    ResultSet nextN = getPrev().get().syncPull(ctx, nRecords);
    while (nextN.hasNext()) {
      while (nextN.hasNext()) {
        Result item = toTraverseResult(nextN.next());
        if(item != null){
          ArrayDeque<RID> stack = new ArrayDeque<RID>();
          item.getIdentity().ifPresent(x -> stack.push(x));
          ((ResultInternal) item).setMetadata("$stack", stack);


          List<RID> path = new ArrayList<>();
          path.add(item.getIdentity().get());
          ((ResultInternal) item).setMetadata("$path", path);

          if (item != null && item.isElement() && !traversed.contains(item.getElement().get().getIdentity())) {
            tryAddEntryPoint(item, ctx);

          }
        }
      }
      nextN = getPrev().get().syncPull(ctx, nRecords);
    }
  }

  private Result toTraverseResult(Result item) {
    TraverseResult res = null;
    if (item instanceof TraverseResult) {
      res = (TraverseResult) item;
    } else if (item.isElement()) {
      res = new TraverseResult();
      res.setElement(item.getElement().get());
      res.depth = 0;
      res.setMetadata("$depth", 0);
    } else {
      return null;
    }

    return res;
  }

  @Override
  protected void fetchNextResults(CommandContext ctx, int nRecords) {
    if (!this.entryPoints.isEmpty()) {
      TraverseResult item = (TraverseResult) this.entryPoints.remove(0);
      this.results.add(item);
      for (TraverseProjectionItem proj : projections) {
        Object nextStep = proj.execute(item, ctx);
        if (this.maxDepth == null || this.maxDepth.getValue().intValue() > item.depth) {
          addNextEntryPoints(nextStep, item.depth + 1, (List<RID>) item.getMetadata("$path"), ctx);
        }
      }
    }
  }

  private void addNextEntryPoints(Object nextStep, int depth, List<RID> path, CommandContext ctx) {
    if (nextStep instanceof Record) {
      addNextEntryPoints(nextStep, depth, path, ctx);
    } else if (nextStep instanceof Iterable) {
      addNextEntryPoints(((Iterable) nextStep).iterator(), depth, path, ctx);
    } else if (nextStep instanceof Result) {
      addNextEntryPoints(((Result) nextStep), depth, path, ctx);
    }
  }

  private void addNextEntryPoints(Iterator nextStep, int depth, List<RID> path, CommandContext ctx) {
    while (nextStep.hasNext()) {
      addNextEntryPoints(nextStep.next(), depth, path, ctx);
    }
  }

  private void addNextEntryPoints(Document nextStep, int depth, List<RID> path, CommandContext ctx) {
    if (this.traversed.contains(nextStep.getIdentity())) {
      return;
    }
    TraverseResult res = new TraverseResult();
    res.setElement(nextStep);
    res.depth = depth;
    res.setMetadata("$depth", depth);

    List<RID> newPath = new ArrayList<>();
    newPath.addAll(path);
    newPath.add(res.getIdentity().get());
    res.setMetadata("$path", newPath);

    List reverseStack = new ArrayList();
    reverseStack.addAll(newPath);
    Collections.reverse(reverseStack);
    ArrayDeque newStack = new ArrayDeque();
    newStack.addAll(reverseStack);
    res.setMetadata("$stack", newStack);

    tryAddEntryPoint(res, ctx);

  }

  private void addNextEntryPoints(Result nextStep, int depth, List<RID> path, CommandContext ctx) {
    if (!nextStep.isElement()) {
      return;
    }
    if (this.traversed.contains(nextStep.getElement().get().getIdentity())) {
      return;
    }
    if (nextStep instanceof TraverseResult) {
      ((TraverseResult) nextStep).depth = depth;
      ((TraverseResult) nextStep).setMetadata("$depth", depth);

      List<RID> newPath = new ArrayList<>();
      newPath.addAll(path);
      newPath.add(nextStep.getIdentity().get());
      ((TraverseResult) nextStep).setMetadata("$path", newPath);

      List reverseStack = new ArrayList();
      reverseStack.addAll(newPath);
      Collections.reverse(reverseStack);
      ArrayDeque newStack = new ArrayDeque();
      newStack.addAll(reverseStack);
      ((TraverseResult) nextStep).setMetadata("$stack", newStack);

      tryAddEntryPoint(nextStep, ctx);
    } else {
      TraverseResult res = new TraverseResult();
      res.setElement(nextStep.getElement().get());
      res.depth = depth;
      res.setMetadata("$depth", depth);

      List<RID> newPath = new ArrayList<>();
      newPath.addAll(path);
      newPath.add(nextStep.getIdentity().get());
      ((TraverseResult) nextStep).setMetadata("$path", newPath);

      List reverseStack = new ArrayList();
      reverseStack.addAll(newPath);
      Collections.reverse(reverseStack);
      ArrayDeque newStack = new ArrayDeque();
      newStack.addAll(reverseStack);
      ((TraverseResult) nextStep).setMetadata("$stack", newStack);

      tryAddEntryPoint(res, ctx);
    }
  }

  private void tryAddEntryPoint(Result res, CommandContext ctx) {
    if (whileClause == null || whileClause.matchesFilters(res, ctx)) {
      this.entryPoints.add(res);
    }
    traversed.add(res.getElement().get().getIdentity());
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ BREADTH-FIRST TRAVERSE \n");
    if (whileClause != null) {
      result.append(spaces);
      result.append("WHILE " + whileClause.toString());
    }
    return result.toString();
  }
}
