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
package com.arcadedb.query.sql.executor;

import com.arcadedb.exception.TimeoutException;

import java.util.*;

/**
 * Created by luigidellaquila on 20/09/16.
 */
public class MatchPrefetchStep extends AbstractExecutionStep {

  public static final String PREFETCHED_MATCH_ALIAS_PREFIX = "__$$Arcadedb_Prefetched_Alias_Prefix__";

  private final String                alias;
  private final InternalExecutionPlan prefetchExecutionPlan;

  boolean executed = false;

  public MatchPrefetchStep(final CommandContext context, final InternalExecutionPlan prefetchExecPlan, final String alias, final boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.prefetchExecutionPlan = prefetchExecPlan;
    this.alias = alias;
  }

  @Override
  public void reset() {
    executed = false;
    prefetchExecutionPlan.reset(context);
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    if (!executed) {
      pullPrevious(context, nRecords);

      ResultSet nextBlock = prefetchExecutionPlan.fetchNext(nRecords);
      final List<Result> prefetched = new ArrayList<>();
      while (nextBlock.hasNext()) {
        while (nextBlock.hasNext()) {
          prefetched.add(nextBlock.next());
        }
        nextBlock = prefetchExecutionPlan.fetchNext(nRecords);
      }
      prefetchExecutionPlan.close();
      context.setVariable(PREFETCHED_MATCH_ALIAS_PREFIX + alias, prefetched);
      executed = true;
    }
    return new InternalResultSet();
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final String result = spaces + "+ PREFETCH " + alias + "\n" + prefetchExecutionPlan.prettyPrint(depth + 1, indent);
    return result;
  }
}
