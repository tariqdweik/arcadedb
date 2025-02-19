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

import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EmbeddedEdgeType;
import com.arcadedb.schema.EmbeddedVertexType;

import java.util.*;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class CreateRecordStep extends AbstractExecutionStep {
  private       int    created = 0;
  private final int    total;
  private final String typeName;

  public CreateRecordStep(final String typeName, final CommandContext context, final int total, final boolean profilingEnabled) {
    super(context, profilingEnabled);
    this.typeName = typeName;
    this.total = total;
  }

  @Override
  public ResultSet syncPull(final CommandContext context, final int nRecords) throws TimeoutException {
    pullPrevious(context, nRecords);

    return new ResultSet() {
      int locallyCreated = 0;

      @Override
      public boolean hasNext() {
        if (locallyCreated >= nRecords) {
          return false;
        }
        return created < total;
      }

      @Override
      public Result next() {
        final long begin = profilingEnabled ? System.nanoTime() : 0;
        try {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          created++;
          locallyCreated++;

          final DocumentType type = context.getDatabase().getSchema().getType(typeName);

          final MutableDocument instance;
          if (type instanceof EmbeddedVertexType)
            instance = context.getDatabase().newVertex(typeName);
          else if (type instanceof EmbeddedEdgeType)
            throw new IllegalArgumentException("Cannot instantiate an edge");
          else
            instance = context.getDatabase().newDocument(typeName);

          return new UpdatableResult(instance);
        } finally {
          if (profilingEnabled) {
            cost += (System.nanoTime() - begin);
          }
        }
      }

    };
  }

  @Override
  public String prettyPrint(final int depth, final int indent) {
    final String spaces = ExecutionStepInternal.getIndent(depth, indent);
    final StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ CREATE EMPTY RECORDS");
    if (profilingEnabled) {
      result.append(" (").append(getCostFormatted()).append(")");
    }
    result.append("\n");
    result.append(spaces);
    if (total == 1) {
      result.append("  1 record");
    } else {
      result.append("  ").append(total).append(" record");
    }
    return result.toString();
  }

}
