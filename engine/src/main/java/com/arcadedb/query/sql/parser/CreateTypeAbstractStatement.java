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
/* Generated By:JJTree: Do not edit this line. OCreateClassStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_USERTYPE_VISIBILITY_PUBLIC=true */
package com.arcadedb.query.sql.parser;

import com.arcadedb.engine.Bucket;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EmbeddedDocumentType;
import com.arcadedb.schema.Schema;

import java.util.*;
import java.util.stream.*;

public abstract class CreateTypeAbstractStatement extends DDLStatement {
  /**
   * Class name
   */
  public Identifier name;

  public boolean ifNotExists;

  /**
   * Direct superclasses for this class
   */
  protected List<Identifier> supertypes;

  /**
   * Cluster IDs for this class
   */
  protected List<BucketIdentifier> buckets;

  /**
   * Total number clusters for this class
   */
  protected PInteger totalBucketNo;

  public CreateTypeAbstractStatement(final int id) {
    super(id);
  }

  protected abstract String commandType();

  protected abstract DocumentType createType(Schema schema);

  @Override
  public ResultSet executeDDL(final CommandContext context) {

    final Schema schema = context.getDatabase().getSchema();
    if (schema.existsType(name.getStringValue())) {
      if (ifNotExists) {
        return new InternalResultSet();
      } else {
        throw new CommandExecutionException("Type " + name + " already exists");
      }
    }
    checkSuperTypes(schema, context);

    final ResultInternal result = new ResultInternal();
    result.setProperty("operation", commandType());
    result.setProperty("typeName", name.getStringValue());

    final DocumentType[] superclasses = getSuperTypes(schema);

    final DocumentType type = createType(schema);

    for (final DocumentType c : superclasses)
      type.addSuperType(c);

    return new InternalResultSet(result);
  }

  protected DocumentType[] getSuperTypes(final Schema schema) {
    if (supertypes == null)
      return new EmbeddedDocumentType[] {};

    return supertypes.stream().map(x -> schema.getType(x.getStringValue())).filter(x -> x != null).collect(Collectors.toList())
        .toArray(new DocumentType[] {});
  }

  protected void checkSuperTypes(final Schema schema, final CommandContext context) {
    if (supertypes != null) {
      for (final Identifier superType : supertypes) {
        if (!schema.existsType(superType.getStringValue())) {
          throw new CommandExecutionException("Supertype '" + superType + "' not found");
        }
      }
    }
  }

  @Override
  public void toString(final Map<String, Object> params, final StringBuilder builder) {
    builder.append(commandType());
    builder.append(" ");
    name.toString(params, builder);
    if (ifNotExists) {
      builder.append(" IF NOT EXISTS");
    }
    if (supertypes != null && supertypes.size() > 0) {
      builder.append(" EXTENDS ");
      boolean first = true;
      for (final Identifier sup : supertypes) {
        if (!first) {
          builder.append(", ");
        }
        sup.toString(params, builder);
        first = false;
      }
    }
    if (buckets != null && buckets.size() > 0) {
      builder.append(" BUCKET ");
      boolean first = true;
      for (final BucketIdentifier bucket : buckets) {
        if (!first) {
          builder.append(",");
        }
        bucket.toString(params, builder);
        first = false;
      }
    }
    if (totalBucketNo != null) {
      builder.append(" BUCKETS ");
      totalBucketNo.toString(params, builder);
    }
  }

  protected CreateTypeAbstractStatement copy(final CreateTypeAbstractStatement result) {
    result.name = name == null ? null : name.copy();
    result.supertypes = supertypes == null ? null : supertypes.stream().map(x -> x.copy()).collect(Collectors.toList());
    result.buckets = buckets == null ? null : buckets.stream().map(x -> x.copy()).collect(Collectors.toList());
    result.totalBucketNo = totalBucketNo == null ? null : totalBucketNo.copy();
    result.ifNotExists = ifNotExists;
    return result;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final CreateTypeAbstractStatement that = (CreateTypeAbstractStatement) o;

    if (!Objects.equals(name, that.name))
      return false;
    if (!Objects.equals(supertypes, that.supertypes))
      return false;
    if (!Objects.equals(buckets, that.buckets))
      return false;
    if (!Objects.equals(totalBucketNo, that.totalBucketNo))
      return false;
    return ifNotExists == that.ifNotExists;
  }

  @Override
  public int hashCode() {
    int result = name != null ? name.hashCode() : 0;
    result = 31 * result + (supertypes != null ? supertypes.hashCode() : 0);
    result = 31 * result + (buckets != null ? buckets.hashCode() : 0);
    result = 31 * result + (totalBucketNo != null ? totalBucketNo.hashCode() : 0);
    return result;
  }

  protected List<com.arcadedb.engine.Bucket> getBuckets(final Schema schema) {
    final List<Bucket> bucketInstances = new ArrayList<>();
    for (final BucketIdentifier b : buckets) {
      if (!schema.existsBucket(b.bucketName.getStringValue()))
        schema.createBucket(b.bucketName.getStringValue());

      bucketInstances.add(b.bucketName != null ?
          schema.getBucketByName(b.bucketName.getStringValue()) :
          schema.getBucketById(b.bucketId.value.intValue()));
    }
    return bucketInstances;
  }
}
/* JavaCC - OriginalChecksum=4043013624f55fdf0ea8fee6d4f211b0 (do not edit this line) */
