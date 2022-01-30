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
/* Generated by: JJTree: Do not edit this line. BackupDatabaseStatement.java Version 1.1 */
/* ParserGeneratorCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.arcadedb.query.sql.parser;

import com.arcadedb.database.Database;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.InternalResultSet;
import com.arcadedb.query.sql.executor.ResultInternal;
import com.arcadedb.query.sql.executor.ResultSet;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Objects;

public class BackupDatabaseStatement extends SimpleExecStatement {
  protected Url url;

  public BackupDatabaseStatement(int id) {
    super(id);
  }

  public BackupDatabaseStatement(SqlParser p, int id) {
    super(p, id);
  }

  @Override
  public ResultSet executeSimple(CommandContext ctx) {
    String targetUrl = this.url != null ? this.url.getUrlString() : null;
    ResultInternal result = new ResultInternal();
    result.setProperty("operation", "backup database");
    if (targetUrl != null)
      result.setProperty("target", targetUrl);

    if (ctx.getDatabase().isTransactionActive())
      ctx.getDatabase().rollback();

    try {
      final Class<?> clazz = Class.forName("com.arcadedb.integration.backup.Backup");
      final Object backup = clazz.getConstructor(Database.class, String.class).newInstance(ctx.getDatabase(), targetUrl);

      // ASSURE THE DIRECTORY CANNOT BE CHANGED
      clazz.getMethod("setDirectory", String.class).invoke(backup, "backups/" + ctx.getDatabase().getName());
      clazz.getMethod("setVerboseLevel", Integer.TYPE).invoke(backup, 1);
      final String backupFile = (String) clazz.getMethod("backupDatabase").invoke(backup);

      result.setProperty("result", "OK");
      result.setProperty("backupFile", backupFile);

      InternalResultSet rs = new InternalResultSet();
      rs.add(result);
      return rs;

    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException e) {
      throw new CommandExecutionException("Error on backing up database, backup libs not found in classpath", e);
    } catch (InvocationTargetException e) {
      throw new CommandExecutionException("Error on backing up database", e.getTargetException());
    }
  }

  @Override
  public void toString(Map<String, Object> params, StringBuilder builder) {
    builder.append("BACKUP DATABASE");
    if (url != null) {
      builder.append(' ');
      url.toString(params, builder);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    BackupDatabaseStatement that = (BackupDatabaseStatement) o;
    return Objects.equals(url, that.url);
  }

  @Override
  public int hashCode() {
    return Objects.hash(url);
  }

  @Override
  public Statement copy() {
    BackupDatabaseStatement result = new BackupDatabaseStatement(-1);
    result.url = this.url;
    return result;
  }
}
/* ParserGeneratorCC - OriginalChecksum=049abbf81eb40fcdc167940a6a34382b (do not edit this line) */
