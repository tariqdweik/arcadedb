/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.arcadedb.sql.function;

import com.arcadedb.sql.executor.OSQLFunction;

/**
 * Abstract class to extend to build Custom SQL Functions. Extend it and register it with:
 * <code>OSQLParser.getInstance().registerStatelessFunction()</code> or
 * <code>OSQLParser.getInstance().registerStatefullFunction()</code> to being used by the SQL engine.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public abstract class OSQLFunctionAbstract implements OSQLFunction {
  protected String name;
  protected int    minParams;
  protected int    maxParams;

  public OSQLFunctionAbstract(final String iName, final int iMinParams, final int iMaxParams) {
    this.name = iName;
    this.minParams = iMinParams;
    this.maxParams = iMaxParams;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public int getMinParams() {
    return minParams;
  }

  @Override
  public int getMaxParams() {
    return maxParams;
  }

  @Override
  public String toString() {
    return name + "()";
  }

  @Override
  public void config(final Object[] iConfiguredParameters) {
  }

  @Override
  public boolean aggregateResults() {
    return false;
  }

  @Override
  public boolean filterResult() {
    return false;
  }

  @Override
  public Object getResult() {
    return null;
  }

  @Override
  public void setResult(final Object iResult) {
  }
}
