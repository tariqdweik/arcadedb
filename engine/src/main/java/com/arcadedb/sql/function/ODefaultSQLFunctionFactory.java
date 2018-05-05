/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.arcadedb.sql.function;

/**
 * Default set of SQL function.
 *
 * @author Johann Sorel (Geomatys)
 */
public final class ODefaultSQLFunctionFactory extends OSQLFunctionFactoryTemplate {
  public ODefaultSQLFunctionFactory() {
    // MISC FUNCTIONS
    register(OSQLFunctionCount.NAME, OSQLFunctionCount.class);
  }

}
