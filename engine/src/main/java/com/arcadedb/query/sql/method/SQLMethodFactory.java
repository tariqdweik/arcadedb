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
 */
package com.arcadedb.query.sql.method;

import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.SQLMethod;

/**
 * @author Johann Sorel (Geomatys)
 */
public interface SQLMethodFactory {
  /**
   * Create method for the given name. returned method may be a new instance each time or a constant.
   *
   * @param name
   *
   * @return OSQLMethod : created method
   *
   * @throws CommandExecutionException : when method creation fail
   */
  SQLMethod createMethod(String name) throws CommandExecutionException;
}
