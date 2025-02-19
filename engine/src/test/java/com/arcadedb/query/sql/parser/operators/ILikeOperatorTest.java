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
package com.arcadedb.query.sql.parser.operators;

import com.arcadedb.query.sql.executor.QueryHelper;
import com.arcadedb.query.sql.parser.ILikeOperator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * @author Luigi Dell'Aquila (luigi.dellaquila-(at)-gmail.com)
 */
public class ILikeOperatorTest {
  @Test
  public void test() {
    final ILikeOperator op = new ILikeOperator(-1);
    Assertions.assertTrue(op.execute(null, "FOOBAR", "%ooba%"));
    Assertions.assertTrue(op.execute(null, "FOOBAR", "%oo%"));
    Assertions.assertFalse(op.execute(null, "FOOBAR", "oo%"));
    Assertions.assertFalse(op.execute(null, "FOOBAR", "%oo"));
    Assertions.assertFalse(op.execute(null, "FOOBAR", "%fff%"));
    Assertions.assertTrue(op.execute(null, "FOOBAR", "foobar"));
    Assertions.assertTrue(op.execute(null, "100%", "100\\%"));
    Assertions.assertTrue(op.execute(null, "100%", "100%"));
  }

  @Test
  public void replaceSpecialCharacters() {
    Assertions.assertEquals("\\\\\\[\\]\\{\\}\\(\\)\\|\\*\\+\\$\\^\\...*", QueryHelper.convertForRegExp("\\[]{}()|*+$^.?%"));
  }
}
