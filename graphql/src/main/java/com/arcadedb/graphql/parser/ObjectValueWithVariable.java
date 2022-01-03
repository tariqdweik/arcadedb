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
/* Generated by: JJTree: Do not edit this line. ObjectValueWithVariable.java Version 1.1 */
/* ParserGeneratorCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.arcadedb.graphql.parser;

import java.util.*;

public class ObjectValueWithVariable extends AbstractValue {
  protected List<ObjectFieldWithValue> values = new ArrayList<>();

  public ObjectValueWithVariable(int id) {
    super(id);
  }

  public ObjectValueWithVariable(GraphQLParser p, int id) {
    super(p, id);
  }

  @Override
  public Object getValue() {
    return values;
  }

  /**
   * Accept the visitor.
   **/
  public Object jjtAccept(GraphQLParserVisitor visitor, Object data) {
    return visitor.visit(this, data);
  }

  @Override
  public String toString() {
    return "ObjectValueWithVariable{" + values + '}';
  }
}
/* ParserGeneratorCC - OriginalChecksum=db23ca8591e199b1d772ae148743a174 (do not edit this line) */
