/*
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at  http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  *  For more information: http://orientdb.com
 *
 */
package com.arcadedb.utility;

/**
 * Container for pair of non null objects.
 */
public class Pair<V1, V2> {
  private final V1 first;
  private final V2 second;

  public Pair(V1 first, V2 second) {
    this.first = first;
    this.second = second;
  }

  public V1 getFirst() {
    return first;
  }

  public V2 getSecond() {
    return second;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    Pair<?, ?> oRawPair = (Pair<?, ?>) o;

    if (!first.equals(oRawPair.first))
      return false;
    return second.equals(oRawPair.second);

  }

  @Override
  public int hashCode() {
    int result = first.hashCode();
    result = 31 * result + second.hashCode();
    return result;
  }
}
