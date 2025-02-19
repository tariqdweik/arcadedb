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
package com.arcadedb.database.bucketselectionstrategy;

import com.arcadedb.database.Document;
import com.arcadedb.schema.EmbeddedDocumentType;
import com.arcadedb.serializer.json.JSONObject;

/**
 * Interface to delegate the assignment of the bucket based on document or keys.
 *
 * @author Luca Garulli
 */
public interface BucketSelectionStrategy {
  void setType(EmbeddedDocumentType type);

  int getBucketIdByRecord(Document record, boolean async);

  int getBucketIdByKeys(Object[] keyValues, boolean async);

  String getName();

  default JSONObject toJSON() {
    return new JSONObject().put("name", getName());
  }

  BucketSelectionStrategy copy();
}
