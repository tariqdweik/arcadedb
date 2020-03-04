/*
 * Copyright (c) - Arcade Data LTD (https://arcadedata.com)
 */

package com.arcadedb.importer;

import java.util.HashMap;
import java.util.Map;

public class SourceSchema {
  private final ContentImporter     contentImporter;
  private final Source              source;
  private final AnalyzedSchema      schema;
  private final Map<String, String> options = new HashMap<>();

  public SourceSchema(final ContentImporter contentImporter, final Source source, final AnalyzedSchema schema) {
    this.contentImporter = contentImporter;
    this.source = source;
    this.schema = schema;
  }

  public SourceSchema set(final String option, final String value) {
    options.put(option, value);
    return this;
  }

  public AnalyzedSchema getSchema() {
    return schema;
  }

  public Source getSource() {
    return source;
  }

  public Map<String, String> getOptions() {
    return options;
  }

  public ContentImporter getContentImporter() {
    return contentImporter;
  }
}
