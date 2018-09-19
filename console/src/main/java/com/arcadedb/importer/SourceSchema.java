/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import java.util.HashMap;
import java.util.Map;

public class SourceSchema {
  private final Source                    source;
  private final SourceDiscovery.FILE_TYPE fileType;
  private final AnalyzedSchema            schema;
  private final Map<String, String>       options = new HashMap<>();

  public SourceSchema(final Source source, final SourceDiscovery.FILE_TYPE fileType, final AnalyzedSchema schema) {
    this.source = source;
    this.fileType = fileType;
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

  public SourceDiscovery.FILE_TYPE getFileType() {
    return fileType;
  }

  public Map<String, String> getOptions() {
    return options;
  }
}
