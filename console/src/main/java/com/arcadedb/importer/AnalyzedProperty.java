/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.schema.Type;

import java.util.HashSet;
import java.util.Set;

public class AnalyzedProperty {
  private final String      name;
  private final long        maxValueSampling;
  private       Type        type;
  private final int         index;
  private       String      lastContent;
  private       Set<String> contents            = new HashSet<>();
  private       boolean     candidateForInteger = true;
  private       boolean     candidateForDecimal = true;
  private       boolean     collectingSamples   = true;

  public AnalyzedProperty(final String name, final Type type, final long maxValueSampling, final int index) {
    this.name = name;
    this.type = type;
    this.maxValueSampling = maxValueSampling;
    this.index = index;
  }

  public String getName() {
    return name;
  }

  public Type getType() {
    return type;
  }

  public void endParsing() {
    if (lastContent != null)
      if (candidateForInteger)
        type = Type.LONG;
      else if (candidateForDecimal)
        type = Type.DOUBLE;
  }

  public int getIndex() {
    return index;
  }

  public void setLastContent(String lastContent) {
    if (!collectingSamples)
      return;

    if (lastContent != null) {
      if (lastContent.length() > 100) {
        collectingSamples = false;
        contents.clear();
        return;
      }

      this.lastContent = lastContent;
      if (contents.size() > maxValueSampling) {
        collectingSamples = false;
        contents.clear();
        return;
      }

      contents.add(lastContent);

      if (!lastContent.isEmpty()) {
        if (candidateForInteger) {
          try {
            Long.parseLong(lastContent);
          } catch (NumberFormatException e) {
            candidateForInteger = false;
          }
        }

        if (candidateForDecimal) {
          try {
            Double.parseDouble(lastContent);
          } catch (NumberFormatException e) {
            candidateForDecimal = false;
          }
        }
      }
    }
  }

  public Set<String> getContents() {
    return contents;
  }

  public boolean isCollectingSamples() {
    return collectingSamples;
  }

  @Override
  public String toString() {
    return name;
  }
}
