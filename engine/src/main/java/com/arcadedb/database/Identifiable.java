/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.database;

public interface Identifiable {
  /**
   * Returns the RID (Record ID) for the current object.
   */
  RID getIdentity();

  /**
   * Returns the generic record.
   *
   * @param loadContent specifies if pre-load the record content
   */
  Record getRecord(boolean loadContent);

  /**
   * Returns the generic record by pre-loading also its content.
   */
  Record getRecord();

}
