/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.server.ha.HAServer;

public class FileContentResponse implements HACommand {
  private Binary  pagesContent;
  private int     pages;
  private boolean last;

  public FileContentResponse() {
  }

  public FileContentResponse(final Binary pagesContent, final int pages, final boolean last) {
    this.pagesContent = pagesContent;
    this.pages = pages;
    this.last = last;
  }

  public Binary getPagesContent() {
    return pagesContent;
  }

  public int getPages() {
    return pages;
  }

  public boolean isLast() {
    return last;
  }

  @Override
  public HACommand execute(HAServer server, String remoteServerName) {
    return null;
  }

  @Override
  public void toStream(final Binary stream) {
    stream.putNumber(pages);
    stream.putBytes(pagesContent.getContent());
    stream.putByte((byte) (last ? 1 : 0));
  }

  @Override
  public void fromStream(final Binary stream) {
    pages = (int) stream.getNumber();
    pagesContent = new Binary(stream.getBytes());
    pagesContent.flip();
    last = stream.getByte() == 1;
  }

  @Override
  public String toString() {
    return "file=" + pages + " pages (" + pagesContent.size() + " bytes)";
  }
}
