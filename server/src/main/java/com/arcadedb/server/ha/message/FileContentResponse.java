/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */
package com.arcadedb.server.ha.message;

import com.arcadedb.database.Binary;
import com.arcadedb.database.Database;
import com.arcadedb.engine.BasePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.network.binary.NetworkProtocolException;
import com.arcadedb.server.ha.HAServer;

import java.io.IOException;

public class FileContentResponse implements HAResponseMessage<FileContentRequest> {
  public final static byte    ID = FileContentRequest.ID;
  private             Binary  pagesContent;
  private             int     pages;
  private             boolean last;

  public FileContentResponse() {
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
  public void toStream(final Binary stream) {
    stream.putByte(ID);

    stream.putNumber(pages);
    stream.putBytes(pagesContent.getContent());
    stream.putByte((byte) (last ? 1 : 0));
  }

  @Override
  public void fromStream(final Binary stream) {
    // SKIP THE COMMAND ID
    stream.getByte();

    pages = (int) stream.getNumber();
    pagesContent = new Binary(stream.getBytes());
    pagesContent.flip();

    last = stream.getByte() == 1;
  }

  @Override
  public void build(final HAServer server, final FileContentRequest request) {
    final Database db = server.getServer().getDatabase(request.getDatabaseName());
    final int pageSize = db.getFileManager().getFile(request.getFileId()).getPageSize();

    try {
      final int totalPages = (int) (db.getFileManager().getFile(request.getFileId()).getSize() / pageSize);

      pagesContent = new Binary();

      pages = 0;

      for (int i = request.getFrom(); i < totalPages && pages < 10; ++i) {
        final PageId pageId = new PageId(request.getFileId(), i);
        final BasePage page = db.getPageManager().getPage(pageId, pageSize, false);
        pagesContent.putByteArray(page.getContent().array(), pageSize);

        ++pages;
      }

      last = pages >= totalPages;

      pagesContent.flip();

    } catch (IOException e) {
      throw new NetworkProtocolException("Cannot load pages");
    }
  }

  @Override
  public String toString() {
    return "file=" + pages + " pages (" + pagesContent.size() + " bytes)";
  }
}
