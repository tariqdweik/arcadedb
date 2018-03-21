package com.arcadedb.engine;

import com.arcadedb.database.PBinary;
import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PRID;
import com.arcadedb.database.PRecord;
import com.arcadedb.exception.PDatabaseOperationException;
import com.arcadedb.exception.PRecordNotFoundException;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import static com.arcadedb.database.PBinary.INT_SERIALIZED_SIZE;

/**
 * HEADER = [recordCount(int:4)] CONTENT-PAGES = [version(long:8),recordCountInPage(short:2),recordOffsetsInPage(2048*uint=8192)]
 */
public class PBucket extends PPaginatedFile {
  public static final String BUCKET_EXT          = "pbucket";
  public static final int    MAX_RECORDS_IN_PAGE = 2048;
  public static final int    DEF_PAGE_SIZE       = 65536;


  protected static final int PAGE_RECORD_COUNT_IN_PAGE_OFFSET = 0;
  protected static final int PAGE_RECORD_TABLE_OFFSET         = PAGE_RECORD_COUNT_IN_PAGE_OFFSET + PBinary.SHORT_SERIALIZED_SIZE;
  protected static final int CONTENT_HEADER_SIZE              =
      PAGE_RECORD_TABLE_OFFSET + (MAX_RECORDS_IN_PAGE * INT_SERIALIZED_SIZE);

  /**
   * Called at creation time.
   */
  public PBucket(final PDatabase database, final String name, final String filePath, final PFile.MODE mode, final int pageSize)
      throws IOException {
    super(database, name, filePath, database.getFileManager().newFileId(), BUCKET_EXT, mode, pageSize);

    // NEW FILE, CREATE HEADER PAGE
    final boolean txActive = database.isTransactionActive();
    if (!txActive)
      this.database.begin();
    final PModifiablePage header = this.database.getTransaction().getPageToModify(new PPageId(file.getFileId(), 0), pageSize);
    header.writeInt(0, 0);
    if (!txActive)
      this.database.commit();
    pageCount = 1;
  }

  /**
   * Called at load time.
   */
  public PBucket(final PDatabase database, final String name, final String filePath, final int id, final PFile.MODE mode,
      final int pageSize) throws IOException {
    super(database, name, filePath, id, mode, pageSize);
  }

  public PRID addRecord(final PRecord record) {
    final PBinary buffer = database.getSerializer().serialize(database, record);

    if (buffer.size() > pageSize - CONTENT_HEADER_SIZE)
      // TODO: SUPPORT MULTI-PAGE CONTENT
      throw new PDatabaseOperationException("Record too big to be stored, size=" + buffer.size());

    try {
      int newPosition = -1;
      PModifiablePage lastPage = null;
      int recordCountInPage = -1;
      boolean createNewPage = false;

      final int txPageCounter = getTotalPages();

      if (txPageCounter > 1) {
        lastPage = database.getTransaction().getPageToModify(new PPageId(file.getFileId(), txPageCounter - 1), pageSize);
        recordCountInPage = lastPage.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
        if (recordCountInPage >= MAX_RECORDS_IN_PAGE)
          // RECORD TOO BIG FOR THIS PAGE, USE A NEW PAGE
          createNewPage = true;
        else if (recordCountInPage > 0) {
          // GET FIRST EMPTY POSITION
          final int lastRecordPositionInPage = (int) lastPage
              .readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + (recordCountInPage - 1) * INT_SERIALIZED_SIZE);
          final long[] lastRecordSize = lastPage.readNumberAndSize(lastRecordPositionInPage);
          newPosition = lastRecordPositionInPage + (int) lastRecordSize[0] + (int) lastRecordSize[1];

          if (newPosition + INT_SERIALIZED_SIZE + buffer.size() > lastPage.getMaxContentSize())
            // RECORD TOO BIG FOR THIS PAGE, USE A NEW PAGE
            createNewPage = true;

        } else
          // FIRST RECORD, START RIGHT AFTER THE HEADER
          newPosition = CONTENT_HEADER_SIZE;
      } else
        createNewPage = true;

      if (createNewPage) {
        if (lastPage != null)
          database.getTransaction().addPageToDispose(lastPage.pageId);

        lastPage = database.getTransaction().addPage(new PPageId(file.getFileId(), txPageCounter), pageSize);
        lastPage.blank(0, CONTENT_HEADER_SIZE);
        newPosition = CONTENT_HEADER_SIZE;
        recordCountInPage = 0;
      }

      final PRID rid = new PRID(database, file.getFileId(),
          (lastPage.getPageId().getPageNumber() - 1) * MAX_RECORDS_IN_PAGE + recordCountInPage);

      lastPage.writeBytes(newPosition, buffer.toByteArray());

      lastPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + recordCountInPage * INT_SERIALIZED_SIZE, newPosition);

      lastPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) ++recordCountInPage);

      incrementRecordCount(1);

      return rid;

    } catch (IOException e) {
      throw new PDatabaseOperationException("Cannot add a new record to the bucket '" + name + "'", e);
    }
  }

  public PBinary getRecord(final PRID rid) {
    final int pageId = (int) (rid.getPosition() / PBucket.MAX_RECORDS_IN_PAGE + 1);
    final int positionInPage = (int) (rid.getPosition() % PBucket.MAX_RECORDS_IN_PAGE);

    if (pageId >= pageCount) {
      int txPageCount = getTotalPages();
      if (pageId >= txPageCount)
        throw new PRecordNotFoundException("Record " + rid + " not found", rid);
    }

    try {
      final PBasePage page = database.getTransaction().getPage(new PPageId(file.getFileId(), pageId), pageSize);
      final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      if (positionInPage >= recordCountInPage)
        throw new PRecordNotFoundException("Record " + rid + " not found", rid);

      final int recordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE);
      final long recordSize[] = page.readNumberAndSize(recordPositionInPage);
      if (recordSize[0] == 0)
        // DELETED
        throw new PRecordNotFoundException("Record " + rid + " not found", rid);

      final int recordContentPositionInPage = (int) (recordPositionInPage + recordSize[1]);

      return page.getImmutableView(recordContentPositionInPage, (int) recordSize[0]);

    } catch (IOException e) {
      throw new PDatabaseOperationException("Error on lookup of record " + rid);
    }
  }

  public void deleteRecord(final PRID rid) {
    final int pageId = (int) (rid.getPosition() / PBucket.MAX_RECORDS_IN_PAGE + 1);
    final int positionInPage = (int) (rid.getPosition() % PBucket.MAX_RECORDS_IN_PAGE);

    if (pageId >= pageCount) {
      int txPageCount = getTotalPages();
      if (pageId >= txPageCount)
        throw new PRecordNotFoundException("Record " + rid + " not found", rid);
    }

    try {
      final PModifiablePage page = database.getTransaction().getPageToModify(new PPageId(file.getFileId(), pageId), pageSize);
      final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      if (positionInPage >= recordCountInPage)
        throw new PRecordNotFoundException("Record " + rid + " not found", rid);

      final int recordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE);
      final long[] removedRecordSize = page.readNumberAndSize(recordPositionInPage);
      if (removedRecordSize[0] == 0)
        // ALREADY DELETED
        throw new PRecordNotFoundException("Record " + rid + " not found", rid);

      // CONTENT SIZE = 0 MEANS DELETED
      page.writeNumber(recordPositionInPage, 0);

      // COMPACT PAGE BY SHIFTING THE RECORDS TO THE LEFT
      for (int pos = positionInPage + 1; pos < recordCountInPage; ++pos) {
        final int nextRecordPosInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + pos * INT_SERIALIZED_SIZE);
        final byte[] record = page.readBytes(nextRecordPosInPage);

        final int newPosition = nextRecordPosInPage - (int) removedRecordSize[0];
        page.writeBytes(newPosition, record);

        // OVERWRITE POS TABLE WITH NEW POSITION
        page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + pos * INT_SERIALIZED_SIZE, newPosition);
      }

      incrementRecordCount(-1);

    } catch (IOException e) {
      throw new PDatabaseOperationException("Error on deletion of record " + rid);
    }
  }

  public void scan(final PRawRecordCallback callback) throws IOException {
    if (count() == 0)
      return;

    final int txPageCount = getTotalPages();

    try {
      for (int pageId = 1; pageId < txPageCount; ++pageId) {
        final PBasePage page = database.getTransaction().getPage(new PPageId(file.getFileId(), pageId), pageSize);
        final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);

        if (recordCountInPage > 0) {
          for (int recordIdInPage = 0; recordIdInPage < recordCountInPage; ++recordIdInPage) {
            final int recordPositionInPage = (int) page
                .readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + recordIdInPage * INT_SERIALIZED_SIZE);
            final long recordSize[] = page.readNumberAndSize(recordPositionInPage);

            if (recordSize[0] > 0) {
              // NOT DELETED
              final int recordContentPositionInPage = recordPositionInPage + (int) recordSize[1];

              final PRID rid = new PRID(database, id, (pageId - 1) * MAX_RECORDS_IN_PAGE + recordIdInPage);

              final PBinary view = page.getImmutableView(recordContentPositionInPage, (int) recordSize[0]);

              if (!callback.onRecord(rid, view))
                return;
            }
          }
        }
      }
    } catch (IOException e) {
      throw new PDatabaseOperationException("Cannot scan bucket '" + name + "'", e);
    }
  }

  public Iterator<PRecord> iterator() throws IOException {
    if (count() == 0)
      return Collections.emptyIterator();

    Iterator<PRecord> result = new PBucketIterator(this, database);

    return result;
  }

  public int getId() {
    return id;
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof PBucket))
      return false;

    return ((PBucket) obj).id == this.id;
  }

  public long count() throws IOException {
    final PBasePage header = this.database.getTransaction().getPage(new PPageId(file.getFileId(), 0), pageSize);
    return header.readInt(0);
  }

  private void incrementRecordCount(final long delta) throws IOException {
    final PModifiablePage header = this.database.getTransaction().getPageToModify(new PPageId(file.getFileId(), 0), pageSize);
    final int recordCount = header.readInt(0);
    header.writeInt(0, (int) (recordCount + delta));
  }
}
