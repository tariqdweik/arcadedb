package com.arcadedb.engine;

import com.arcadedb.database.PBinary;
import com.arcadedb.database.PDatabase;
import com.arcadedb.database.PRID;
import com.arcadedb.database.PRecord;
import com.arcadedb.exception.PDatabaseOperationException;
import com.arcadedb.exception.PRecordNotFoundException;
import com.arcadedb.utility.PLogManager;

import java.io.IOException;
import java.util.Iterator;

import static com.arcadedb.database.PBinary.INT_SERIALIZED_SIZE;

/**
 * PAGE CONTENT = [version(long:8),recordCountInPage(short:2),recordOffsetsInPage(2048*uint=8192)]
 */
public class PBucket extends PPaginatedComponent {
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
  public PBucket(final PDatabase database, final String name, final String filePath, final PPaginatedFile.MODE mode,
      final int pageSize) throws IOException {
    super(database, name, filePath, database.getFileManager().newFileId(), BUCKET_EXT, mode, pageSize);
  }

  /**
   * Called at load time.
   */
  public PBucket(final PDatabase database, final String name, final String filePath, final int id, final PPaginatedFile.MODE mode,
      final int pageSize) throws IOException {
    super(database, name, filePath, id, mode, pageSize);
  }

  public PRID createRecord(final PRecord record) {
    final PBinary buffer = database.getSerializer().serialize(database, record, id);

    if (buffer.size() > pageSize - CONTENT_HEADER_SIZE)
      // TODO: SUPPORT MULTI-PAGE CONTENT
      throw new PDatabaseOperationException("Record too big to be stored, size=" + buffer.size());

    try {
      int newPosition = -1;
      PModifiablePage lastPage = null;
      int recordCountInPage = -1;
      boolean createNewPage = false;

      final int txPageCounter = getTotalPages();

      if (txPageCounter > 0) {
        lastPage = database.getTransaction().getPageToModify(new PPageId(file.getFileId(), txPageCounter - 1), pageSize, false);
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
        lastPage = database.getTransaction().addPage(new PPageId(file.getFileId(), txPageCounter), pageSize);
        //lastPage.blank(0, CONTENT_HEADER_SIZE);
        newPosition = CONTENT_HEADER_SIZE;
        recordCountInPage = 0;
      }

      final PRID rid = new PRID(database, file.getFileId(),
          lastPage.getPageId().getPageNumber() * MAX_RECORDS_IN_PAGE + recordCountInPage);

      lastPage.writeBytes(newPosition, buffer.toByteArray());

      lastPage.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + recordCountInPage * INT_SERIALIZED_SIZE, newPosition);

      lastPage.writeShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET, (short) ++recordCountInPage);

      PLogManager.instance().debug(this, "Created record %s (page=%s records=%d threadId=%d)", rid, lastPage, recordCountInPage,
          Thread.currentThread().getId());

      return rid;

    } catch (IOException e) {
      throw new PDatabaseOperationException("Cannot add a new record to the bucket '" + name + "'", e);
    }
  }

  public void updateRecord(final PRecord record) {
    final PBinary buffer = database.getSerializer().serialize(database, record, id);
    final PRID rid = record.getIdentity();

    final int pageId = (int) rid.getPosition() / PBucket.MAX_RECORDS_IN_PAGE;
    final int positionInPage = (int) (rid.getPosition() % PBucket.MAX_RECORDS_IN_PAGE);

    if (pageId >= pageCount.get()) {
      int txPageCount = getTotalPages();
      if (pageId >= txPageCount)
        throw new PRecordNotFoundException("Record " + rid + " not found", rid);
    }

    try {
      final PModifiablePage page = database.getTransaction()
          .getPageToModify(new PPageId(file.getFileId(), pageId), pageSize, false);
      final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      if (positionInPage >= recordCountInPage)
        throw new PRecordNotFoundException("Record " + rid + " not found", rid);

      final int recordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE);
      final long recordSize[] = page.readNumberAndSize(recordPositionInPage);
      if (recordSize[0] == 0)
        // DELETED
        throw new PRecordNotFoundException("Record " + rid + " not found", rid);

      if (buffer.size() > recordSize[0])
        throw new IllegalArgumentException(
            "Record " + rid + " cannot be updated because the size (" + buffer.size() + ") is major than the existent one ("
                + recordSize[0] + ")");

      if (buffer.size() < recordSize[0])
        // UPDATE THE SIZE. THE REMAINING SPACE IS UNUSED
        recordSize[1] = page.writeNumber(recordPositionInPage, buffer.size());

      final int recordContentPositionInPage = (int) (recordPositionInPage + recordSize[1]);

      page.writeByteArray(recordContentPositionInPage, buffer.toByteArray());

      PLogManager.instance().debug(this, "Updated record %s (page=%s threadId=%d)", rid, page, Thread.currentThread().getId());

    } catch (IOException e) {
      throw new PDatabaseOperationException("Error on update record " + rid);
    }
  }

  public PBinary getRecord(final PRID rid) {
    final int pageId = (int) rid.getPosition() / PBucket.MAX_RECORDS_IN_PAGE;
    final int positionInPage = (int) (rid.getPosition() % PBucket.MAX_RECORDS_IN_PAGE);

    if (pageId >= pageCount.get()) {
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

      assert recordSize[0] > -1;
      assert recordSize[1] > -1;

      if (recordSize[0] == 0)
        // DELETED
        throw new PRecordNotFoundException("Record " + rid + " not found", rid);

      final int recordContentPositionInPage = (int) (recordPositionInPage + recordSize[1]);

      return page.getImmutableView(recordContentPositionInPage, (int) recordSize[0]);

    } catch (IOException e) {
      throw new PDatabaseOperationException("Error on lookup of record " + rid);
    }
  }

  public boolean existsRecord(final PRID rid) {
    final int pageId = (int) rid.getPosition() / PBucket.MAX_RECORDS_IN_PAGE;
    final int positionInPage = (int) (rid.getPosition() % PBucket.MAX_RECORDS_IN_PAGE);

    if (pageId >= pageCount.get()) {
      int txPageCount = getTotalPages();
      if (pageId >= txPageCount)
        return false;
    }

    try {
      final PBasePage page = database.getTransaction().getPage(new PPageId(file.getFileId(), pageId), pageSize);

      final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);
      if (positionInPage >= recordCountInPage)
        return false;

      final int recordPositionInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + positionInPage * INT_SERIALIZED_SIZE);
      final long recordSize[] = page.readNumberAndSize(recordPositionInPage);

      assert recordSize[0] > -1;
      assert recordSize[1] > -1;

      return recordSize[0] > 0;

    } catch (IOException e) {
      throw new PDatabaseOperationException("Error on checking record existence for " + rid);
    }
  }

  public void deleteRecord(final PRID rid) {
    final int pageId = (int) rid.getPosition() / PBucket.MAX_RECORDS_IN_PAGE;
    final int positionInPage = (int) (rid.getPosition() % PBucket.MAX_RECORDS_IN_PAGE);

    if (pageId >= pageCount.get()) {
      int txPageCount = getTotalPages();
      if (pageId >= txPageCount)
        throw new PRecordNotFoundException("Record " + rid + " not found", rid);
    }

    try {
      final PModifiablePage page = database.getTransaction()
          .getPageToModify(new PPageId(file.getFileId(), pageId), pageSize, false);
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

// COMMENTED BECAUSE CAUSED CORRUPTION
//
//      // COMPACT PAGE BY SHIFTING THE RECORDS TO THE LEFT
//      for (int pos = positionInPage + 1; pos < recordCountInPage; ++pos) {
//        final int nextRecordPosInPage = (int) page.readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + pos * INT_SERIALIZED_SIZE);
//        final byte[] record = page.readBytes(nextRecordPosInPage);
//
//        final int newPosition = nextRecordPosInPage - (int) removedRecordSize[0];
//        page.writeBytes(newPosition, record);
//
//        // OVERWRITE POS TABLE WITH NEW POSITION
//        page.writeUnsignedInt(PAGE_RECORD_TABLE_OFFSET + pos * INT_SERIALIZED_SIZE, newPosition);
//      }

      PLogManager.instance().debug(this, "Deleted record %s (page=%s threadId=%d)", rid, page, Thread.currentThread().getId());

    } catch (IOException e) {
      throw new PDatabaseOperationException("Error on deletion of record " + rid);
    }
  }

  public void scan(final PRawRecordCallback callback) throws IOException {
    final int txPageCount = getTotalPages();

    try {
      for (int pageId = 0; pageId < txPageCount; ++pageId) {
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

              final PRID rid = new PRID(database, id, pageId * MAX_RECORDS_IN_PAGE + recordIdInPage);

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
    return new PBucketIterator(this, database);
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

  public long count() {
    long total = 0;

    final int txPageCount = getTotalPages();

    try {
      for (int pageId = 0; pageId < txPageCount; ++pageId) {
        final PBasePage page = database.getTransaction().getPage(new PPageId(file.getFileId(), pageId), pageSize);
        final short recordCountInPage = page.readShort(PAGE_RECORD_COUNT_IN_PAGE_OFFSET);

        if (recordCountInPage > 0) {
          for (int recordIdInPage = 0; recordIdInPage < recordCountInPage; ++recordIdInPage) {
            final int recordPositionInPage = (int) page
                .readUnsignedInt(PAGE_RECORD_TABLE_OFFSET + recordIdInPage * INT_SERIALIZED_SIZE);
            final long recordSize[] = page.readNumberAndSize(recordPositionInPage);

            if (recordSize[0] > 0)
              total++;

          }
        }
      }
    } catch (IOException e) {
      throw new PDatabaseOperationException("Cannot count bucket '" + name + "'", e);
    }
    return total;
  }
}
