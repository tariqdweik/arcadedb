/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.mongodbw;

import de.bwaldvogel.mongo.backend.AbstractMongoCollection;
import de.bwaldvogel.mongo.backend.DocumentComparator;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class MongoDBMemoryCollectionWrapper extends AbstractMongoCollection<Long> {
  private static final Logger         log            = LoggerFactory.getLogger(MongoDBMemoryCollectionWrapper.class);
  private              List<Document> documents      = new ArrayList<>();
  private              Queue<Long>    emptyPositions = new LinkedList<>();
  private              AtomicLong     dataSize       = new AtomicLong();

  public MongoDBMemoryCollectionWrapper(String databaseName, String collectionName, String idField) {
    super(databaseName, collectionName, idField);
  }

  protected void updateDataSize(long sizeDelta) {
    this.dataSize.addAndGet(sizeDelta);
  }

  protected long getDataSize() {
    return this.dataSize.get();
  }

  protected Long addDocumentInternal(Document document) {
    Long position = this.emptyPositions.poll();
    if (position == null) {
      position = (long) this.documents.size();
    }

    if (position == this.documents.size()) {
      this.documents.add(document);
    } else {
      this.documents.set(position.intValue(), document);
    }

    return position;
  }

  protected Iterable<Document> matchDocuments(Document query, Iterable<Long> positions, Document orderBy, int numberToSkip,
      int numberToReturn) throws MongoServerException {
    List<Document> matchedDocuments = new ArrayList();
    Iterator var7 = positions.iterator();

    while (var7.hasNext()) {
      Integer position = (Integer) var7.next();
      Document document = this.getDocument(position.longValue());
      if (this.documentMatchesQuery(document, query)) {
        matchedDocuments.add(document);
      }
    }

    this.sortDocumentsInMemory(matchedDocuments, orderBy);
    if (numberToSkip > 0) {
      matchedDocuments = ((List) matchedDocuments).subList(numberToSkip, matchedDocuments.size());
    }

    if (numberToReturn > 0 && matchedDocuments.size() > numberToReturn) {
      matchedDocuments = ((List) matchedDocuments).subList(0, numberToReturn);
    }

    return matchedDocuments;
  }

  protected Iterable<Document> matchDocuments(Document query, Document orderBy, int numberToSkip, int numberToReturn)
      throws MongoServerException {
    List<Document> matchedDocuments = new ArrayList();
    boolean ascending = true;
    if (orderBy != null && !orderBy.keySet().isEmpty() && orderBy.keySet().iterator().next().equals("$natural")) {
      int sortValue = (Integer) orderBy.get("$natural");
      if (sortValue == -1) {
        ascending = false;
      }
    }

    Iterator var9 = this.iterateAllDocuments(ascending).iterator();

    while (var9.hasNext()) {
      Document document = (Document) var9.next();
      if (this.documentMatchesQuery(document, query)) {
        matchedDocuments.add(document);
      }
    }

    if (orderBy != null && !orderBy.keySet().isEmpty() && !orderBy.keySet().iterator().next().equals("$natural")) {
      matchedDocuments.sort(new DocumentComparator(orderBy));
    }

    if (numberToSkip > 0) {
      if (numberToSkip >= matchedDocuments.size()) {
        return Collections.emptyList();
      }

      matchedDocuments = ((List) matchedDocuments).subList(numberToSkip, matchedDocuments.size());
    }

    if (numberToReturn > 0 && matchedDocuments.size() > numberToReturn) {
      matchedDocuments = ((List) matchedDocuments).subList(0, numberToReturn);
    }

    return matchedDocuments;
  }

  private Iterable<Document> iterateAllDocuments(boolean ascending) {
    return ascending ?
        new DocumentIterable(this.documents) :
        new ReverseDocumentIterable(this.documents);
  }

  public synchronized int count() {
    return this.documents.size() - this.emptyPositions.size();
  }

  protected Long findDocumentPosition(Document document) {
    long position = this.documents.indexOf(document);
    return position < 0 ? null : position;
  }

  protected int getRecordCount() {
    return this.documents.size();
  }

  protected int getDeletedCount() {
    return this.emptyPositions.size();
  }

  protected void removeDocument(Long position) {
    this.documents.set(position.intValue(), null);
    this.emptyPositions.add(position);
  }

  protected Document getDocument(Long position) {
    return this.documents.get(position.intValue());
  }

  public void drop() {
    log.debug("dropping {}", this);
  }

  protected void handleUpdate(Document document) {
  }

  private static class ReverseDocumentIterable implements Iterable<Document> {
    private List<Document> documents;

    public ReverseDocumentIterable(List<Document> documents) {
      this.documents = documents;
    }

    public Iterator<Document> iterator() {
      return new MongoDBMemoryCollectionWrapper.ReverseDocumentIterator(this.documents);
    }
  }

  private static class DocumentIterable implements Iterable<Document> {
    private List<Document> documents;

    public DocumentIterable(List<Document> documents) {
      this.documents = documents;
    }

    public Iterator<Document> iterator() {
      return new MongoDBMemoryCollectionWrapper.DocumentIterator(this.documents);
    }
  }

  private static class ReverseDocumentIterator extends MongoDBMemoryCollectionWrapper.AbstractDocumentIterator {
    protected ReverseDocumentIterator(List<Document> documents) {
      super(documents, documents.size() - 1);
    }

    protected Document getNext() {
      while (true) {
        if (this.pos >= 0) {
          Document document = this.documents.get(this.pos--);
          if (document == null) {
            continue;
          }

          return document;
        }

        return null;
      }
    }
  }

  private static class DocumentIterator extends MongoDBMemoryCollectionWrapper.AbstractDocumentIterator {
    protected DocumentIterator(List<Document> documents) {
      super(documents, 0);
    }

    protected Document getNext() {
      while (true) {
        if (this.pos < this.documents.size()) {
          Document document = this.documents.get(this.pos++);
          if (document == null) {
            continue;
          }

          return document;
        }

        return null;
      }
    }
  }

  private abstract static class AbstractDocumentIterator implements Iterator<Document> {
    protected       int            pos;
    protected final List<Document> documents;
    protected       Document       current;

    protected AbstractDocumentIterator(List<Document> documents, int pos) {
      this.documents = documents;
      this.pos = pos;
    }

    protected abstract Document getNext();

    public boolean hasNext() {
      if (this.current == null) {
        this.current = this.getNext();
      }

      return this.current != null;
    }

    public Document next() {
      Document document = this.current;
      this.current = this.getNext();
      return document;
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
