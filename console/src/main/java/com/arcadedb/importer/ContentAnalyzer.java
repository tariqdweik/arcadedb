/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LogManager;
import com.sun.xml.internal.stream.XMLInputFactoryImpl;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ContentAnalyzer {
  private String  url;
  private long    limitBytes       = 10000000;
  private long    limitEntries     = 0;
  private int     objectNestLevel  = 1;
  private int     maxValueSampling = 300;
  private boolean trimText         = true;

  public enum FILE_TYPE {JSON, XML, CSV, TSV}

  public ContentAnalyzer(final String[] args) {
    parseParameters(args);
  }

  public ContentAnalyzer(final String url) {
    this.url = url;
  }

  public static void main(final String[] args) throws IOException {
    new ContentAnalyzer(args).analyzeSchema();
  }

  public AbstractImporter.SourceInfo analyzeSchema() throws IOException {
    LogManager.instance().info(this, "Analyzing url: %s...", url);

    final Source source = getSource();

    final AbstractImporter.SourceInfo sourceInfo = analyzeText(source, limitBytes);

    if (sourceInfo == null)
      LogManager.instance().info(this, "Unknown format");
    else {
      LogManager.instance().info(this, "Recognized format %s", sourceInfo.fileType);
      if (!sourceInfo.options.isEmpty()) {
        for (Map.Entry<String, String> o : sourceInfo.options.entrySet())
          LogManager.instance().info(this, "- %s = %s", o.getKey(), o.getValue());
      }
    }

    source.close();

    return sourceInfo;
  }

  public Source getSource() throws IOException {
    final Source source;
    if (url.startsWith("http://") || url.startsWith("https://"))
      source = analyzeHttp(url);
    else
      source = analyzeFile(url);
    return source;
  }

  private Source analyzeHttp(final String url) throws IOException {
    final HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    connection.setRequestMethod("GET");
    connection.setDoOutput(true);

    connection.connect();

    return getSource(new BufferedInputStream(connection.getInputStream()), connection.getContentLengthLong(), new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        connection.disconnect();
        return null;
      }
    });
  }

  private Source analyzeFile(final String url) throws IOException {
    final File file = new File(url);
    final BufferedInputStream fis = new BufferedInputStream(new FileInputStream(file));

    return getSource(fis, file.length(), new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        fis.close();
        return null;
      }
    });
  }

  private AbstractImporter.SourceInfo analyzeText(final Source source, final long limit) throws IOException {
    final Parser parser = new Parser(source, limit);

    parser.nextChar();

    AbstractImporter.SourceInfo format = analyzeChar(parser);
    if (format != null)
      return format;

    parser.mark();

    // SKIP COMMENTS '#' IF ANY
    while (parser.isAvailable() && parser.getCurrentChar() == '#') {
      skipLine(parser);
      format = analyzeChar(parser);
      if (format != null)
        return format;
    }

    // SKIP COMMENTS '//' IF ANY
    parser.reset();

    while (parser.nextChar() == '/' && parser.nextChar() == '/') {
      skipLine(parser);
      format = analyzeChar(parser);
      if (format != null)
        return format;
    }

    // CHECK FOR CSV-LIKE FILES
    final Map<Character, AtomicInteger> candidateSeparators = new HashMap<>();

    while (parser.isAvailable() && parser.nextChar() != '\n') {
      final char c = parser.getCurrentChar();
      if (!Character.isLetterOrDigit(c)) {
        final AtomicInteger sep = candidateSeparators.get(c);
        if (sep == null) {
          candidateSeparators.put(c, new AtomicInteger(1));
        } else
          sep.incrementAndGet();
      }
    }

    if (!candidateSeparators.isEmpty()) {
      if (candidateSeparators.size() > 1) {
        final ArrayList<Map.Entry<Character, AtomicInteger>> list = new ArrayList(candidateSeparators.entrySet());
        list.sort(new Comparator<Map.Entry<Character, AtomicInteger>>() {
          @Override
          public int compare(final Map.Entry<Character, AtomicInteger> o1, final Map.Entry<Character, AtomicInteger> o2) {
            if (o1.getValue().get() == o2.getValue().get())
              return 0;
            return o1.getValue().get() > o2.getValue().get() ? 1 : -1;
          }
        });

        final Map.Entry<Character, AtomicInteger> bestSeparator = list.get(0);

        LogManager.instance().info(this, "Best separator candidate=%s (all candidates=%s)", bestSeparator.getKey(), list);

        if (bestSeparator.getKey() == ',')
          return new AbstractImporter.SourceInfo(FILE_TYPE.CSV, null);
        if (bestSeparator.getKey() == '\t')
          return new AbstractImporter.SourceInfo(FILE_TYPE.TSV, null);
        else
          return new AbstractImporter.SourceInfo(FILE_TYPE.TSV, null).set("separator", "" + bestSeparator.getKey());
      }
    }

    // UNKNOWN
    return null;
  }

  private void skipLine(final Parser parser) throws IOException {
    while (parser.isAvailable() && parser.nextChar() != '\n')
      ;
  }

  private AbstractImporter.SourceInfo analyzeChar(final Parser parser) {
    final char currentChar = parser.getCurrentChar();
    if (currentChar == '<')
      return analyzeXML(parser);
    else if (currentChar == '{')
      return new AbstractImporter.SourceInfo(FILE_TYPE.JSON, null);

    return null;
  }

  private AbstractImporter.SourceInfo analyzeXML(final Parser parser) {

    long parsedObjects = 0;
    final AnalyzedSchema schema = new AnalyzedSchema(maxValueSampling);

    final String currentUnit = parser.isCompressed() ? "uncompressed " : "";
    final String totalUnit = parser.isCompressed() ? "compressed " : "";

    try {

      parser.reset();

      final XMLInputFactoryImpl xmlFactory = new XMLInputFactoryImpl();
      final XMLStreamReader xmlReader = xmlFactory.createXMLStreamReader(parser.getInputStream());

      int nestLevel = 0;

      boolean parsedStructure = false;

      String entityName = null;
      String lastName = null;
      String lastContent = null;

      while (xmlReader.hasNext()) {
        final int eventType = xmlReader.next();

        switch (eventType) {
        case XMLStreamReader.COMMENT:
        case XMLStreamReader.SPACE:
          // IGNORE IT
          break;

        case XMLStreamReader.START_ELEMENT:
          LogManager.instance().debug(this, "<%s> attributes=%d (nestLevel=%d)", xmlReader.getName(), xmlReader.getAttributeCount(), nestLevel);

          if (nestLevel == objectNestLevel) {
            entityName = xmlReader.getName().toString();

            // GET ELEMENT'S ATTRIBUTES AS PROPERTIES
            for (int i = 0; i < xmlReader.getAttributeCount(); ++i) {
              schema.setProperty(entityName, xmlReader.getAttributeName(i).toString(), xmlReader.getAttributeValue(i));
              lastName = null;
            }
          } else if (nestLevel == objectNestLevel + 1) {
            // GET ELEMENT'S SUB-NODES AS PROPERTIES
            if (lastName != null)
              schema.setProperty(entityName, lastName, lastContent);

            lastName = xmlReader.getName().toString();
          }

          ++nestLevel;
          break;

        case XMLStreamReader.END_ELEMENT:
          if (lastName != null)
            schema.setProperty(entityName, lastName, lastContent);

          LogManager.instance().debug(this, "</%s> (nestLevel=%d)", xmlReader.getName(), nestLevel);

          --nestLevel;

          if (nestLevel == objectNestLevel) {
            ++parsedObjects;

            if (!parsedStructure)
              parsedStructure = true;

            if (parsedObjects % 10000 == 0) {
              LogManager.instance()
                  .info(this, "- Parsed %d XML objects (%s%s/%s%s)", parsedObjects, currentUnit, FileUtils.getSizeAsString(parser.getPosition()), totalUnit,
                      FileUtils.getSizeAsString(parser.getTotal()));

              if (parsedObjects % 100000 == 0)
                dumpSchema(schema, parsedObjects);
            }
          }
          break;

        case XMLStreamReader.ATTRIBUTE:
          ++nestLevel;
          LogManager.instance().debug(this, "- attribute %s attributes=%d (nestLevel=%d)", xmlReader.getName(), xmlReader.getAttributeCount(), nestLevel);
          break;

        case XMLStreamReader.CHARACTERS:
        case XMLStreamReader.CDATA:
          final String text = xmlReader.getText();
          if (!text.isEmpty() && !text.equals("\n")) {
            if (trimText)
              lastContent = text.trim();
            else
              lastContent = text;
          } else
            lastContent = null;
          break;

        default:
          // IGNORE IT
        }

        if (limitEntries > 0 && parsedObjects > limitEntries)
          break;
      }

    } catch (XMLStreamException e) {
      // IGNORE IT

    } catch (Exception e) {
      LogManager.instance().error(this, "Error on parsing XML", e);
      return null;
    }

    // END OF PARSING. THIS DETERMINES THE TYPE
    schema.endParsing();

    dumpSchema(schema, parsedObjects);

    return new AbstractImporter.SourceInfo(FILE_TYPE.XML, schema);
  }

  private void dumpSchema(final AnalyzedSchema schema, final long parsedObjects) {
    LogManager.instance().info(this, "---------------------------------------------------------------");

    LogManager.instance()
        .info(this, "XML objects found %d (limitBytes=%s limitEntries=%d)", parsedObjects, FileUtils.getSizeAsString(limitBytes), limitEntries);

    for (String entity : schema.getEntities()) {
      LogManager.instance().info(this, "---------------------------------------------------------------");
      LogManager.instance().info(this, "Entity '%s':", entity);

      for (Map.Entry<String, AnalyzedProperty> p : schema.getProperties(entity)) {
        LogManager.instance().info(this, "- %s (%s)", p.getKey(), p.getValue().getType());
        if (p.getValue().isCollectingSamples())
          LogManager.instance().info(this, "    contents (%d items): %s", p.getValue().getContents().size(), p.getValue().getContents());
      }
    }
    LogManager.instance().info(this, "---------------------------------------------------------------");
  }

  protected void parseParameters(final String[] args) {
    for (int i = 0; i < args.length - 1; i += 2)
      parseParameter(args[i], args[i + 1]);

    if (url == null)
      throw new IllegalArgumentException("Missing URL");
  }

  protected void parseParameter(final String name, final String value) {
    if ("-url".equals(name))
      url = value;
    else if ("-analyzeLimitBytes".equals(name))
      limitBytes = FileUtils.getSizeAsNumber(value);
    else if ("-analyzeLimitEntries".equals(name))
      limitEntries = Long.parseLong(value);
    else if ("-analyzeMaxValueSampling".equals(name))
      maxValueSampling = Integer.parseInt(value);
    else if ("-analyzeTrimText".equals(name))
      trimText = Boolean.parseBoolean(value);
    else if ("-objectNestLevel".equals(name))
      objectNestLevel = Integer.parseInt(value);
    else
      throw new IllegalArgumentException("Invalid setting '" + name + "'");
  }

  private Source getSource(final BufferedInputStream in, final long totalSize, final Callable<Void> closeCallback) throws IOException {
    in.mark(0);

    final ZipInputStream zip = new ZipInputStream(in);
    final ZipEntry entry = zip.getNextEntry();
    if (entry != null) {
      // ZIPPED FILE
      return new Source(url, zip, totalSize, true, closeCallback);
    }

    in.reset();
    in.mark(0);

    try {
      final GZIPInputStream gzip = new GZIPInputStream(in, 8192);
      return new Source(url, gzip, totalSize, true, closeCallback);
    } catch (IOException e) {
      // NOT GZIP
    }

    in.reset();

    // ANALYZE THE INPUT AS TEXT
    return new Source(url, in, totalSize, false, closeCallback);
  }
}
