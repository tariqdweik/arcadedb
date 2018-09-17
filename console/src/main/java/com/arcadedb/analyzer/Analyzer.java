/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.analyzer;

import com.arcadedb.importer.AbstractImporter;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LogManager;
import com.sun.xml.internal.stream.XMLInputFactoryImpl;

import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class Analyzer {
  private String  url;
  private long    limitBytes       = 1000000;
  private long    limitEntries     = 0;
  private int     objectNestLevel  = 1;
  private int     maxValueSampling = 100;
  private boolean trimText         = true;

  public enum FILE_TYPE {JSON, XML, CSV, TSV}

  public Analyzer(final String[] args) {
    parseParameters(args);
  }

  public static void main(final String[] args) throws IOException {
    new Analyzer(args).analyze();
  }

  public void analyze() throws IOException {
    final AbstractImporter.ImporterConfiguration format;

    LogManager.instance().info(this, "Analyzing url: %s...", url);

    if (url.startsWith("http://") || url.startsWith("https://"))
      format = analyzeHttp(url);
    else
      format = analyzeFile(url);

    if (format == null)
      LogManager.instance().info(this, "Unknown format");
    else {
      LogManager.instance().info(this, "Recognized format %s", format.fileType);
      if (!format.options.isEmpty()) {
        for (Map.Entry<String, String> o : format.options.entrySet())
          LogManager.instance().info(this, "- %s = %s", o.getKey(), o.getValue());
      }
    }
  }

  private AbstractImporter.ImporterConfiguration analyzeHttp(final String url) throws IOException {
    final HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
    connection.setRequestMethod("GET");
    connection.setDoOutput(true);

    try {
      connection.connect();
      final int contentLength = connection.getContentLength();
      return analyzeInputStream(new BufferedInputStream(connection.getInputStream()), contentLength);
    } finally {
      connection.disconnect();
    }
  }

  private AbstractImporter.ImporterConfiguration analyzeFile(final String url) throws IOException {
    final File file = new File(url);
    try (final BufferedInputStream fis = new BufferedInputStream(new FileInputStream(file))) {
      return analyzeInputStream(fis, (int) file.length());
    }
  }

  private AbstractImporter.ImporterConfiguration analyzeInputStream(final BufferedInputStream in, final int total) throws IOException {
    in.mark(0);

    final ZipInputStream zip = new ZipInputStream(in);
    final ZipEntry entry = zip.getNextEntry();
    if (entry != null) {
      // ZIPPED FILE
      return analyzeText(in, total, limitBytes, true);
    }

    in.reset();
    in.mark(0);

    try {
      final GZIPInputStream gzip = new GZIPInputStream(in);
      return analyzeText(gzip, total, limitBytes, true);
    } catch (IOException e) {
      // NOT GZIP
    }

    in.reset();

    // ANALYZE THE INPUT AS TEXT
    return analyzeText(in, total, limitBytes, false);
  }

  private AbstractImporter.ImporterConfiguration analyzeText(final InputStream is, final int total, final long limit, final boolean compressed)
      throws IOException {
    final Parser parser = new Parser(is, total, limit, compressed);

    AbstractImporter.ImporterConfiguration format = analyzeChar(parser);
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
          return new AbstractImporter.ImporterConfiguration(FILE_TYPE.CSV);
        if (bestSeparator.getKey() == '\t')
          return new AbstractImporter.ImporterConfiguration(FILE_TYPE.TSV);
        else
          return new AbstractImporter.ImporterConfiguration(FILE_TYPE.TSV).set("separator", "" + bestSeparator.getKey());
      }
    }

    // UNKNOWN
    return null;
  }

  private void skipLine(final Parser parser) throws IOException {
    while (parser.isAvailable() && parser.nextChar() != '\n')
      ;
  }

  private AbstractImporter.ImporterConfiguration analyzeChar(final Parser parser) {
    final char currentChar = parser.getCurrentChar();
    if (currentChar == '<')
      return analyzeXML(parser);
    else if (currentChar == '{')
      return new AbstractImporter.ImporterConfiguration(FILE_TYPE.JSON);

    return null;
  }

  private AbstractImporter.ImporterConfiguration analyzeXML(final Parser parser) {

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
            ++parsedObjects;

            // GET ELEMENT'S ATTRIBUTES AS PROPERTIES
            for (int i = 0; i < xmlReader.getAttributeCount(); ++i) {
              schema.set(xmlReader.getAttributeName(i).toString(), xmlReader.getAttributeValue(i));
              lastName = null;
            }
          } else if (nestLevel == objectNestLevel + 1) {
            // GET ELEMENT'S SUB-NODES AS PROPERTIES
            if (lastName != null)
              schema.set(lastName, lastContent);

            lastName = xmlReader.getName().toString();
          }

          ++nestLevel;
          break;

        case XMLStreamReader.END_ELEMENT:
          if (lastName != null)
            schema.set(lastName, lastContent);

          LogManager.instance().debug(this, "</%s> (nestLevel=%d)", xmlReader.getName(), nestLevel);

          --nestLevel;

          if (nestLevel == objectNestLevel) {
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

    return new AbstractImporter.ImporterConfiguration(FILE_TYPE.XML);
  }

  private void dumpSchema(final AnalyzedSchema schema, final long parsedObjects) {
    LogManager.instance().info(this, "---------------------------------------------------------------");

    LogManager.instance()
        .info(this, "\nXML objects found %d (limitBytes=%s limitEntries=%d)", parsedObjects, FileUtils.getSizeAsString(limitBytes), limitEntries);
    LogManager.instance().info(this, "XML schema properties found:");
    for (Map.Entry<String, AnalyzedProperty> p : schema.properties()) {
      LogManager.instance().info(this, "- %s (%s)", p.getKey(), p.getValue().getType());
      if (p.getValue().isCollectingSamples())
        LogManager.instance().info(this, "    contents (%d items): %s", p.getValue().getContents().size(), p.getValue().getContents());
    }

    LogManager.instance().info(this, "---------------------------------------------------------------");
    LogManager.instance().info(this, "ADVICE:");

    int advice = 1;
    for (Map.Entry<String, AnalyzedProperty> p : schema.properties()) {
      if (p.getValue().getType() == Type.STRING && p.getValue().isCollectingSamples() && !p.getValue().getContents().isEmpty()) {
        LogManager.instance().info(this, "%d) Property '%s' could be linked to a vertex type containing the values:", advice++, p.getKey());
        LogManager.instance().info(this, "    %s", p.getValue().getContents());
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
    else if ("-limitBytes".equals(name))
      limitBytes = FileUtils.getSizeAsNumber(value);
    else if ("-limitEntries".equals(name))
      limitEntries = Long.parseLong(value);
    else if ("-maxValueSampling".equals(name))
      maxValueSampling = Integer.parseInt(value);
    else if ("-trimText".equals(name))
      trimText = Boolean.parseBoolean(value);
    else
      throw new IllegalArgumentException("Invalid setting '" + name + "'");
  }
}
