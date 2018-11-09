/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer;

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.log.LogManager;
import com.arcadedb.utility.FileUtils;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Level;

public class XMLImporter implements ContentImporter {
  @Override
  public void load(final Parser parser, final Database database, final ImporterContext context, final ImporterSettings settings) throws IOException {
    try {
      int objectNestLevel = 1;
      long maxValueSampling = 300;

      for (Map.Entry<String, String> entry : settings.options.entrySet()) {
        if ("objectNestLevel".equals(entry.getKey()))
          objectNestLevel = Integer.parseInt(entry.getValue());
        else if ("maxValueSampling".equals(entry.getKey()))
          maxValueSampling = Integer.parseInt(entry.getValue());
      }

      final XMLInputFactory xmlFactory = XMLInputFactory.newInstance();
      final XMLStreamReader xmlReader = xmlFactory.createXMLStreamReader(parser.getInputStream());

      int nestLevel = 0;

      String entityName = null;
      String lastName = null;
      String lastContent = null;

      final Map<String, Object> object = new LinkedHashMap<>();

      while (xmlReader.hasNext()) {
        final int eventType = xmlReader.next();

        switch (eventType) {
        case XMLStreamReader.COMMENT:
        case XMLStreamReader.SPACE:
          // IGNORE IT
          break;

        case XMLStreamReader.START_ELEMENT:
          if (nestLevel == objectNestLevel) {
            entityName = "v_" + xmlReader.getName().toString();

            // GET ELEMENT'S ATTRIBUTES AS PROPERTIES
            for (int i = 0; i < xmlReader.getAttributeCount(); ++i) {
              object.put(xmlReader.getAttributeName(i).toString(), xmlReader.getAttributeValue(i));
              lastName = null;
            }
          } else if (nestLevel == objectNestLevel + 1) {
            // GET ELEMENT'S SUB-NODES AS PROPERTIES
            if (lastName != null)
              object.put(lastName, lastContent);

            lastName = xmlReader.getName().toString();
          }

          ++nestLevel;
          break;

        case XMLStreamReader.END_ELEMENT:
          if (lastName != null)
            object.put(lastName, lastContent);

          LogManager.instance().log(this, Level.FINE, "</%s> (nestLevel=%d)", null, xmlReader.getName(), nestLevel);

          --nestLevel;

          if (nestLevel == objectNestLevel) {
            ++context.parsed;

            final MutableVertex record = database.newVertex(entityName);
            record.fromMap(object);
            database.async().createRecord(record);
            ++context.createdVertices;
          }
          break;

        case XMLStreamReader.ATTRIBUTE:
          ++nestLevel;
          LogManager.instance()
              .log(this, Level.FINE, "- attribute %s attributes=%d (nestLevel=%d)", null, xmlReader.getName(), xmlReader.getAttributeCount(), nestLevel);
          break;

        case XMLStreamReader.CHARACTERS:
        case XMLStreamReader.CDATA:
          final String text = xmlReader.getText();
          if (!text.isEmpty() && !text.equals("\n")) {
            if (settings.trimText)
              lastContent = text.trim();
            else
              lastContent = text;
          } else
            lastContent = null;
          break;

        default:
          // IGNORE IT
        }

        if (settings.limitEntries > 0 && context.parsed > settings.limitEntries)
          break;
      }
    } catch (Exception e) {
      throw new ImportException("Error on importing from source '" + parser.getSource() + "'", e);
    }
  }

  @Override
  public SourceSchema analyze(final Parser parser, final ImporterSettings settings) {
    int objectNestLevel = 1;
    long limitBytes = 0;
    long limitEntries = 0;
    long maxValueSampling = 300;

    for (Map.Entry<String, String> entry : settings.options.entrySet()) {
      if ("limitBytes".equals(entry.getKey()))
        limitBytes = FileUtils.getSizeAsNumber(entry.getValue());
      else if ("limitEntries".equals(entry.getKey()))
        limitEntries = Long.parseLong(entry.getValue());
      else if ("objectNestLevel".equals(entry.getKey()))
        objectNestLevel = Integer.parseInt(entry.getValue());
      else if ("maxValueSampling".equals(entry.getKey()))
        maxValueSampling = Integer.parseInt(entry.getValue());
    }

    long parsedObjects = 0;
    final AnalyzedSchema schema = new AnalyzedSchema(maxValueSampling);

    final String currentUnit = parser.isCompressed() ? "uncompressed " : "";
    final String totalUnit = parser.isCompressed() ? "compressed " : "";

    try {

      parser.reset();

      final XMLInputFactory xmlFactory = XMLInputFactory.newInstance();
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
          LogManager.instance().log(this, Level.FINE, "<%s> attributes=%d (nestLevel=%d)", null, xmlReader.getName(), xmlReader.getAttributeCount(), nestLevel);

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

          LogManager.instance().log(this, Level.FINE, "</%s> (nestLevel=%d)", null, xmlReader.getName(), nestLevel);

          --nestLevel;

          if (nestLevel == objectNestLevel) {
            ++parsedObjects;

            if (!parsedStructure)
              parsedStructure = true;

            if (parsedObjects % 10000 == 0) {
              LogManager.instance().log(this, Level.INFO, "- Parsed %d XML objects (%s%s/%s%s)", null, parsedObjects, currentUnit,
                  FileUtils.getSizeAsString(parser.getPosition()), totalUnit, FileUtils.getSizeAsString(parser.getTotal()));
            }
          }
          break;

        case XMLStreamReader.ATTRIBUTE:
          ++nestLevel;
          LogManager.instance()
              .log(this, Level.FINE, "- attribute %s attributes=%d (nestLevel=%d)", null, xmlReader.getName(), xmlReader.getAttributeCount(), nestLevel);
          break;

        case XMLStreamReader.CHARACTERS:
        case XMLStreamReader.CDATA:
          final String text = xmlReader.getText();
          if (!text.isEmpty() && !text.equals("\n")) {
            if (settings.trimText)
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
      LogManager.instance().log(this, Level.SEVERE, "Error on parsing XML", e);
      return null;
    }

    // END OF PARSING. THIS DETERMINES THE TYPE
    schema.endParsing();

    return new SourceSchema(this, parser.getSource(), schema);
  }

  @Override
  public String getFormat() {
    return "XML";
  }
}
