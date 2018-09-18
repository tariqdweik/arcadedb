/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.importer.xml;

import com.arcadedb.engine.Dictionary;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.importer.*;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.SchemaImpl;
import com.arcadedb.schema.VertexType;
import com.arcadedb.utility.FileUtils;
import com.arcadedb.utility.LogManager;
import com.sun.xml.internal.stream.XMLInputFactoryImpl;

import javax.xml.stream.XMLStreamReader;
import java.util.LinkedHashMap;
import java.util.Map;

public class XMLImporter extends AbstractImporter {

  private final String[] args;
  private       Parser   parser;
  private       int      objectNestLevel = 1;
  private       boolean  trimText        = true;
  private       long     limitBytes;
  private       long     limitEntries;

  public XMLImporter(final String[] args) {
    super(args);
    this.args = args;
  }

  public static void main(final String[] args) {
    new XMLImporter(args).load();
  }

  protected void load() {
    openDatabase();
    try {
      final ContentAnalyzer analyzer = new ContentAnalyzer(args);

      final SourceInfo sourceInfo = analyzer.analyzeSchema();
      if (sourceInfo == null)
        return;

      updateSchema(sourceInfo.schema);

      source = analyzer.getSource();
      parser = new Parser(source, 0);

      parser.reset();

      final XMLInputFactoryImpl xmlFactory = new XMLInputFactoryImpl();
      final XMLStreamReader xmlReader = xmlFactory.createXMLStreamReader(source.inputStream);

      startImporting();

      int nestLevel = 0;

      String entitytName = null;
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
            entitytName = "v_" + xmlReader.getName().toString();

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

          LogManager.instance().debug(this, "</%s> (nestLevel=%d)", xmlReader.getName(), nestLevel);

          --nestLevel;

          if (nestLevel == objectNestLevel) {
            ++parsed;

            final MutableVertex record = database.newVertex(entitytName);
            record.fromMap(object);
            database.asynch().createRecord(record);
            ++createdVertices;
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

        if (limitEntries > 0 && parsed > limitEntries)
          break;
      }
    } catch (Exception e) {
      LogManager.instance().error(this, "Error on parsing XML", e);
    } finally {
      database.asynch().waitCompletion();
      stopImporting();
      closeDatabase();
      closeInputFile();
    }
  }

  private void updateSchema(final AnalyzedSchema schema) {
    if (schema == null)
      return;

    final Dictionary dictionary = database.getSchema().getDictionary();

    LogManager.instance().info(this, "Checking schema...");

    for (String entity : schema.getEntities()) {
      final VertexType type = getOrCreateVertexType(entity);

      for (Map.Entry<String, AnalyzedProperty> p : schema.getProperties(entity)) {
        final String propName = p.getKey();
        final AnalyzedProperty propValue = p.getValue();

        if (type.existsProperty(propName)) {
          // CHECK TYPE
          final Property property = type.getProperty(propName);
          if (property.getType() != propValue.getType()) {
            LogManager.instance()
                .warn(this, "- found schema property %s.%s of type %s, while analyzing the source type %s was found", entity, propName, property.getType(),
                    propValue.getType());
          }
        } else {
          // CREATE IT
          LogManager.instance().info(this, "- creating property %s.%s of type %s", entity, propName, propValue.getType());
          type.createProperty(propName, propValue.getType());
        }

        for (String sample : propValue.getContents()) {
          dictionary.getIdByName(sample, true);
        }
      }
    }

    ((SchemaImpl) database.getSchema()).saveConfiguration();

    database.commit();
    database.begin();
  }

  @Override
  protected long getInputFilePosition() {
    return parser.getPosition();
  }

  @Override
  protected void parseParameter(final String name, final String value) {
    if ("-recordType".equals(name))
      recordType = RECORD_TYPE.valueOf(value.toUpperCase());
    else if ("-analyzeTrimText".equals(name))
      trimText = Boolean.parseBoolean(value);
    else if ("-limitBytes".equals(name))
      limitBytes = FileUtils.getSizeAsNumber(value);
    else if ("-limitEntries".equals(name))
      limitEntries = Long.parseLong(value);
    else if ("-objectNestLevel".equals(name))
      objectNestLevel = Integer.parseInt(value);
    else
      super.parseParameter(name, value);
  }
}
