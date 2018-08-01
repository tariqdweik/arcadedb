/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package org.apache.tinkerpop.gremlin.arcadedb.structure;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class ArcadeGraphFeatures implements Graph.Features {

  protected GraphFeatures          graphFeatures          = new ArcadeGraphGraphFeatures();
  protected VertexFeatures         vertexFeatures         = new ArcadeVertexFeatures();
  protected EdgeFeatures           edgeFeatures           = new ArcadeEdgeFeatures();
  protected VertexPropertyFeatures vertexPropertyFeatures = new ArcadeVertexPropertyFeatures();
  protected EdgePropertyFeatures   edgePropertyFeatures   = new ArcadeEdgePropertyFeatures();

  @Override
  public GraphFeatures graph() {
    return graphFeatures;
  }

  @Override
  public VertexFeatures vertex() {
    return vertexFeatures;
  }

  @Override
  public EdgeFeatures edge() {
    return edgeFeatures;
  }

  @Override
  public String toString() {
    return StringFactory.featureString(this);
  }

  public class ArcadeGraphGraphFeatures implements GraphFeatures {

    private VariableFeatures variableFeatures = new ArcadeGraphVariables.ArcadeVariableFeatures();

    @Override
    public boolean supportsConcurrentAccess() {
      return false;
    }

    @Override
    public boolean supportsComputer() {
      return false;
    }

    @Override
    public boolean supportsThreadedTransactions() {
      return false;
    }

    @Override
    public VariableFeatures variables() {
      return variableFeatures;
    }

  }

  public class ArcadeElementFeatures implements ElementFeatures {
    @Override
    public boolean supportsNumericIds() {
      return false;
    }

    @Override
    public boolean supportsCustomIds() {
      return false;
    }

    @Override
    public boolean supportsUserSuppliedIds() {
      return false;
    }

    @Override
    public boolean supportsUuidIds() {
      return false;
    }

    @Override
    public boolean supportsAnyIds() {
      return false;
    }

    @Override
    public boolean willAllowId(Object id) {
      return false;
    }

    @Override
    public boolean supportsStringIds() {
      return false;
    }
  }

  public class ArcadeVertexFeatures extends ArcadeElementFeatures implements VertexFeatures {

    @Override
    public VertexPropertyFeatures properties() {
      return vertexPropertyFeatures;
    }

    @Override
    public VertexProperty.Cardinality getCardinality(String key) {
      return VertexProperty.Cardinality.single;
    }

    @Override
    public boolean supportsMetaProperties() {
      return false;
    }

    @Override
    public boolean supportsMultiProperties() {
      return false;
    }
  }

  public class ArcadeEdgeFeatures extends ArcadeElementFeatures implements EdgeFeatures {
    @Override
    public EdgePropertyFeatures properties() {
      return edgePropertyFeatures;
    }
  }

  public class ArcadeVertexPropertyFeatures extends ArcadePropertyFeatures implements VertexPropertyFeatures {

    @Override
    public boolean supportsAnyIds() {
      return false;
    }

    @Override
    public boolean supportsCustomIds() {
      return false;
    }

    @Override
    public boolean supportsNumericIds() {
      return false;
    }

    @Override
    public boolean supportsUserSuppliedIds() {
      return false;
    }

    @Override
    public boolean supportsUuidIds() {
      return false;
    }

    @Override
    public boolean willAllowId(Object id) {
      return false;
    }
  }

  public class ArcadeEdgePropertyFeatures extends ArcadePropertyFeatures implements EdgePropertyFeatures {
  }

  public class ArcadePropertyFeatures implements PropertyFeatures {
    @Override
    public boolean supportsMapValues() {
      return false;
    }

    @Override
    public boolean supportsUniformListValues() {
      return false;
    }

    @Override
    public boolean supportsSerializableValues() {
      return false;
    }

    @Override
    public boolean supportsStringArrayValues() {
      return false;
    }

    @Override
    public boolean supportsBooleanArrayValues() {
      return false;
    }

    @Override
    public boolean supportsDoubleArrayValues() {
      return false;
    }

    @Override
    public boolean supportsFloatArrayValues() {
      return false;
    }

    @Override
    public boolean supportsIntegerArrayValues() {
      return false;
    }

    @Override
    public boolean supportsLongArrayValues() {
      return false;
    }

    @Override
    public boolean supportsMixedListValues() {
      return false;
    }
  }

}
