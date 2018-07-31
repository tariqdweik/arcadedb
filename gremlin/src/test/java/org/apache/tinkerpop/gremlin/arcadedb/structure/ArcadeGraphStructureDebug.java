/*
 * Copyright (c) 2018 - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package org.apache.tinkerpop.gremlin.arcadedb.structure;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.arcadedb.ArcadeGraphProvider;
import org.apache.tinkerpop.gremlin.arcadedb.suite.ArcadeDebugSuite;
import org.junit.runner.RunWith;

/**
 * Created by Enrico Risa on 30/07/2018.
 */

@RunWith(ArcadeDebugSuite.class)
@GraphProviderClass(provider = ArcadeGraphProvider.class, graph = ArcadeGraph.class)
public class ArcadeGraphStructureDebug {
}
