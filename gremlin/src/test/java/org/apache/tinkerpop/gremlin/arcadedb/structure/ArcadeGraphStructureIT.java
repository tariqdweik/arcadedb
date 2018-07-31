package org.apache.tinkerpop.gremlin.arcadedb.structure;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.arcadedb.ArcadeGraphProvider;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;

/**
 * Created by Enrico Risa on 30/07/2018.
 */

@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = ArcadeGraphProvider.class, graph = ArcadeGraph.class)
public class ArcadeGraphStructureIT {
}
