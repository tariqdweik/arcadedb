package org.apache.tinkerpop.gremlin.arcadedb.process;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.arcadedb.ArcadeGraphProvider;
import org.apache.tinkerpop.gremlin.arcadedb.structure.ArcadeGraph;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;

/**
 * Created by Enrico Risa on 30/07/2018.
 */

@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = ArcadeGraphProvider.class, graph = ArcadeGraph.class)
public class ArcadeGraphProcessStandardTest {
}
