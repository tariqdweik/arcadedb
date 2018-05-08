package com.arcadedb.sql.executor;

import com.arcadedb.database.Record;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;

import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by luigidellaquila on 07/07/16.
 */
public interface ResultSet extends Spliterator<Result>, Iterator<Result>, AutoCloseable {

  @Override
  boolean hasNext();

  @Override
  Result next();

  default void remove() {
    throw new UnsupportedOperationException();
  }

  void close();

  Optional<ExecutionPlan> getExecutionPlan();

  Map<String, Long> getQueryStats();

  default void reset() {
    throw new UnsupportedOperationException("Implement RESET on " + getClass().getSimpleName());
  }

  default boolean tryAdvance(Consumer<? super Result> action) {
    if (hasNext()) {
      action.accept(next());
      return true;
    }
    return false;
  }

  default void forEachRemaining(Consumer<? super Result> action) {
    Spliterator.super.forEachRemaining(action);
  }

  default ResultSet trySplit() {
    return null;
  }

  default long estimateSize() {
    return Long.MAX_VALUE;
  }

  default int characteristics() {
    return ORDERED;
  }

  /**
   * Returns the result set as a stream. IMPORTANT: the stream consumes the result set!
   *
   * @return
   */
  default Stream<Result> stream() {
    return StreamSupport.stream(this, false);
  }

  /**
   * Returns the result set as a stream of elements (filters only the results that are elements - where the isElement() method
   * returns true). IMPORTANT: the stream consumes the result set!
   *
   * @return
   */

  default Stream<Record> elementStream() {
    return StreamSupport.stream(new Spliterator<Record>() {
      @Override
      public boolean tryAdvance(Consumer<? super Record> action) {
        while (hasNext()) {
          Result elem = next();
          if (elem.isElement()) {
            action.accept(elem.getElement().get());
            return true;
          }
        }
        return false;
      }

      @Override
      public Spliterator<Record> trySplit() {
        return null;
      }

      @Override
      public long estimateSize() {
        return Long.MAX_VALUE;
      }

      @Override
      public int characteristics() {
        return ORDERED;
      }
    }, false);
  }

  /**
   * Returns the result set as a stream of vertices (filters only the results that are vertices - where the isVertex() method
   * returns true). IMPORTANT: the stream consumes the result set!
   *
   * @return
   */

  default Stream<Vertex> vertexStream() {
    return StreamSupport.stream(new Spliterator<Vertex>() {
      @Override
      public boolean tryAdvance(Consumer<? super Vertex> action) {
        while (hasNext()) {
          Result elem = next();
          if (elem.isVertex()) {
            action.accept(elem.getVertex().get());
            return true;
          }
        }
        return false;
      }

      @Override
      public Spliterator<Vertex> trySplit() {
        return null;
      }

      @Override
      public long estimateSize() {
        return Long.MAX_VALUE;
      }

      @Override
      public int characteristics() {
        return ORDERED;
      }
    }, false);
  }

  /**
   * Returns the result set as a stream of vertices (filters only the results that are edges - where the isEdge() method
   * returns true). IMPORTANT: the stream consumes the result set!
   *
   * @return
   */

  default Stream<Edge> edgeStream() {
    return StreamSupport.stream(new Spliterator<Edge>() {
      @Override
      public boolean tryAdvance(Consumer<? super Edge> action) {
        while (hasNext()) {
          Result nextElem = next();
          if (nextElem != null && nextElem.isEdge()) {
            action.accept(nextElem.getEdge().get());
            return true;
          }
        }
        return false;
      }

      @Override
      public Spliterator<Edge> trySplit() {
        return null;
      }

      @Override
      public long estimateSize() {
        return Long.MAX_VALUE;
      }

      @Override
      public int characteristics() {
        return ORDERED;
      }
    }, false);
  }
}
