/*
 * Copyright (c) - Arcade Analytics LTD (https://arcadeanalytics.com)
 */

package com.arcadedb.sql.executor;

import com.arcadedb.database.Document;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.Vertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Returns an OResult containing metadata regarding the schema types.
 *
 * @author Luigi Dell'Aquila (l.dellaquila - at - orientdb.com)
 */
public class FetchFromSchemaTypesStep extends AbstractExecutionStep {

    private final List<ResultInternal> result = new ArrayList<>();

    private int cursor = 0;
    private long cost = 0;

    public FetchFromSchemaTypesStep(final CommandContext ctx, final boolean profilingEnabled) {
        super(ctx, profilingEnabled);
    }

    @Override
    public ResultSet syncPull(final CommandContext ctx, final int nRecords) throws TimeoutException {
        getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));

        if (cursor == 0) {
            long begin = profilingEnabled ? System.nanoTime() : 0;
            try {
                final Schema schema = ctx.getDatabase().getSchema();

                for (DocumentType type : schema.getTypes()) {
                    final ResultInternal r = new ResultInternal();
                    result.add(r);


                    r.setProperty("name", type.getName());

                    String t = "?";

                    if (type.getType() == Document.RECORD_TYPE)
                        t = "document";
                    else if (type.getType() == Vertex.RECORD_TYPE)
                        t = "vertex";
                    else if (type.getType() == Edge.RECORD_TYPE)
                        t = "edge";

                    r.setProperty("type", t);

                    List<String> parents = type.getParentTypes().stream().map(pt -> pt.getName()).collect(Collectors.toList());
                    r.setProperty("parentTypes", parents);

                    final Set<ResultInternal> propertiesTypes = type.getPropertyNames().stream()
                            .map(name -> type.getProperty(name))
                            .map(property -> {
                                final ResultInternal propRes = new ResultInternal();
                                propRes.setProperty("id", property.getId());
                                propRes.setProperty("name", property.getName());
                                propRes.setProperty("type", property.getType());
                                return propRes;
                            })
                            .collect(Collectors.toSet());
                    r.setProperty("properties", propertiesTypes);

                    final Set<ResultInternal> indexes = Stream.of(type.getAllIndexes())
                            .map(typeIndex -> {
                                final ResultInternal propRes = new ResultInternal();
                                propRes.setProperty("name", typeIndex.getName());
                                propRes.setProperty("typeName", typeIndex.getTypeName());
                                propRes.setProperty("type", typeIndex.getType());
                                propRes.setProperty("properties", Arrays.asList(typeIndex.getPropertyNames()));
                                propRes.setProperty("automatic", typeIndex.isAutomatic());
                                propRes.setProperty("unique", typeIndex.isUnique());
                                return propRes;
                            })
                            .collect(Collectors.toSet());
                    r.setProperty("indexes", indexes);
                }
            } finally {
                if (profilingEnabled) {
                    cost += (System.nanoTime() - begin);
                }
            }
        }
        return new ResultSet() {
            @Override
            public boolean hasNext() {
                return cursor < result.size();
            }

            @Override
            public Result next() {
                return result.get(cursor++);
            }

            @Override
            public void close() {
                result.clear();
            }

            @Override
            public Optional<ExecutionPlan> getExecutionPlan() {
                return null;
            }

            @Override
            public Map<String, Long> getQueryStats() {
                return null;
            }

            @Override
            public void reset() {
                cursor = 0;
            }
        };
    }

    @Override
    public String prettyPrint(int depth, int indent) {
        String spaces = ExecutionStepInternal.getIndent(depth, indent);
        String result = spaces + "+ FETCH DATABASE METADATA TYPES";
        if (profilingEnabled) {
            result += " (" + getCostFormatted() + ")";
        }
        return result;
    }

    @Override
    public long getCost() {
        return cost;
    }
}
