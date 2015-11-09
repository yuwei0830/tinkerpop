/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.process.computer.edgecopy;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.computer.util.StaticVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedFactory;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedElement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class EdgeCopyVertexProgram extends StaticVertexProgram<Edge> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EdgeCopyVertexProgram.class);

    public static final String EDGE_COPY_VERTEX_PROGRAM_CFG_PREFIX = "gremlin.edgeCopyVertexProgram";

    private final Direction direction;
    private Configuration configuration;

    private EdgeCopyVertexProgram() {
        this.direction = Direction.OUT;
    }

    @Override
    public void setup(final Memory memory) {
    }

    @Override
    public void loadState(final Graph graph, final Configuration config) {
        configuration = new BaseConfiguration();
        if (config != null) {
            ConfigurationUtils.copy(config, configuration);
        }
    }

    @Override
    public void storeState(final Configuration config) {
        if (configuration != null) {
            ConfigurationUtils.copy(configuration, config);
        }
    }

    @Override
    public void execute(final Vertex sourceVertex, final Messenger<Edge> messenger, final Memory memory) {
        if (memory.isInitialIteration()) {
            sourceVertex.edges(direction).forEachRemaining(edge -> {
                final Object inVId = edge.inVertex().id();
                LOGGER.info("send edge from " + sourceVertex.id() + " to " + edge.inVertex().id());
                MessageScope messageScope = MessageScope.Local.of(() -> __.<Vertex>start().outE().filter(__.inV().hasId(inVId)));
                messenger.sendMessage(messageScope, DetachedFactory.detach(edge, true));
            });
        } else if (memory.getIteration() == 1) {
            final Iterator<Edge> ei = messenger.receiveMessages();
            final Vertex inV = sourceVertex instanceof WrappedElement
                    ? (Vertex) ((WrappedElement) sourceVertex).getBaseElement() : sourceVertex;
            final Graph sg = inV.graph();
            while (ei.hasNext()) {
                final Edge edge = ei.next();
                if (sourceVertex.id().equals(edge.inVertex().id())) {
                    LOGGER.info("create edge from " + edge.outVertex().id() + " to " + sourceVertex.id());
                    final Object outVId = edge.outVertex().id();
                    final Iterator<Vertex> vi = sg.vertices(outVId);
                    final Vertex outV = vi.hasNext() ? vi.next() : sg.addVertex(T.id, outVId);
                    final Edge clonedEdge = outV.addEdge(edge.label(), inV);
                    edge.properties().forEachRemaining(p -> clonedEdge.property(p.key(), p.value()));
                }
            }
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        return memory.getIteration() == 1;
    }

    @Override
    public Set<String> getElementComputeKeys() {
        return Collections.emptySet();
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        return Collections.emptySet();
    }

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.NEW;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.EDGES;
    }

    @Override
    public String toString() {
        return StringFactory.vertexProgramString(this, "direction=" + direction);
    }


    public static Builder build() {
        return new Builder();
    }

    public static class Builder extends AbstractVertexProgramBuilder<Builder> {

        private Builder() {
            super(EdgeCopyVertexProgram.class);
        }

        @SuppressWarnings("unchecked")
        @Override
        public EdgeCopyVertexProgram create(final Graph graph) {
            ConfigurationUtils.append(graph.configuration().subset(EDGE_COPY_VERTEX_PROGRAM_CFG_PREFIX), configuration);
            return (EdgeCopyVertexProgram) VertexProgram.createVertexProgram(graph, configuration);
        }
    }

    @Override
    public Features getFeatures() {
        return new Features() {
            @Override
            public boolean requiresLocalMessageScopes() {
                return true;
            }
        };
    }
}
