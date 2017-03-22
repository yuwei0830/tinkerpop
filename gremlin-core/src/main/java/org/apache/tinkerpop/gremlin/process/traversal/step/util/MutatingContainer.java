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
package org.apache.tinkerpop.gremlin.process.traversal.step.util;

import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.MutatingContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class MutatingContainer implements Supplier<Element>, Mutating, Serializable, Cloneable {
    public enum Operation implements Function<MutatingContainer, Element> {
        ADD_V {
            @Override
            public Element apply(final MutatingContainer container) {
                return (container.holder.getGraph()).addVertex(container.keyValues.toArray());
            }
        },
        ADD_E {
            @Override
            public Element apply(final MutatingContainer container) {
                final Vertex outV = container.holder.getLabelledVertex(container.outVLabel);
                final Vertex inV = container.holder.getLabelledVertex(container.inVLabel);
                return outV.addEdge(container.edgeLabel, inV, container.keyValues.toArray());
            }
        }
    }

    private final Operation op;
    private final MutatingContainerHolder holder;
    private String outVLabel;
    private String inVLabel;
    private String edgeLabel;
    private List<Object> keyValues;
    private Set<String> labels = Collections.emptySet();

    public MutatingContainer(final Operation op, final MutatingContainerHolder holder, final Object... keyValues) {
        this.op = op;
        this.holder = holder;
        if (keyValues != null && keyValues.length > 0) this.keyValues = new ArrayList<>();
        Collections.addAll(this.keyValues, keyValues);
    }

    @Override
    public void addPropertyMutations(final Object... keyValues) {
        if (null == this.keyValues) this.keyValues = new ArrayList<>();
        Collections.addAll(this.keyValues, keyValues);
    }

    public void setOutVLabel(final String label) {
        this.outVLabel = label;
    }

    public void setInVLabel(final String label) {
        this.inVLabel = label;
    }

    public void addStepLabels(final Set<String> labels) {
        this.labels = labels;
    }

    public void setEdgeLabel(final String label) {
        this.edgeLabel = label;
    }

    public Set<String> getStepLabels() {
        return Collections.unmodifiableSet(labels);
    }

    @Override
    public CallbackRegistry<Event> getMutatingCallbackRegistry() {
        // hmmmmmmmmmmmmm
        return null;
    }

    @Override
    public Element get() {
        return op.apply(this);
    }

}
