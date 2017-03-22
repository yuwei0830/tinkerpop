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
package org.apache.tinkerpop.gremlin.process.traversal.step.map;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.MutatingContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MutatingContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.CallbackRegistry;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.Event;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.ListCallbackRegistry;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class MutatingStep<S> extends MapStep<S, Element> implements MutatingContainerHolder, Mutating {

    private Map<String,Vertex> labelledVertices = new HashMap<>();
    private List<MutatingContainer> mutatingContainers = new ArrayList<>();
    private CallbackRegistry<Event> callbackRegistry;

    public MutatingStep(final Traversal.Admin traversal, final MutatingContainer... mutatingContainers) {
        super(traversal);
        Collections.addAll(this.mutatingContainers, mutatingContainers);
    }

    @Override
    protected Element map(final Traverser.Admin<S> traverser) {
        Element e = null;
        for (final MutatingContainer container : mutatingContainers) {
            final Element element = container.get();
            if (element instanceof Vertex) {
                // TODO: how to handle multiple labelled stuffs g.addV().as('a').addV().as('a')
                for (String label : container.getStepLabels()) {
                    labelledVertices.put(label, (Vertex) element);
                }
            }

            e = element;
        }

        return e;
    }

    @Override
    public void addPropertyMutations(final Object... keyValues) {
        final MutatingContainer container = mutatingContainers.get(mutatingContainers.size() - 1);
        container.addPropertyMutations(keyValues);
    }

    @Override
    public Graph getGraph() {
        return (Graph) this.traversal.getGraph().get();
    }

    @Override
    public List<MutatingContainer> getMutatingContainers() {
        return this.mutatingContainers;
    }

    @Override
    public void addMutatingContainer(final MutatingContainer mutatingContainer) {
        this.mutatingContainers.add(mutatingContainer);
    }

    @Override
    public CallbackRegistry<Event> getMutatingCallbackRegistry() {
        if (null == callbackRegistry) callbackRegistry = new ListCallbackRegistry<>();
        return callbackRegistry;
    }

    @Override
    public Vertex getLabelledVertex(final String label) {
        return labelledVertices.get(label);
    }
}