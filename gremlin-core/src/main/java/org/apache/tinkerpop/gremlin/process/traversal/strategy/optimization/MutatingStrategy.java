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
package org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStartStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.AddVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MutatingStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.List;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class MutatingStrategy extends AbstractTraversalStrategy<TraversalStrategy.OptimizationStrategy> implements TraversalStrategy.OptimizationStrategy {

    private static final MutatingStrategy INSTANCE = new MutatingStrategy();

    private MutatingStrategy() {}

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final MutatingStep mutatingStep = new MutatingStep(traversal);

        Step start = null;
        Step end = null;
        final List<Step> steps = traversal.getSteps();
        for (int ix = 0; ix < steps.size(); ix++) {
            final Step currentStep = steps.get(ix);
            if (currentStep instanceof AddVertexStep) {
                //mutatingStep.addMutatingStep(null, (MapStep) currentStep);
                if (null == start) start = currentStep;
            }

            end = currentStep;
        }

        TraversalHelper.removeBetween(start, end);
        traversal.addStep(mutatingStep);
    }

    public static MutatingStrategy instance() {
        return INSTANCE;
    }
}
