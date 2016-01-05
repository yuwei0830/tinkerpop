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
package org.apache.tinkerpop.gremlin.spark.process.computer.edgecopy;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.ConfigurationUtils;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.script.ScriptRecordReader;
import org.apache.tinkerpop.gremlin.hadoop.structure.io.script.ScriptRecordWriter;
import org.apache.tinkerpop.gremlin.process.computer.edgecopy.EdgeCopyVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.spark.process.computer.SparkGraphComputer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.apache.tinkerpop.gremlin.hadoop.Constants.GREMLIN_HADOOP_INPUT_LOCATION;
import static org.apache.tinkerpop.gremlin.hadoop.Constants.GREMLIN_HADOOP_OUTPUT_LOCATION;
import static org.apache.tinkerpop.gremlin.hadoop.Constants.HIDDEN_G;
import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class EdgeCopyVertexProgramTest {

    private Configuration configuration;

    private static Configuration getNextConfiguration(final Configuration configuration) {
        final Configuration next = new BaseConfiguration();
        ConfigurationUtils.copy(configuration, next);
        next.setProperty(GREMLIN_HADOOP_INPUT_LOCATION, configuration.getString(GREMLIN_HADOOP_OUTPUT_LOCATION) + HIDDEN_G);
        next.setProperty(GREMLIN_HADOOP_OUTPUT_LOCATION, configuration.getString(GREMLIN_HADOOP_OUTPUT_LOCATION).replaceAll("/$", "_/"));
        return next;
    }

    @Before
    public void initialize() throws IOException, ConfigurationException {
        final File readGraphConfigurationFile = TestHelper.generateTempFileFromResource(
                EdgeCopyVertexProgramTest.class, "hadoop-script.properties", "");
        final File inputFile = TestHelper.generateTempFileFromResource(
                EdgeCopyVertexProgramTest.class, "tinkerpop-classic.txt", "");
        final File scriptInputFile = TestHelper.generateTempFileFromResource(
                EdgeCopyVertexProgramTest.class, "script-input.groovy", "");
        final File scriptOutputFile = TestHelper.generateTempFileFromResource(
                EdgeCopyVertexProgramTest.class, "script-output.groovy", "");
        configuration = new PropertiesConfiguration(readGraphConfigurationFile.getAbsolutePath());
        configuration.setProperty(GREMLIN_HADOOP_INPUT_LOCATION, inputFile.getAbsolutePath());
        configuration.setProperty(ScriptRecordReader.SCRIPT_FILE, scriptInputFile.getAbsolutePath());
        configuration.setProperty(ScriptRecordWriter.SCRIPT_FILE, scriptOutputFile.getAbsolutePath());
        configuration.setProperty(GREMLIN_HADOOP_OUTPUT_LOCATION, TestHelper.makeTestDataDirectory(
                EdgeCopyVertexProgramTest.class, "output"));
        configuration.setProperty("gremlin.hadoop.graphOutputFormat", "org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.GraphSONOutputFormat");
    }

    @Test
    @Ignore
    public void shouldCopyOutEdges() throws ExecutionException, InterruptedException {

        final Graph readGraph = GraphFactory.open(configuration);
        final GraphTraversalSource g1 = readGraph.traversal();
        final Map<Object, Long> o1 = g1.V().map(__.outE().count()).groupCount().next();
        final Map<Object, Long> i1 = g1.V().map(__.inE().count()).groupCount().next();
        assertEquals(4, o1.size());
        assertEquals(1, i1.size());

        final EdgeCopyVertexProgram ecvp = EdgeCopyVertexProgram.build().create(readGraph);
        readGraph.compute(SparkGraphComputer.class).workers(1).program(ecvp).submit().get();

        final Graph nextGraph = GraphFactory.open(getNextConfiguration(configuration));
        final GraphTraversalSource g2 = nextGraph.traversal();
        final Map<Object, Long> o2 = g2.V().map(__.outE().count()).groupCount().next();
        final Map<Object, Long> i2 = g2.V().map(__.inE().count()).groupCount().next();
        assertEquals(4, o2.size());
        assertEquals(3, i2.size());
    }

    @Test
    public void shouldCopyOutEdgesGraphSON() throws ExecutionException, InterruptedException {

        final Graph readGraph = GraphFactory.open(configuration);
        final GraphTraversalSource g1 = readGraph.traversal();
        final Map<Object, Long> o1 = g1.V().map(__.outE().count()).groupCount().next();
        final Map<Object, Long> i1 = g1.V().map(__.inE().count()).groupCount().next();
        assertEquals(4, o1.size());
        assertEquals(1, i1.size());

        final EdgeCopyVertexProgram ecvp = EdgeCopyVertexProgram.build().create(readGraph);
        final Graph nextGraph = readGraph.compute(SparkGraphComputer.class).workers(1).program(ecvp).submit().get().graph();

        final GraphTraversalSource g2 = nextGraph.traversal();
        final Map<Object, Long> o2 = g2.V().map(__.outE().count()).groupCount().next();
        final Map<Object, Long> i2 = g2.V().map(__.inE().count()).groupCount().next();
        assertEquals(4, o2.size());
        assertEquals(3, i2.size());
    }
}
