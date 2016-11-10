/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.tinkerpop.gremlin.pipes.jsr223;

import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.GremlinScriptEngineFactory;
import org.apache.tinkerpop.gremlin.jsr223.JavaTranslator;
import org.apache.tinkerpop.gremlin.pipes.Parser;
import org.apache.tinkerpop.gremlin.pipes.PipesTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;

import javax.script.AbstractScriptEngine;
import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptException;
import javax.script.SimpleBindings;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class GremlinPipesScriptEngine extends AbstractScriptEngine implements GremlinScriptEngine {

    @Override
    public Object eval(final String script, final ScriptContext context) throws ScriptException {
        final Parser parser = new Parser(script);
        final TraversalSource source = (TraversalSource) context.getBindings(ScriptContext.ENGINE_SCOPE).get(parser.getTraversalSource());
        System.out.println(script);
        return JavaTranslator.of(source).translate(parser.getBytecode());
    }

    @Override
    public Object eval(final Reader reader, final ScriptContext context) throws ScriptException {
        try {
            final BufferedReader buffer = new BufferedReader(reader);
            final StringBuilder builder = new StringBuilder();
            String line;
            while (null != (line = buffer.readLine())) {
                builder.append(line).append("\n");
            }
            return this.eval(builder.toString(), context);
        } catch (final IOException e) {
            throw new ScriptException(e);
        }
    }

    @Override
    public Bindings createBindings() {
        return new SimpleBindings();
    }

    @Override
    public GremlinScriptEngineFactory getFactory() {
        return new GremlinPipesScriptEngineFactory();
    }

    @Override
    public Traversal.Admin eval(final Bytecode bytecode, final Bindings bindings) throws ScriptException {
        return (Traversal.Admin) this.eval(PipesTranslator.of("g").translate(bytecode), bindings);
    }
}
