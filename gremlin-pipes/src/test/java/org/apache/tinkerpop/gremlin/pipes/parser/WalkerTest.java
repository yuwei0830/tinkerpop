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

package org.apache.tinkerpop.gremlin.pipes.parser;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class WalkerTest {

    @Test
    public void shouldMoveCorrectly() {
        SourceCode sourceCode = new SourceCode("---a---bc");
        Walker walker = new Walker(sourceCode, 0, 0);
        assertEquals('-', walker.read());
        walker.moveToNextInstruction();
        assertEquals('a', walker.read());
        assertEquals('-', walker.peek(-1, 0));
        assertEquals('-', walker.peek(1, 0));
        walker.moveToNextInstruction();
        assertEquals('b', walker.read());
        assertEquals('-', walker.peek(-1, 0));
        assertEquals('c', walker.peek(1, 0));
        ////
        final StringBuilder builder = new StringBuilder();
        builder.append("----a-\\").append("\n").
                append("       \\--bc").append("\n");
        sourceCode = new SourceCode(builder.toString());
        walker = new Walker(sourceCode, 0, 0);
        assertEquals('-', walker.read());
        walker.moveToNextInstruction();
        assertEquals('a', walker.read());
        assertEquals('-', walker.peek(-1, 0));
        assertEquals('-', walker.peek(1, 0));
        walker.moveToNextInstruction();
        assertEquals('b', walker.read());
        assertEquals('-', walker.peek(-1, 0));
        assertEquals('c', walker.peek(1, 0));
    }
}
