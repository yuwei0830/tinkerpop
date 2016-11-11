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

import static org.apache.tinkerpop.gremlin.pipes.parser.SymbolHelper.ENDS;
import static org.apache.tinkerpop.gremlin.pipes.parser.SymbolHelper.PIPES;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class Walker {

    private int x;
    private int y;
    private final SourceCode sourceCode;

    public Walker(final SourceCode sourceCode, final int x, final int y) {
        this.sourceCode = sourceCode;
        this.x = x;
        this.y = y;
    }

    public int getX() {
        return this.x;
    }

    public int getY() {
        return this.y;
    }

    protected boolean moveToNextInstruction() {
        // move through current instruction
        while (true) {
            final char current = this.sourceCode.getCharacter(this);
            if (!PIPES.contains(current) && !ENDS.contains(current))
                this.move(1, 0);
            else
                break;
        }
        // walk the pipes until you get to the next instruction
        while (true) {
            final char current = this.sourceCode.getCharacter(this);
            if (PIPES.contains(current)) {
                if ('-' == current)
                    this.move(1, 0);
                else if ('/' == current) {
                    if ('/' == this.peek(1, -1))
                        this.move(1, -1);
                    else if ('-' == this.peek(1, 0))
                        this.move(1, 0);
                    else
                        return false;
                } else if ('\\' == current) {
                    if ('\\' == this.peek(1, 1))
                        this.move(1, 1);
                    else if ('-' == this.peek(1, 0))
                        this.move(1, 0);
                    else
                        return false;
                } else
                    return false;
            } else if (ENDS.contains(current)) {
                if ('/' == this.peek(0, -1))
                    this.move(0, -1);
                else if ('\\' == this.peek(0, 1))
                    this.move(0, 1);
                else
                    return false;
            } else
                return true;
        }
    }


    protected char peek(final int xOffset, final int yOffset) {
        return this.sourceCode.getCharacter(this.x + xOffset, this.y + yOffset);
    }

    protected char read() {
        return this.sourceCode.getCharacter(this.x, this.y);
    }

    protected void move(final int xOffset, final int yOffset) {
        this.x = this.x + xOffset;
        this.y = this.y + yOffset;
    }
}
