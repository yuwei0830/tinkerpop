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

import static org.apache.tinkerpop.gremlin.pipes.parser.SymbolHelper.NO_CHAR;
import static org.apache.tinkerpop.gremlin.pipes.parser.SymbolHelper.SPACE;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class SourceCode {

    private final char[][] source;
    private final int maxX;
    private final int maxY;

    public SourceCode(final String sourceCode) {
        int tempMaxX = 0;
        final String[] lines = sourceCode.split("\n");
        for (String line : lines) {
            line = line.replaceAll("\\s+$", "");
            if (line.length() > tempMaxX)
                tempMaxX = line.length();
        }
        this.maxX = tempMaxX;
        this.maxY = lines.length;
        this.source = new char[this.maxY][this.maxX];
        int row = 0;
        for (String line : lines) {
            line = line.replaceAll("\\s+$", "");
            final char[] chars = line.toCharArray();
            for (int column = 0; column < this.maxX; column++) {
                this.source[row][column] = column > chars.length - 1 ? SPACE : chars[column];
            }
            row++;
        }
    }

    public int getMaxX() {
        return this.maxX;
    }

    public int getMaxY() {
        return this.maxY;
    }

    public char getCharacter(final int x, final int y) {
        return y >= 0 && y < this.maxY && x >= 0 && x < this.maxX ?
                this.source[y][x] :
                NO_CHAR;
    }

    public char getCharacter(final Walker walker) {
        return this.getCharacter(walker.getX(), walker.getY());
    }
}
