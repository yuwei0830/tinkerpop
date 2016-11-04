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

package org.apache.tinkerpop.gremlin.tinkergraph.g2;

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class g2Parser {

    private char[][] source;
    private int cRow;
    private int cColumn;
    private int numRows;
    private int numCols;

    public g2Parser(final File file) {
        try {
            final BufferedReader reader = new BufferedReader(new FileReader(file));
            final StringBuilder builder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                builder.append(line).append("\n");
            }
            this.buildCharacterSource(builder.toString());
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    public g2Parser(final String source) {
        this.buildCharacterSource(source);
    }

    private void buildCharacterSource(final String source) {
        final String[] lines = source.split("\n");
        for (final String line : lines) {
            if (line.length() > this.numCols)
                this.numCols = line.length();
        }
        this.numRows = lines.length;
        this.source = new char[this.numRows][this.numCols];
        int row = 0;
        for (final String line : lines) {
            final char[] chars = line.toCharArray();
            for (int column = 0; column < this.numCols; column++) {
                this.source[row][column] = column > chars.length - 1 ? ' ' : chars[column];
            }
            row++;
        }
    }

    public Bytecode getBytecode() {
        final Bytecode bytecode = new Bytecode();
        this.resetToSource();
        while (nextInstruction(bytecode)) {

        }
        System.out.println(bytecode);
        return bytecode;
    }

    public String toString() {
        final StringBuilder builder = new StringBuilder();
        for (final char[] row : this.source) {
            builder.append(new String(row)).append("\n");
        }
        return builder.toString();
    }

    private void resetToSource() {
        for (int i = 0; i < this.source.length; i++) {
            for (int j = 0; j < this.source[i].length; j++) {
                if (this.source[i][j] == 'g') {
                    this.cRow = i;
                    this.cColumn = j;
                    return;
                }
            }
        }
        throw new IllegalStateException("The source does not have a g traversal source");
    }

    private boolean nextInstruction(final Bytecode bytecode) {
        Character current;
        while ((current = this.currentCharacter()) != null) {
            if ('-' != current)
                break;
            this.advance();
        }
        final String instruction = this.advanceThroughNextInstruction();

        if (null == instruction)
            return false;

        //System.out.println(this.getOperator(instruction) + "::" + Arrays.toString(this.getArguments(instruction)));
        if (!instruction.equals("g"))
            bytecode.addStep(this.getOperator(instruction), this.getArguments(instruction));
        return true;
    }

    private String getOperator(final String instruction) {
        if (instruction.startsWith("~") && instruction.endsWith(">"))
            return "out";
        else if (instruction.startsWith("<") && instruction.endsWith("~"))
            return "in";
        else if (instruction.startsWith("<") && instruction.endsWith(">"))
            return "both";
        else if (instruction.startsWith("~"))
            return "values";
        else if (instruction.startsWith("@"))
            return "as";
        else if (instruction.startsWith("*"))
            return "select";
        else
            return instruction.contains("(") ? instruction.substring(0, instruction.indexOf('(')) : instruction;
    }

    private Object[] getArguments(final String instruction) {
        final String[] args;
        if (instruction.contains("~") && (instruction.startsWith("<") || instruction.endsWith(">")))
            args = stringify(instruction.replaceAll("~", "").replaceAll("<", "").replaceAll(">", "").split(","));
        else if (instruction.startsWith("@") || instruction.startsWith("*") || instruction.startsWith("~"))
            args = stringify(instruction.substring(1).split(","));
        else if (instruction.contains("("))
            args = instruction.substring(instruction.indexOf("(") + 1, instruction.length() - 1).split(",");
        else
            args = new String[0];

        Object[] arguments = new Object[args.length];
        for (int i = 0; i < args.length; i++) {
            final String s = args[i];
            if (s.startsWith("'") && s.endsWith("'"))
                arguments[i] = s.substring(1, s.length() - 1);
            else
                arguments[i] = Double.valueOf(s);
            //else
            //    throw new IllegalStateException("The following argument can not be parsed:" + s);
        }
        return arguments;
    }

    private String[] stringify(final String[] args) {
        for (int i = 0; i < args.length; i++) {
            args[i] = "'" + args[i] + "'";
        }
        return args;
    }

    private String advanceThroughNextInstruction() {
        String instruction = "";
        Character current;
        while ((current = this.currentCharacter()) != null) {
            if ('-' == current || '[' == current)
                break;
            instruction = instruction + current;
            this.advance();
        }
        return instruction.isEmpty() ? null : instruction;

    }

    //////////

    private void advance() {
        this.cColumn++;
    }

    private Character currentCharacter() {
        return this.inSource() ? this.source[this.cRow][this.cColumn] : null;
    }

    private boolean inSource() {
        return this.cRow < this.numRows && this.cColumn < this.numCols;
    }

}
