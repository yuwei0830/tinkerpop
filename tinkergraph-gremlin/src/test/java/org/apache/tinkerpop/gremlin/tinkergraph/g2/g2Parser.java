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
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Arrays;
import java.util.LinkedList;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class g2Parser {

    private static final char NO_CHAR = '\u0489';

    private char[][] source;
    private int cRow;
    private int cColumn;
    private int numRows;
    private int numCols;

    private LinkedList<Pair<Integer, Integer>> traversalStack = new LinkedList<>();

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
        for (String line : lines) {
            line = line.replaceAll("\\s+$", "");
            if (line.length() > this.numCols)
                this.numCols = line.length();
        }
        this.numRows = lines.length;
        this.source = new char[this.numRows][this.numCols];
        int row = 0;
        for (String line : lines) {
            line = line.replaceAll("\\s+$", "");
            final char[] chars = line.toCharArray();
            for (int column = 0; column < this.numCols; column++) {
                this.source[row][column] = column > chars.length - 1 ? ' ' : chars[column];
            }
            row++;
        }
        this.advanceToSource();
    }

    public Bytecode getBytecode() {
        final Bytecode bytecode = new Bytecode();
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

    private void advanceToSource() {
        for (int i = 0; i < this.source.length; i++) {
            for (int j = 0; j < this.source[i].length; j++) {
                if (this.source[i][j] == 'g' && this.source[i][j + 1] == '-') {
                    this.cRow = i;
                    this.cColumn = j;
                    return;
                }
            }
        }
        throw new IllegalStateException("The source does not have a g traversal source");
    }

    private int locateAdjacentTraversals() {
        int childCounter = 0;
        int tempRow = this.cRow;
        int tempColumn = this.cColumn;
        this.traversalStack.push(Pair.with(this.cRow, this.cColumn + 1));
        this.cRow++;
        if ('\\' == this.currentCharacter()) {
            this.traversalStack.push(Pair.with(this.cRow, this.cColumn + 1));
            childCounter++;
        }
        this.cRow = tempRow;
        this.cColumn = tempColumn;
        this.cRow--;
        if ('/' == this.currentCharacter()) {
            this.traversalStack.push(Pair.with(this.cRow, this.cColumn + 1));
            childCounter++;
        }
        this.cRow = tempRow;
        this.cColumn = tempColumn;
        return childCounter;
    }

    private void resetToTraversalHead() {
        final Pair<Integer, Integer> point = this.traversalStack.pop();
        this.cRow = point.getValue0();
        this.cColumn = point.getValue1();
    }

    private boolean nextInstruction(final Bytecode bytecode) {
        Character current;
        while (NO_CHAR != (current = this.currentCharacter())) {
            if ('-' != current)
                break;
            this.advance();
        }
        final String instruction = this.advanceThroughNextInstruction();

        if (null == instruction)
            return false;

        final String operator = this.getOperator(instruction);
        System.out.println(operator + "::" + Arrays.toString(this.getArguments(instruction)));
        if (operator.equals("by")) {
            final int numberOfChildren = this.locateAdjacentTraversals();
            for (int i = 0; i < numberOfChildren; i++) {
                this.resetToTraversalHead();
                final Bytecode childBytecode = this.getBytecode();
                bytecode.addStep("by", childBytecode);
            }
            this.resetToTraversalHead();
        } else if (!instruction.equals("g"))
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
        else if (instruction.equals("["))
            return "by";
        else if (instruction.startsWith("}$"))
            return "count";
        else if (instruction.startsWith("}+"))
            return "sum";
        else if (instruction.equals("i"))
            return "identity";
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
        while (NO_CHAR != (current = this.currentCharacter())) {
            if ('-' == current || ('[' == current && !instruction.isEmpty()))
                break;
            if ('[' == current && instruction.isEmpty()) {
                this.advance();
                return "[";
            } else {
                instruction = instruction + current;
                this.advance();
            }
        }
        return instruction.trim().isEmpty() ? null : instruction;

    }

    //////////

    private void advance() {
        this.cColumn++;
    }

    private Character currentCharacter() {
        return this.inSource() ? this.source[this.cRow][this.cColumn] : NO_CHAR;
    }

    private boolean inSource() {
        return this.cRow < this.numRows && this.cColumn < this.numCols;
    }

}
