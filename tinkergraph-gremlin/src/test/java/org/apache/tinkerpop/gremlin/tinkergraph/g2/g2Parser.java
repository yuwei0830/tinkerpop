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
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.javatuples.Pair;
import org.javatuples.Triplet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class g2Parser {

    private static final char NO_CHAR = '\u0489';
    private static final char SPACE = ' ';
    private static Set<Character> PIPES = new HashSet<>(Arrays.asList('-', '/', '|', '\\'));
    private static Set<Character> ENDS = new HashSet<>(Arrays.asList(SPACE, NO_CHAR));

    private static final List<Triplet<Pattern, String, Boolean>> OPERATOR_PATTERNS = new ArrayList<Triplet<Pattern, String, Boolean>>() {{
        add(Triplet.with(Pattern.compile("(?<=^~)[^~]*(?=~>>)"), "outE", false));
        add(Triplet.with(Pattern.compile("(?<=<<~)[^~]*(?=~)"), "inE", false));
        add(Triplet.with(Pattern.compile("(?<=<<~)[^~]*(?=~>>)"), "bothE", false));
        add(Triplet.with(Pattern.compile("(?<=^~)[^~]*(?=~>)"), "out", false));
        add(Triplet.with(Pattern.compile("(?<=^<~)[^~]*(?=~>)"), "both", false));
        add(Triplet.with(Pattern.compile("(?<=^<~)[^~]*(?=~)"), "in", false));
        add(Triplet.with(Pattern.compile("(?<=^<<)$"), "inV", false));
        add(Triplet.with(Pattern.compile("(?<=^>>)$"), "outV", false));
        add(Triplet.with(Pattern.compile("(?<=^<<>>)$"), "bothV", false));
        // add(Triplet.with(Pattern.compile("(?<=%)$"), "map", true));
        add(Triplet.with(Pattern.compile("(?<=^#)$"), "filter", true));
        add(Triplet.with(Pattern.compile("(?<=^~)(.*)"), "values", false));
        add(Triplet.with(Pattern.compile("(?<=^@)(.*)"), "as", false));
        add(Triplet.with(Pattern.compile("(?<=^#)([\\*].*)"), "where", false));
        add(Triplet.with(Pattern.compile("(?<=^\\*)(.*)"), "select", false));
        add(Triplet.with(Pattern.compile("(?<=^#!)(.*)"), "hasNot", false));
        add(Triplet.with(Pattern.compile("(?<=^#)(.*)"), "has", false));
        add(Triplet.with(Pattern.compile("(?<=^!)$"), "not", true));
        add(Triplet.with(Pattern.compile("(?<=^}\\$)$"), "count", false));
        add(Triplet.with(Pattern.compile("(?<=^}\\+)$"), "sum", false));
        add(Triplet.with(Pattern.compile("(?<=^}%\\$)(.*)"), "groupCount", false));
        add(Triplet.with(Pattern.compile("(?<=^}%)(.*)"), "group", false));
        add(Triplet.with(Pattern.compile("(?<=^})([0-9]*)(?=\\{)"), "barrier", false));
        add(Triplet.with(Pattern.compile("(?<=^})([a-z|A-Z]*)(?=\\{)"), "aggregate", false));
        add(Triplet.with(Pattern.compile("(?<=^i)$"), "identity", false));
        add(Triplet.with(Pattern.compile("(?<=^M)$"), "match", true));
        add(Triplet.with(Pattern.compile("(?<=^U)$"), "union", true));
        add(Triplet.with(Pattern.compile("(?<=^x)(.*)"), "times", false));
        add(Triplet.with(Pattern.compile("(?<=\\{)"), "flatMap", true));
    }};

    private static final Pattern PREDICATE_PATTERN = Pattern.compile("([^!<>=]+)(([<>][=]?)|([!=]=))([^!<>=]+)");
    private static final Pattern NUMBER_PATTERN = Pattern.compile("^[0-9]");

    // accessible objects

    private String traversalSource;
    private Bytecode rootTraversal;

    // source matrix

    private char[][] source;
    private int cRow;
    private int cColumn;
    private int numRows;
    private int numCols;
    private Pair<Integer, Integer> repeat = null;

    public g2Parser(final String source) {
        this.buildCharacterSource(source);
    }

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
                this.source[row][column] = column > chars.length - 1 ? SPACE : chars[column];
            }
            row++;
        }
        this.traversalSource = this.advanceToSource();
        this.rootTraversal = this.advanceThroughBytecode();
    }

    public Bytecode getBytecode() {
        return this.rootTraversal;
    }

    public String getSourceCode() {
        final StringBuilder builder = new StringBuilder();
        for (final char[] row : this.source) {
            builder.append(new String(row)).append("\n");
        }
        return builder.toString();
    }

    public String getTraversalSource() {
        return this.traversalSource;
    }

    //////////////

    private String advanceToSource() {
        for (int column = 0; column < this.numCols; column++) {
            for (int row = 0; row < this.numRows; row++) {
                if (SPACE != this.source[row][column]) {
                    this.cRow = row;
                    this.cColumn = column;
                    return this.advanceThroughNextInstruction();
                }
            }
        }
        throw new IllegalStateException("The source does not have a traversal source defined");
    }

    private Bytecode advanceThroughBytecode() {
        final Bytecode bytecode = new Bytecode();
        while (processNextInstruction(bytecode)) {

        }
        //System.out.println(bytecode);
        return bytecode;
    }

    private static Bytecode processBytecode(final String traversal) {
        return new g2Parser(traversal).getBytecode();
    }

    private boolean processNextInstruction(final Bytecode bytecode) {
        String instruction = this.advanceThroughNextInstruction();
        if (null == instruction) {
            if (null != this.repeat) {
                final Pair<Integer, Integer> temp = this.repeat;
                this.repeat = null;
                bytecode.addStep("repeat", this.getChildBytecode().toArray());
                int rTemp = this.cRow;
                int cTemp = this.cColumn;
                this.setCursor(temp.getValue0() + 1, temp.getValue1() + 1);
                final Bytecode untilTraversal = this.getChildBytecode().get(0);
                for (final Bytecode.Instruction inst : untilTraversal.getStepInstructions()) {
                    bytecode.addStep(inst.getOperator(), inst.getArguments());
                }
                this.setCursor(rTemp, cTemp);
                return true;
            } else {
                return false;
            }
        }
        ///
        instruction = instruction.trim();
        //System.out.println(instruction + "!!");
        for (final Triplet<Pattern, String, Boolean> entry : OPERATOR_PATTERNS) {
            final Pattern pattern = entry.getValue0();
            final String operand = entry.getValue1();
            final Boolean children = entry.getValue2();
            final Matcher matcher = pattern.matcher(instruction);
            if (matcher.find()) {
                final String arguments = matcher.group();
                if (!arguments.isEmpty()) {
                    Object[] args;
                    if (operand.equals("has")) {
                        final Matcher m = PREDICATE_PATTERN.matcher(instruction.substring(1));
                        args = m.find() ?
                                new Object[]{m.group(1), this.mapToPredicate(m.group(2), m.group(5))} :
                                this.processArguments(arguments);
                    } else if (operand.equals("where")) {
                        final Matcher m = PREDICATE_PATTERN.matcher(instruction.substring(1));
                        args = m.find() ?
                                new Object[]{m.group(1).substring(1), this.mapToPredicate(m.group(2), m.group(5).substring(1))} :
                                this.processArguments(arguments);
                    } else
                        args = this.processArguments(arguments);
                    bytecode.addStep(operand, args);
                } else if (children) {
                    this.moveCursor(0, 1);
                    bytecode.addStep(operand, this.getChildBytecode().toArray());
                } else
                    bytecode.addStep(operand);
                return true;
            }
        }
        ////////
        if (instruction.equals("[")) {
            for (final Bytecode b : this.getChildBytecode()) {
                if (b.getInstructions().iterator().hasNext())
                    bytecode.addStep("by", b);
                else
                    bytecode.addStep("by");
            }
        } else if (instruction.contains("(")) {
            bytecode.addStep(instruction.substring(0, instruction.indexOf('(')),
                    this.processArguments(instruction.substring(instruction.indexOf('(') + 1, instruction.length() - 1)));
        } else
            bytecode.addStep(instruction);

        return true;
    }

    private List<Bytecode> getChildBytecode() {
        return this.getChildBytecode(true, 0, this.cRow, this.cColumn, new ArrayList<>());
    }

    private List<Bytecode> getChildBytecode(final boolean branch, final int previousRowOffset, final int cRow, final int cColumn, final List<Bytecode> children) {
        this.setCursor(cRow, cColumn);
        int endRow = -1;
        int endColumn = -1;
        if (branch) {
            int openNest = 1;
            while (true) {
                if ('[' == this.currentCharacter()) {
                    openNest++;
                } else if (']' == this.currentCharacter()) {
                    openNest--;
                    if (0 == openNest) {
                        endRow = this.cRow;
                        endColumn = this.cColumn + 1;
                        break;
                    }
                } else if (NO_CHAR == this.currentCharacter()) {
                    endRow = this.cRow;
                    endColumn = this.cColumn + 1;
                    break;
                }
                this.moveCursor(0, 1);
            }
        }
        this.setCursor(cRow, cColumn);
        if ('|' == this.peekCharacter(-1, 0) && previousRowOffset <= 0) {
            this.getChildBytecode(false, -1, cRow - 1, cColumn, children);
        }
        this.setCursor(cRow, cColumn);
        if ('/' == this.peekCharacter(-1, 0)) {
            this.getChildBytecode(false, -1, cRow - 1, cColumn + 1, children);
        }
        //
        this.setCursor(cRow, cColumn);
        if ('-' == this.peekCharacter(0, 1) || '-' == this.currentCharacter()) {
            this.moveCursor(0, 1);
            children.add(this.advanceThroughBytecode());
        }
        //
        this.setCursor(cRow, cColumn);
        if ('\\' == this.peekCharacter(1, 0)) {
            this.getChildBytecode(false, 1, cRow + 1, cColumn + 1, children);
        }
        //
        this.setCursor(cRow, cColumn);
        if ('|' == this.peekCharacter(1, 0) && previousRowOffset >= 0) {
            this.getChildBytecode(false, 1, cRow + 1, cColumn, children);
        }
        if (branch)
            this.setCursor(endRow, endColumn);
        return children;
    }

    private boolean advanceToNextInstruction() {
        while (true) {
            final char current = this.currentCharacter();
            if ('[' == current && '^' == this.peekCharacter(1, 0)) {
                this.repeat = Pair.with(this.cRow, this.cColumn);
                this.moveCursor(0, 1);
                return true;
            }
            if (PIPES.contains(current)) {
                if ('-' == current)
                    this.moveCursor(0, 1);
                else if ('/' == current) {
                    if ('/' == this.peekCharacter(-1, 1))
                        this.moveCursor(-1, 1);
                    else if ('-' == this.peekCharacter(0, 1))
                        this.moveCursor(0, 1);
                    else
                        return false;
                } else if ('\\' == current) {
                    if ('\\' == this.peekCharacter(1, 1))
                        this.moveCursor(1, 1);
                    else if ('-' == this.peekCharacter(0, 1))
                        this.moveCursor(0, 1);
                    else
                        return false;
                } else
                    return false;
            } else if (ENDS.contains(current)) {
                if ('/' == this.peekCharacter(-1, 0))
                    this.moveCursor(-1, 0);
                else if ('\\' == this.peekCharacter(1, 0))
                    this.moveCursor(1, 0);
                else
                    return false;
            } else
                return true;
        }
    }

    private String advanceThroughNextInstruction() {
        if (!this.advanceToNextInstruction())
            return null;
        String instruction = "";
        char current;
        while (NO_CHAR != (current = this.currentCharacter())) {
            if ('-' == current || ('[' == current && !instruction.isEmpty()))
                break;
            if ('[' == current && instruction.isEmpty()) {
                this.moveCursor(0, 1);
                return "[";
            } else if (']' == current) {
                this.moveCursor(0, 1);
                break;
            } else {
                instruction = instruction + current;
                this.moveCursor(0, 1);
            }
        }
        return instruction.trim().isEmpty() ? null : instruction;

    }

    //////////

    private char currentCharacter() {
        return this.cRow >= 0 && this.cRow < this.numRows && this.cColumn >= 0 && this.cColumn < this.numCols ?
                this.source[this.cRow][this.cColumn] :
                NO_CHAR;
    }

    private char peekCharacter(final int rOffset, final int cOffset) {
        final int tempRow = this.cRow;
        final int tempColumn = this.cColumn;
        this.moveCursor(rOffset, cOffset);
        final char peek = this.currentCharacter();
        this.setCursor(tempRow, tempColumn);
        return peek;
    }

    private void moveCursor(final int rOffset, final int cOffset) {
        this.setCursor(this.cRow + rOffset, this.cColumn + cOffset);
    }

    private void setCursor(final int cRow, final int cColumn) {
        this.cRow = cRow;
        this.cColumn = cColumn;
    }

    private <V> P<V> mapToPredicate(final String predicate, final V value) {
        if (predicate.equals("=="))
            return P.eq(value);
        else if (predicate.equals(">="))
            return P.gte(value);
        else if (predicate.equals("<="))
            return P.lte(value);
        else if (predicate.equals(">"))
            return P.gt(value);
        else if (predicate.equals("<"))
            return P.lt(value);
        else if (predicate.equals("!="))
            return P.neq(value);
        else
            throw new IllegalStateException("Can not parse predicate: " + predicate + ":" + value);

    }

    private Object[] processArguments(final String arguments) {
        final String[] args = arguments.split(",");
        final Object[] objects = new Object[args.length];
        for (int i = 0; i < args.length; i++) {
            final String arg = args[i];
            if (arg.equals("true") || arg.equals("false"))
                objects[i] = Boolean.valueOf(arg);
            else if (NUMBER_PATTERN.matcher(arg).find()) {
                if (arg.contains(".") && arg.endsWith("f"))
                    objects[i] = Float.valueOf(arg);
                else if (arg.contains("."))
                    objects[i] = Double.valueOf(arg);
                else if (arg.endsWith("l"))
                    objects[i] = Long.valueOf(arg);
                else
                    objects[i] = Integer.valueOf(arg);
            } else if (arg.startsWith("'") && arg.endsWith("'")) {
                objects[i] = arg.substring(1, arg.length() - 1);
            } else
                objects[i] = arg;
        }
        return objects;
    }

}
