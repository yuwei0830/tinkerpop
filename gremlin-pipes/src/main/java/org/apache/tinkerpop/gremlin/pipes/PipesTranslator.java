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

package org.apache.tinkerpop.gremlin.pipes;

import org.apache.tinkerpop.gremlin.pipes.jsr223.GremlinPipesScriptEngineFactory;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Translator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class PipesTranslator implements Translator.ScriptTranslator {

    private static final String EMPTY_STRING = new String();

    private final String traversalSource;
    private final List<String> sourceCode = PipesTranslator.generateSourceCode();

    private static Map<Enum, String> ENUM_MAP = new HashMap<Enum, String>() {{
        put(Compare.eq, "==");
        put(Compare.gt, ">");
        put(Compare.gte, ">=");
        put(Compare.lt, "<");
        put(Compare.lte, "<=");
        put(Compare.neq, "!=");
    }};

    private PipesTranslator(final String traversalSource) {
        this.traversalSource = traversalSource;
    }

    public static PipesTranslator of(final String traversalSource) {
        return new PipesTranslator(traversalSource);
    }

    @Override
    public String getTraversalSource() {
        return this.traversalSource;
    }

    @Override
    public String getTargetLanguage() {
        return new GremlinPipesScriptEngineFactory().getLanguageName();
    }

    @Override
    public String translate(final Bytecode bytecode) {

        int cRow = 10;
        int cCol = 0;
        this.insertStatement(cRow, cCol, this.traversalSource);
        cCol = cCol + this.traversalSource.length();
        for (final Bytecode.Instruction instruction : bytecode.getStepInstructions()) {
            final String[] statements = processInstruction(instruction.getOperator(), instruction.getArguments()).split("\n");
            int tempCol = cCol;
            int tempRow = cRow;
            for (int i = 0; i < statements.length; i++) {
                if (0 == i)
                    tempCol = tempCol + statements[i].length() + 1;
                this.insertStatement(cRow + i, cCol, i == 0 ? "-" + statements[i] : statements[i]);
            }
            cRow = tempRow;
            cCol = tempCol;
        }
        StringBuilder builder = new StringBuilder();
        for (final String line : this.sourceCode) {
            builder.append(line).append("\n");
        }
        return builder.toString().trim();
    }

    private static String processInstruction(final String operator, final Object[] arguments) {
        switch (operator) {
            case "out":
                return "~" + commaSeparateOrNoneArguments(arguments) + "~>";
            case "in":
                return "<~" + commaSeparateOrNoneArguments(arguments) + "~";
            case "both":
                return "<~" + commaSeparateOrNoneArguments(arguments) + "~>";
            case "outE":
                return "~" + commaSeparateOrNoneArguments(arguments) + "~>>";
            case "inE":
                return "<<~" + commaSeparateOrNoneArguments(arguments) + "~";
            case "bothE":
                return "<<~" + commaSeparateOrNoneArguments(arguments) + "~>>";
            case "outV":
                return ">>";
            case "inV":
                return "<<";
            case "bothV":
                return "<<>>";
            case "count":
                return "}$";
            case "sum":
                return "}+";
            case "barrier":
                return "}" + commaSeparateOrNoneArguments(arguments) + "{";
            case "values":
                return "~" + commaSeparateOrNoneArguments(arguments);
            case "select":
                return "*" + commaSeparateOrNoneArguments(arguments);
            case "as":
                return "@" + commaSeparateOrNoneArguments(arguments);
            case "has":
                return "#" + commaSeparateOrNoneArguments(arguments).replaceAll(",", "");
            case "where": {
                if (arguments[0] instanceof String) {
                    return "#*" + arguments[0] + ENUM_MAP.get(((P) arguments[1]).getBiPredicate()) + "*" + ((P) arguments[1]).getOriginalValue();
                } else if (arguments[0] instanceof P) {
                    return "#" + ENUM_MAP.get(((P) arguments[0]).getBiPredicate()) + "*" + ((P) arguments[0]).getOriginalValue();
                }
            }

            case "match": {
                String statement = "M[ ]\n";
                for (final Object bytecode : arguments) {
                    statement = statement + "   |" + PipesTranslator.of("-").translate((Bytecode) bytecode) + "\n";
                }
                return statement;
            }
            default:
                return 0 == arguments.length ? operator : operator + "(" + commaSeparateOrNoneArguments(arguments) + ")";
        }
    }

    private static String commaSeparateOrNoneArguments(final Object[] arguments) {
        if (0 == arguments.length)
            return EMPTY_STRING;
        else {
            final StringBuilder builder = new StringBuilder();
            for (final Object argument : arguments) {
                builder.append(stringifyArgument(argument)).append(',');
            }
            builder.deleteCharAt(builder.length() - 1);
            return builder.toString();
        }
    }

    private static String stringifyArgument(final Object argument) {
        if (argument instanceof Enum)
            return "`" + ((Enum) argument).name();
        else if (argument instanceof Number) {
            if (argument instanceof Integer)
                return argument.toString();
            else if (argument instanceof Long)
                return argument + "l";
            else if (argument instanceof Float)
                return argument + "f";
            else if (argument instanceof Double)
                return argument + "d";
            else
                throw new IllegalArgumentException("Unknown number: " + argument);
        } else if (argument instanceof P) {
            final BiPredicate predicate = ((P) argument).getBiPredicate();
            if (predicate instanceof Compare)
                return ENUM_MAP.get(predicate) + stringifyArgument(((P) argument).getValue());
            else
                return argument.toString();
        } else if (argument instanceof Bytecode.Binding)
            return stringifyArgument(((Bytecode.Binding) argument).value());
        else
            return argument.toString();
    }

    private static List<String> generateSourceCode() {
        final List<String> sourceCode = new ArrayList<>(25);
        for (int rows = 0; rows < 25; rows++) {
            StringBuilder s = new StringBuilder();
            for (int columns = 0; columns < 100; columns++) {
                s.append(' ');
            }
            sourceCode.add(s.toString());
        }
        return sourceCode;
    }

    private void insertStatement(final int row, final int column, final String statement) {
        final String rLine = this.sourceCode.get(row);
        final String newRLine = rLine.substring(0, column) + statement + rLine.substring(column + statement.length());
        this.sourceCode.remove(row);
        this.sourceCode.add(row, newRLine);
    }


}
