package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.junit.Test;

import java.io.ByteArrayInputStream;

import static org.junit.Assert.assertEquals;

/**
 * @author Daniel Kuppitz (http://gremlin.guru)
 */
public class GraphSONReaderTest {

    @Test
    public void elementOrderShouldNotMatter() throws Exception {
        final GraphSONReader reader = GraphSONReader.build().create();
        final String bytecodeJSON1 = "{\"@type\":\"g:Bytecode\",\"@value\":{\"step\":[[\"addV\",\"poc_int\"],[\"property\",\"bigint1value\",{\"@type\":\"g:Int32\",\"@value\":-4294967295}]]}}";
        final String bytecodeJSON2 = "{\"@value\":{\"step\":[[\"addV\",\"poc_int\"],[\"property\",\"bigint1value\",{\"@value\":-4294967295,\"@type\":\"g:Int32\"}]]},\"@type\":\"g:Bytecode\"}";
        final Bytecode bytecode1 = reader.readObject(new ByteArrayInputStream(bytecodeJSON1.getBytes()), Bytecode.class);
        final Bytecode bytecode2 = reader.readObject(new ByteArrayInputStream(bytecodeJSON2.getBytes()), Bytecode.class);
        assertEquals(bytecode1, bytecode2);
    }
}