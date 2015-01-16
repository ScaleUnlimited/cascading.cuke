package com.scaleunlimited.cascading.cuke;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

public class WorkflowContextTest {

    @Test
    public void testMakePathSafe() throws IOException {
        // Trim leading & trailing spaces
        // Get rid of |, ?, etc.
        // Replace remaining spaces with _
        assertEquals("word1_word2", WorkflowContext.makePathSafe("  word1 | ? word2  "));
    }

}
