package com.scaleunlimited.cascading.cuke;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Test;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class WorkflowUtilsTest {
    
    @Test
    public void testExpandMacrosWithWindowsPath() throws IOException {
        WorkflowContext context = new WorkflowContext("name", this.getClass());
        context.setTestDir("test\\dir");
        final String expanded = WorkflowUtils.expandMacros(context, "${testdir}");
        assertTrue("Didn't end with expected result: " + expanded, expanded.endsWith("test\\dir"));
    }
    
    @Test
    public void testDiffTupleAndTargetFieldPermutations() {
        String fieldName = "foo_bar";
        TupleEntry te = new TupleEntry(new Fields(fieldName), Tuple.size(1));
        te.setString(fieldName, "1");
        assertNull(WorkflowUtils.diffTupleAndTarget(te, "foo_bar", "1"));
        assertNull(WorkflowUtils.diffTupleAndTarget(te, "fooBar", "1"));

        fieldName = "fooBar";
        te = new TupleEntry(new Fields(fieldName), Tuple.size(1));
        te.setString(fieldName, "1");
        assertNull(WorkflowUtils.diffTupleAndTarget(te, "foo_bar", "1"));
        assertNull(WorkflowUtils.diffTupleAndTarget(te, "fooBar", "1"));

        fieldName = "foobar";
        te = new TupleEntry(new Fields(fieldName), Tuple.size(1));
        te.setString(fieldName, "1");
        assertNull(WorkflowUtils.diffTupleAndTarget(te, "foo_bar", "1"));
        assertNull(WorkflowUtils.diffTupleAndTarget(te, "fooBar", "1"));
    }

    @Test
    public void testDiffTupleAndTargetExact() {
        String fieldName = "foo_bar";
        TupleEntry te = new TupleEntry(new Fields(fieldName), Tuple.size(1), false);
        te.setString(fieldName, "1");
        assertNull(WorkflowUtils.diffTupleAndTarget(te, "foo_bar", "1", false));
        try {
            WorkflowUtils.diffTupleAndTarget(te, "fooBar", "1", false);
            fail("");
        } catch (IllegalArgumentException e) {
        }

        fieldName = "fooBar";
        te = new TupleEntry(new Fields(fieldName), Tuple.size(1));
        te.setString(fieldName, "1");
        try {
            WorkflowUtils.diffTupleAndTarget(te, "foo_bar", "1", false);
            fail("");
        } catch (IllegalArgumentException e) {
        }
        assertNull(WorkflowUtils.diffTupleAndTarget(te, "fooBar", "1", false));

        fieldName = "foobar";
        te = new TupleEntry(new Fields(fieldName), Tuple.size(1));
        te.setString(fieldName, "1");
        try {
            WorkflowUtils.diffTupleAndTarget(te, "foo_bar", "1", false);
            fail("");
        } catch (IllegalArgumentException e) {
        }
        try {
            WorkflowUtils.diffTupleAndTarget(te, "fooBar", "1", false);
            fail("");
        } catch (IllegalArgumentException e) {
        }
    }
}
