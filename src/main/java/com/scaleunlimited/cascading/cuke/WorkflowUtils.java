package com.scaleunlimited.cascading.cuke;

import java.io.IOException;

import org.apache.hadoop.fs.PathNotFoundException;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.local.LocalFlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.google.common.base.CaseFormat;
import com.scaleunlimited.cascading.local.KryoScheme;

public class WorkflowUtils {

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static TupleEntryCollector openTextForWrite(WorkflowPlatform platform, String path) throws IOException {
        TupleEntryCollector writer = null;

        if (platform == WorkflowPlatform.DISTRIBUTED) {
            Tap tap = new Hfs(new cascading.scheme.hadoop.TextLine(), path, SinkMode.REPLACE);
            writer = tap.openForWrite(new HadoopFlowProcess());
        } else if (platform == WorkflowPlatform.LOCAL) {
            Tap tap = new FileTap(new cascading.scheme.local.TextLine(), path, SinkMode.REPLACE);
            writer = tap.openForWrite(new LocalFlowProcess());
        } else {
            throw new RuntimeException("Unknown platform: " + platform);
        }

        return writer;
    }

    public static TupleEntryIterator openBinaryForRead(WorkflowPlatform platform, String readPath, Fields fields) throws Throwable {
        if (platform == WorkflowPlatform.DISTRIBUTED) {
            Tap tap = new Hfs(new cascading.scheme.hadoop.SequenceFile(fields), readPath);
            if (!tap.resourceExists(new HadoopFlowProcess())) {
                throw new PathNotFoundException(String.format("The Hadoop path \"%s\" doesn't exist", readPath));
            }
            return tap.openForRead(new HadoopFlowProcess());
        } else if (platform == WorkflowPlatform.LOCAL) {
            Tap tap = new FileTap(new KryoScheme(fields), readPath);
            if (!tap.resourceExists(new LocalFlowProcess())) {
                throw new PathNotFoundException(String.format("The local path \"%s\" doesn't exist", readPath));
            }
            return tap.openForRead(new LocalFlowProcess());
        } else {
            throw new IllegalArgumentException(String.format("The workflow platform %s is unknown", platform));
        }
    }

    /**
     * Diffs between a field-value and a TupleEntry.
     * NOTE: this method attempts to fuzzy-match field names (camelcase, underscore and all lowercase).
     *
     * @param te tuple entry
     * @param fieldName field
     * @param expected expected value
     * @return the diff, or null if match
     */
    public static TupleDiff diffTupleAndTarget(TupleEntry te, String fieldName, String expected) {
        return diffTupleAndTarget(te, fieldName, expected, true);
    }

    /**
     * Diffs between a field-value and a TupleEntry.
     * NOTE: this method attempts to fuzzy-match field names (camelcase, underscore and all lowercase).
     *
     * @param te tuple entry
     * @param fieldName field
     * @param expected expected value
     * @param fieldNameFuzzyMatching should fuzzy matching be applied to field names?
     * @return the diff, or null if matched
     */
    public static TupleDiff diffTupleAndTarget(TupleEntry te, String fieldName, String expected, boolean fieldNameFuzzyMatching) {
        final Fields fields = te.getFields();
        if (!fields.contains(new Fields(fieldName))) { // if we couldn't find the fieldName, try both underscore and camelcase
            if(fieldNameFuzzyMatching) {
                String underscore = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, fieldName);

                // NOTE: if the string is already camel-case, the following conversion will result in
                // an all-lowercased string without underscores or camelcase. Which might actually be desirable
                // since it covers an additional case of all lowercased
                String camelcase = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, fieldName);
                if (fields.contains(new Fields(underscore))) {
                    fieldName = underscore;
                } else if (fields.contains(new Fields(camelcase))) {
                    fieldName = camelcase;
                } else if (fields.contains(new Fields(camelcase.toLowerCase()))) {
                    fieldName = camelcase.toLowerCase();
                } else {
                    throw new IllegalArgumentException("Field " + fieldName + " not found in " + fields);
                }
            } else {
                throw new IllegalArgumentException("Field " + fieldName + " not found in " + fields);
            }
        }
        String tupleValue = te.getString(fieldName);
        if ((tupleValue == null) && !expected.equals("null")) {
            return new TupleDiff(fieldName, expected, tupleValue, TupleDiff.Type.MISSING);
        } else if ((tupleValue != null) && expected.equals("null")) {
            return new TupleDiff(fieldName, expected, tupleValue, TupleDiff.Type.NULL_EXPECTED);
        } else if ((tupleValue != null) && !tupleValue.equals(expected)) {
            return new TupleDiff(fieldName, expected, tupleValue, TupleDiff.Type.NOT_EQUAL);
        }
        return null;
    }

}
