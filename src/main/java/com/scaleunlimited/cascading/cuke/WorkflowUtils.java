package com.scaleunlimited.cascading.cuke;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;

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

import cucumber.api.DataTable;

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

    @SuppressWarnings({ "rawtypes", "unchecked" })
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
     * Expand the {testdir} macro using the context's setting for this.
     * 
     * @param context
     * @param s
     * @return
     * @throws IOException
     */
    public static String expandMacros(WorkflowContext context, String s) throws IOException {
        // Note that we have to use Matcher.quoteReplacement to avoid issues with '/' chars in
        // Windows paths...thanks Prasanth!
        return s.replaceAll("\\$\\{testdir\\}", Matcher.quoteReplacement(context.getTestDir()));
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

    public static void checkCounter(String targetCounterName, long minValue, long maxValue) {
        Long counterValue = null;
        String matchedCounterName = null;
        Map<String, Long> counters = WorkflowContext.getCurrentContext().getCounters();
        for (String counterName : counters.keySet()) {
            if (counterName.equals(targetCounterName) || counterName.endsWith("." + targetCounterName)) {
                if (matchedCounterName != null) {
                    throw new AssertionError(String.format("Counter \"%s\" for workflow %s has multiple matches: \"%s\" and \"%s\"",
                            targetCounterName,
                            WorkflowContext.getCurrentWorkflowName(),
                            matchedCounterName,
                            counterName));

                }

                matchedCounterName = counterName;
                counterValue = counters.get(counterName);
            }
        }

        // If we can't find the counter, then it has an implicit value of 0
        if (counterValue == null) {
            counterValue = 0L;
        }

        if (counterValue < minValue) {
            throw new AssertionError(String.format("Counter \"%s\" for workflow %s is too small, was %d, must be at least %d",
                    targetCounterName,
                    WorkflowContext.getCurrentWorkflowName(),
                    counterValue,
                    minValue));
        } else if (counterValue > maxValue) {
            throw new AssertionError(String.format("Counter \"%s\" for workflow %s is too bog, was %d, must be no more than %d",
                    targetCounterName,
                    WorkflowContext.getCurrentWorkflowName(),
                    counterValue,
                    maxValue));
        }
    }

    public static void matchResults(String directoryName, DataTable targetValues, boolean allowUnmatchedResults) throws Throwable  {
        // For every TupleEntry, see if we have a match with one of our target records.
        // If so, remove its index from the list, and make sure the list is empty when we're done.
        List<Map<String, String>> remainingValues = new ArrayList<Map<String,String>>(targetValues.asMaps(String.class, String.class));

        WorkflowContext context = WorkflowContext.getCurrentContext();
        WorkflowInterface workflow = context.getWorkflow();
        TupleEntryIterator iter = openForRead(context, workflow, directoryName);
        while (iter.hasNext()) {
            TupleEntry te = iter.next();

            boolean foundMatch = false;
            int leastDiffs = Integer.MAX_VALUE;
            Set<TupleDiff> leastDiffsSet = null;

            for (int i = 0; i < remainingValues.size() && !foundMatch; i++) {
                Map<String, String> row = remainingValues.get(i);
                Set<TupleDiff> tupleDiffs = diffTupleAndTarget(workflow, te, row);
                if(tupleDiffs.size() < leastDiffs) {
                    leastDiffs = tupleDiffs.size();
                    leastDiffsSet = tupleDiffs;
                }
                if (tupleDiffs.size() == 0) {
                    foundMatch = true;
                    remainingValues.remove(i);
                }
            }

            if (!foundMatch && !allowUnmatchedResults) {
                StringBuilder sb = new StringBuilder();
                sb.append(String.format("Record \"%s\" found for workflow %s in results \"%s\" that weren't in the target list",
                    te,
                    context.getWorkflowName(),
                    directoryName));
                sb.append("\n");
                for (TupleDiff diff: leastDiffsSet) {
                    sb.append(diff.toString()).append("\n");
                }
                throw new AssertionError(sb.toString());
            }
        }

        if (!remainingValues.isEmpty()) {
            throw new AssertionError(String.format("No record found for workflow %s in results \"%s\" that matched the target value %s",
                    context.getWorkflowName(),
                    directoryName,
                    remainingValues.get(0)));
        }
    }

    public static void dontMatchResults(String directoryName, DataTable excludedValues) throws Throwable  {
        WorkflowContext context = WorkflowContext.getCurrentContext();
        WorkflowInterface workflow = context.getWorkflow();
        TupleEntryIterator iter = openForRead(context, workflow, directoryName);
        List<Map<String, String>> excludedValuesMap = excludedValues.asMaps(String.class, String.class);
        while (iter.hasNext()) {
            TupleEntry te = iter.next();

            for (Map<String, String> row: excludedValuesMap) {
                if (tupleMatchesTarget(workflow, te, row)) {
                    throw new AssertionError(String.format("Record \"%s\" found for workflow %s in results \"%s\" that was in the excluded list",
                            te,
                            context.getWorkflowName(),
                            directoryName));
                }
            }
        }
    }

    public static TupleEntryIterator openForRead(WorkflowContext context, WorkflowInterface workflow, String directoryName) throws Throwable {
        return workflow.isBinary(directoryName) ? workflow.openBinaryForRead(context, directoryName) : workflow.openTextForRead(context, directoryName);
    }

    public static boolean tupleMatchesTarget(WorkflowInterface workflow, TupleEntry te, Map<String, String> targetValues) {
        return diffTupleAndTarget(workflow, te, targetValues).size() == 0;
    }

    public static Set<TupleDiff> diffTupleAndTarget(WorkflowInterface workflow, TupleEntry te, Map<String, String> targetValues) {
        return diffTupleAndTarget(workflow, te, targetValues, false);
    }

        /**
         * @param workflow     workflow
         * @param te           source
         * @param targetValues target
         * @param reportAdditionalColumns should TupleDiff.ADDITIONAL be reported?
         * @return
         */
    public static Set<TupleDiff> diffTupleAndTarget(WorkflowInterface workflow, TupleEntry te, Map<String, String> targetValues, boolean reportAdditionalColumns) {
        Set<TupleDiff> diffs = new LinkedHashSet<TupleDiff>();
        for (Map.Entry<String, String> entry : targetValues.entrySet()) {
            String fieldName = entry.getKey();
            final TupleDiff diff = workflow.diffTupleAndTarget(te, fieldName, entry.getValue());
            if(diff != null) {
                diffs.add(diff);
            }
        }

        if(reportAdditionalColumns) {
            for (Iterator<?> it = te.getFields().iterator(); it.hasNext(); ) {
                String field = (String) it.next();
                final String actual = te.getString(field);
                if (actual != null && !targetValues.containsKey(field)) {
                    diffs.add(new TupleDiff(field, null, actual, TupleDiff.Type.ADDITIONAL));
                }
            }
        }

        return diffs;
    }

    public static WorkflowPlatform getPlatform(String workflowName, String platformName) {
        platformName = platformName.trim();

        if (platformName.isEmpty()) {
            return WorkflowContext.getContext(workflowName).getDefaultPlatform();
        } else if (platformName.equals("locally") || platformName.equals("local")) {
            return WorkflowPlatform.LOCAL;
        } else if (platformName.equals("on a cluster") || platformName.equals("hdfs")) {
            return WorkflowPlatform.DISTRIBUTED;
        } else {
            throw new IllegalArgumentException(String.format("The workflow platform \"%s\" is unknown", platformName));
        }
    }

}
