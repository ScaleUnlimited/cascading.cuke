package com.scaleunlimited.cascading.cuke;

import java.io.IOException;

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

import com.scaleunlimited.cascading.local.KryoScheme;

public class WorkflowUtils {

	@SuppressWarnings({ "rawtypes", "unchecked" })
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
                throw new IllegalArgumentException(String.format("The Hadoop path \"%s\" doesn't exist", readPath));
            }
            return tap.openForRead(new HadoopFlowProcess());
        } else if (platform == WorkflowPlatform.LOCAL) {
            Tap tap = new FileTap(new KryoScheme(fields), readPath);
            if (!tap.resourceExists(new LocalFlowProcess())) {
                throw new IllegalArgumentException(String.format("The local path \"%s\" doesn't exist", readPath));
            }
            return tap.openForRead(new LocalFlowProcess());
        } else {
            throw new IllegalArgumentException(String.format("The workflow platform %s is unknown", platform));
        }
	}

    public static TupleMatchFailure tupleFieldMatchesTarget(TupleEntry te, String fieldName, String expected) {
        String tupleValue = te.getString(fieldName);
        if ((tupleValue == null) && !expected.equals("null")) {
            return TupleMatchFailure.MISSING;
        } else if ((tupleValue != null) && expected.equals("null")) {
            return TupleMatchFailure.NULL_EXPECTED;
        } else if ((tupleValue != null) && !tupleValue.equals(expected)) {
            return TupleMatchFailure.NOT_EQUAL;
        }
        return null;
    }

}
