package com.scaleunlimited.cascading.cuke;

import java.io.File;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.regex.RegexGenerator;
import cascading.operation.state.Counter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.hadoop.HadoopPlatform;
import com.scaleunlimited.cascading.local.KryoScheme;
import com.scaleunlimited.cascading.local.LocalPlatform;

public class WordCountTool implements WorkflowInterface {

	private enum WordCountCounters {
		VALID_LINES,
		INVALID_LINES,
		WORDS
	}
	
	// The name we use for the input directory parameter.
	private static final String INPUT_PARAM_NAME = "input";
	
	// The name we use for the output directory parameter, and how Cucumber features refers to the output directory.
	private static final String OUTPUT_PARAM_NAME = "output";
	
	// Pattern used by RegexGenerator function to turn the input text into words
    private static final String WORD_BREAK_PATTERN_STRING = "(\\b\\w+\\b)";

    @SuppressWarnings("rawtypes")
    @Override
    public Flow createFlow(WorkflowPlatform platformName, WorkflowParams params) throws Throwable {
        // TODO avoid using cascading.utils support here.
        // So call local makeInputTextTap(platformName, isText?)
        BasePlatform platform =  makePlatform(platformName);

        String inputDir = params.remove(INPUT_PARAM_NAME);
        BasePath inputPath = platform.makePath(inputDir);
        if (!inputPath.exists()) {
        	throw new IllegalArgumentException(String.format("Input path %s doesn't exist", inputPath));
        }
        
        Tap source = platform.makeTap(platform.makeTextScheme(), inputPath);
        
        String outputDir = params.remove(OUTPUT_PARAM_NAME);
        Tap sink = makeBinarySink(platformName, new Fields("word", "count"), outputDir);
        
        String minCountStr = params.removeOptional("mincount");
        int minCount = (minCountStr == null ? 1 : Integer.parseInt(minCountStr));
        
        if (!params.isEmpty()) {
            throw new IllegalArgumentException("Unknown parameter(s): " + params.getNames());
        }
        
        Pipe wordPipe = new Pipe("words");
        wordPipe = new Each(wordPipe, new Counter(WordCountCounters.VALID_LINES));
        
        wordPipe = new Each(wordPipe, 
                new Fields("line"), 
                new RegexGenerator( new Fields("word"),
                        WORD_BREAK_PATTERN_STRING));
        wordPipe = new Each(wordPipe, new Counter(WordCountCounters.WORDS));
        wordPipe = new CountBy(wordPipe, new Fields("word"), new Fields("count"));
        wordPipe = new Each(wordPipe, new Fields("count"), new ExpressionFilter(String.format("$0 < %d", minCount), Integer.class));
        
        FlowDef flowDef = new FlowDef()
            .setName("Counting words")
            .addSource(wordPipe, source)
            .addTailSink(wordPipe, sink);
        
        FlowConnector flowConnector = platform.makeFlowConnector();
        return flowConnector.connect(flowDef);
    }
    
    private Tap makeBinarySink(WorkflowPlatform platform, Fields fields, String outputDir) {
        if (platform == WorkflowPlatform.DISTRIBUTED) {
            return new Hfs(new cascading.scheme.hadoop.SequenceFile(fields), outputDir, SinkMode.REPLACE);
        } else if (platform == WorkflowPlatform.LOCAL) {
            // We have to make sure the path exists
            File outputFile = new File(outputDir);
            File parentDir = outputFile.getParentFile();
            parentDir.mkdirs();
            return new FileTap(new KryoScheme(fields), outputDir, SinkMode.REPLACE);
        } else {
            throw new IllegalArgumentException(String.format("The workflow platform %s is unknown", platform));
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public TupleEntryIterator openBinaryForRead(WorkflowPlatform platform, WorkflowParams params, String resultsName) throws Throwable {
    	if (!resultsName.equals(OUTPUT_PARAM_NAME)) {
            throw new IllegalArgumentException(String.format("The output result \"%s\" is unknown", resultsName));
    	}
    	
    	String outputPath = params.get(OUTPUT_PARAM_NAME);
    	Fields fields = new Fields("word", "count");
        if (platform == WorkflowPlatform.DISTRIBUTED) {
            Tap tap = new Hfs(new cascading.scheme.hadoop.SequenceFile(fields), outputPath);
            return tap.openForRead(new HadoopFlowProcess());
        } else if (platform == WorkflowPlatform.LOCAL) {
            Tap tap = new FileTap(new KryoScheme(fields), outputPath);
            return tap.openForRead(new LocalFlowProcess());
        } else {
            throw new IllegalArgumentException(String.format("The workflow platform %s is unknown", platform));
        }
    }

    private BasePlatform makePlatform(WorkflowPlatform platform) {
        if (platform == WorkflowPlatform.DISTRIBUTED) {
            return new HadoopPlatform(this.getClass());
        } else if (platform == WorkflowPlatform.LOCAL) {
            return new LocalPlatform(this.getClass());
        } else {
            throw new IllegalArgumentException("Unknown platform: " + platform);
        }
    }

	@Override
	public TupleEntryCollector openBinaryForWrite(WorkflowPlatform platformName, String resultsName) throws Throwable {
		// TODO use makeBinaryTap call, that's also used by createFlow call
		return null;
	}

}
