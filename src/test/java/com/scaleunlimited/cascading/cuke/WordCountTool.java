package com.scaleunlimited.cascading.cuke;

import java.io.File;
import java.util.Map;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
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
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.local.KryoScheme;

public class WordCountTool extends BaseTool implements WorkflowInterface {

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
    public Flow createFlow(WorkflowContext context) throws Throwable {
        // TODO avoid using cascading.utils support here.
        // So call local makeInputTextTap(platformName, isText?)
        BasePlatform basePlatform =  makePlatform(context.getDefaultPlatform());

        WorkflowParams params = context.getParamsCopy();
        
        String inputDir = params.remove(INPUT_PARAM_NAME);
        BasePath inputPath = basePlatform.makePath(inputDir);
        if (!inputPath.exists()) {
        	throw new IllegalArgumentException(String.format("Input path %s doesn't exist", inputPath));
        }
        
        Tap source = basePlatform.makeTap(basePlatform.makeTextScheme(), inputPath);
        
        String outputDir = params.remove(OUTPUT_PARAM_NAME);
        BasePath outputPath = basePlatform.makePath(outputDir);
        Tap sink = makeBinarySink(context.getDefaultPlatform(), new Fields("word", "count"), outputPath.getAbsolutePath());
        
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
        
        FlowConnector flowConnector = basePlatform.makeFlowConnector();
        return flowConnector.connect(flowDef);
    }
    
    @Override
    public TupleEntryIterator openBinaryForRead(WorkflowContext context, String path) throws Throwable {
    	if (!path.equals(OUTPUT_PARAM_NAME)) {
            throw new IllegalArgumentException(String.format("The binary output directory \"%s\" is unknown, must be \"%s\"", path, OUTPUT_PARAM_NAME));
    	}
    	
    	Fields fields = new Fields("word", "count");
    	return WorkflowUtils.openBinaryForRead(context.getDefaultPlatform(), convertPath(context, path), fields);
    }

	@Override
	public TupleEntryCollector openTextForWrite(WorkflowContext context, String path) throws Throwable {
		return WorkflowUtils.openTextForWrite(context.getDefaultPlatform(), convertPath(context, path));
	}

	@Override
	public TupleEntryCollector openBinaryForWrite(WorkflowContext context, String path, String recordName) throws Throwable {
        throw new UnsupportedOperationException(String.format("The WordCountTool doesn't support creating test output data"));
	}
	
	@Override
	public Tuple createTuple(WorkflowContext context, String recordName, Map<String, String> tupleValues) throws Throwable {
        throw new UnsupportedOperationException(String.format("The WordCountTool doesn't support creating test output data"));
	}
	
    private String convertPath(WorkflowContext context, String path) {
    	if (path.equals(INPUT_PARAM_NAME)) {
    		return context.getParams().get(INPUT_PARAM_NAME);
    	} if (path.equals(OUTPUT_PARAM_NAME)) {
    		return context.getParams().get(OUTPUT_PARAM_NAME);
    	} else {
    		return path;
    	}
    }
    
    @SuppressWarnings("rawtypes")
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

}
