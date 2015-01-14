package com.scaleunlimited.cascading.cuke;

import java.io.File;
import java.util.Random;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Debug;
import cascading.operation.DebugLevel;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Rename;
import cascading.pipe.assembly.SumBy;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.local.KryoScheme;

public class LetterCountTool extends BaseTool implements WorkflowInterface {

	// The name we use for the input directory parameter.
	private static final String INPUT_PARAM_NAME = "input";
	
	// The name we use for the output directory parameter, and how Cucumber features refers to the output directory.
	private static final String OUTPUT_PARAM_NAME = "output";
	
	private static final String DEBUG_PARAM_NAME = "debug";

	private static final String WORD_COUNT_RECORD_NAME = "word count";

	private static final String[] RANDOM_WORDS = {
		"Lorem",
        "ipsum",
        "dolor",
        "sit",
        "amet",
        "consectetur",
        "adipiscing",
        "elit",
        "Mauris",
        "convallis",
        "sem",
        "id",
        "nisi",
        "porttitor",
        "auctor",
        "Nulla",
        "tincidunt",
        "eget",
        "eros",
        "commodo",
        "Praesent",
        "eu",
        "efficitur",
        "ligula",
        "vestibulum",
        "metus",
        "et",
        "ante",
        "ac",
        "eleifend",
        "dapibus",
        "in",
        "Etiam",
        "purus",
        "mi",
        "turpis",
        "tellus",
        "at",
        "consequat",
        "ullamcorper",
        "libero",
        "Nam",
        "lacinia",
        "sapien",
        "quis",
        "aliquam",
        "rhoncus",
        "neque",
        "gravida",
        "tortor",
        "vitae",
        "non",
        "primis",
        "faucibus",
        "orci",
        "luctus",
        "ultrices",
        "posuere",
        "cubilia",
        "Curae",
        "massa",
        "ut",
        "Duis",
        "leo",
        "feugiat",
        "elementum",
        "lectus",
        "mattis",
        "maximus",
        "Proin",
        "pretium",
        "interdum",
        "Phasellus",
        "vel",
        "ultricies",
        "lacus",
        "viverra",
        "velit",
        "semper",
        "condimentum",
        "molestie",
        "sollicitudin"
	};
	
    @SuppressWarnings("serial")
	private static class LetterGenerator extends BaseOperation<Void> implements Function<Void> {
    	
    	public LetterGenerator() {
    		super(2, new Fields("letter", "count"));
    	}
    	
    	@SuppressWarnings("rawtypes")
		@Override
    	public void operate(FlowProcess flowProcess, FunctionCall<Void> functionCall) {
    		TupleEntry te = functionCall.getArguments();
    		String word = te.getString("word");
    		int count = te.getInteger("count");
    		
    		Tuple result = new Tuple("", count);
    		for (int i = 0; i < word.length(); i++) {
    			result.setString(0, "" + word.charAt(i));
    			functionCall.getOutputCollector().add(result);
    		}
    	}
    }
    
    @SuppressWarnings("rawtypes")
    @Override
    public Flow createFlow(WorkflowContext context) throws Throwable {
        // TODO avoid using cascading.utils support here.
        // So call local makeInputTextTap(platformName, isText?)
    	WorkflowPlatform platform = context.getDefaultPlatform();
        BasePlatform basePlatform =  makePlatform(platform);

        WorkflowParams params = context.getParamsCopy();
        String inputDir = params.remove(INPUT_PARAM_NAME);
        BasePath inputPath = basePlatform.makePath(inputDir);
        
        if (!inputPath.exists()) {
        	throw new IllegalArgumentException(String.format("Input path %s doesn't exist", inputPath));
        }
        
        Tap source = makeBinarySource(platform, new Fields("word", "count"), inputDir);
        
        String outputDir = params.remove(OUTPUT_PARAM_NAME);
        BasePath outputPath = basePlatform.makePath(outputDir);
        Tap sink = makeBinarySink(platform, new Fields("letter", "count"), outputPath.getAbsolutePath());
        
        String debugStr = params.removeOptional(DEBUG_PARAM_NAME);
        boolean debug = debugStr == null ? false : Boolean.parseBoolean(debugStr);
        
        if (!params.isEmpty()) {
            throw new IllegalArgumentException("Unknown parameter(s): " + params.getNames());
        }
        
        Pipe letterPipe = new Pipe("letters");
        letterPipe = new Each(letterPipe, DebugLevel.VERBOSE, new Debug("words", true));
        letterPipe = new Each(letterPipe, new LetterGenerator());
        letterPipe = new Each(letterPipe, DebugLevel.VERBOSE, new Debug("letters", true));

        letterPipe = new SumBy(letterPipe, new Fields("letter"), new Fields("count"), new Fields("sum"), Integer.class);
        letterPipe = new Rename(letterPipe, new Fields("letter", "sum"), new Fields("letter", "count"));
        
        FlowDef flowDef = new FlowDef()
            .setName("Counting letters")
            .addSource(letterPipe, source)
            .addTailSink(letterPipe, sink)
            .setDebugLevel(debug ? DebugLevel.VERBOSE : DebugLevel.NONE);
        
        FlowConnector flowConnector = basePlatform.makeFlowConnector();
        return flowConnector.connect(flowDef);
    }
    
    @SuppressWarnings("rawtypes")
	private Tap makeBinarySource(WorkflowPlatform platform, Fields fields, String inputDir) {
        if (platform == WorkflowPlatform.DISTRIBUTED) {
            return new Hfs(new cascading.scheme.hadoop.SequenceFile(fields), inputDir);
        } else if (platform == WorkflowPlatform.LOCAL) {
            return new FileTap(new KryoScheme(fields), inputDir);
        } else {
            throw new IllegalArgumentException(String.format("The workflow platform %s is unknown", platform));
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

	@Override
	public TupleEntryCollector openTextForWrite(WorkflowContext context, String path) throws Throwable {
        throw new UnsupportedOperationException(String.format("The LetterCountTool doesn't support creating test input text data"));
	}

    @Override
    public TupleEntryIterator openBinaryForRead(WorkflowContext context, String path) throws Throwable {
    	if (!path.equals(OUTPUT_PARAM_NAME)) {
            throw new IllegalArgumentException(String.format("The binary output directory \"%s\" is unknown, must be \"%s\"", path, OUTPUT_PARAM_NAME));
    	}

    	Fields fields = new Fields("letter", "count");
    	return WorkflowUtils.openBinaryForRead(context.getDefaultPlatform(), convertPath(context, path), fields);
    }

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public TupleEntryCollector openBinaryForWrite(WorkflowContext context, String path, String recordName) throws Throwable {
    	if (!recordName.equals(WORD_COUNT_RECORD_NAME)) {
            throw new IllegalArgumentException(String.format("The record name \"%s\" is unknown", recordName));
    	}

    	path = convertPath(context, path);
    	Fields fields = new Fields("word", "count");
    	WorkflowPlatform platform = context.getDefaultPlatform();
    	
    	// TODO make this a WorkflowUtils.openBinaryForWrite() call.
    	if (platform == WorkflowPlatform.DISTRIBUTED) {
            Tap tap = new Hfs(new cascading.scheme.hadoop.SequenceFile(fields), path, SinkMode.REPLACE);
            return tap.openForWrite(new HadoopFlowProcess());
        } else if (platform == WorkflowPlatform.LOCAL) {
            // We have to make sure the path exists
            File outputFile = new File(path);
            File parentDir = outputFile.getParentFile();
            parentDir.mkdirs();
            Tap tap = new FileTap(new KryoScheme(fields), path, SinkMode.REPLACE);
            return tap.openForWrite(new LocalFlowProcess());
        } else {
            throw new IllegalArgumentException(String.format("The workflow platform %s is unknown", platform));
        }
	}
	
    @Override
    public boolean isBinary(String path) {
        return true;
    }
    
	@Override
	public Tuple createTuple(WorkflowContext context, String recordName, TupleValues tupleValues) throws Throwable {

        if (!recordName.equals(WORD_COUNT_RECORD_NAME)) {
            throw new IllegalArgumentException(String.format("The record name \"%s\" is unknown", recordName));
        }

        // TODO get random from this workflow's context. Which means finding the context by instantiated workflow class
        Random rand = new Random(1L);
        
        Fields fields = new Fields("word", "count");
        TupleEntry result = new TupleEntry(fields, Tuple.size(fields.size()));
        
        if (tupleValues.containsKey("word")) {
            result.setString("word", tupleValues.remove("word"));
        } else {
            // Set to random word
            result.setString("word", RANDOM_WORDS[rand.nextInt(RANDOM_WORDS.length)]);
        }
        
        if (tupleValues.containsKey("count")) {
            result.setInteger("count", Integer.parseInt(tupleValues.remove("count")));
        } else {
            // Set to random count
            result.setInteger("count", 1 + rand.nextInt(1000));
        }

        if (!tupleValues.isEmpty()) {
            throw new IllegalArgumentException(String.format("Record \"%s\" does not have fields named \"%s\"", recordName, tupleValues.keySet().toString()));
        }
        
        return result.getTuple();
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

}
