package com.infochimps.wukong.storm;

import java.util.Map;
import java.util.Iterator;
import java.io.IOException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import backtype.storm.tuple.Values;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TopicExtractorFunction extends BaseFunction {
    
    private static Logger LOG = Logger.getLogger(TopicExtractorFunction.class);

    private String      defaultTopic;
    private String      topicField;
    private JsonFactory parserFactory;
    
    public TopicExtractorFunction(String defaultTopic, String topicField) {
	this.defaultTopic = defaultTopic;
	this.topicField   = topicField;
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
	parserFactory = new JsonFactory();
	LOG.info("Default topic <" + defaultTopic + "> overridden by <" + topicField + ">-field, if defined");
    }

    @Override
    public void cleanup() {
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
	collector.emit(new Values(topicFor(tuple)));
    }

    public String topicFor(TridentTuple tuple) {
	if (tuple == null) { return defaultTopic; }
	String jsonContent = tuple.getString(0);
	if (jsonContent == null) {
	    return defaultTopic;
	} else {
	    try {
		JsonParser parser = parserFactory.createJsonParser(jsonContent);
		while (parser.nextToken() != JsonToken.END_OBJECT) {
		    String fieldName = parser.getCurrentName();
		    if (fieldName == null) { continue; }
		    if (fieldName.equals(topicField)) {
			parser.nextToken();
			String topic = parser.getText();
			return topic;
		    }
		}
		return defaultTopic;
	    } catch (IOException e) {
		return defaultTopic;
	    }
	}
    }
}
