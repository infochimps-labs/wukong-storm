package com.infochimps.wukong.storm;

import org.apache.log4j.Logger;
import com.infochimps.storm.trident.KafkaState;

public class StateBuilder extends Builder {

    static Logger LOG = Logger.getLogger(StateBuilder.class);

    public KafkaState.Factory state() {
	return new KafkaState.Factory(kafkaOutputTopic(), zookeeperHosts());
    }

    public KafkaState.Updater updater() {
	return new KafkaState.Updater();
    }

    @Override
    public Boolean valid() {
	if (kafkaOutputTopic() == null) {
	    LOG.error("Must set a Kafka output topic using the " + KAFKA_OUTPUT_TOPIC + "property");
	    return false;
	}
	return true;
    }

    @Override
    public void logInfo() {
	LOG.info("STATE: Writing to Kafka topic <" + kafkaOutputTopic() + ">");
    }

    public static String usage() {
	String s = "STATE OPTIONS\n"
	    + "\n"
	    + "The only available state is Kafka which has the following options:\n"
	    + "\n"
	    + "  " + String.format("%10s", KAFKA_OUTPUT_TOPIC) + "  The Kafka output topic (Required)\n";
	return s;
    }
    
    public static String KAFKA_OUTPUT_TOPIC = "wukong.output.kafka.topic";
    public String kafkaOutputTopic() {
	return prop(KAFKA_OUTPUT_TOPIC);
    }
    
}
