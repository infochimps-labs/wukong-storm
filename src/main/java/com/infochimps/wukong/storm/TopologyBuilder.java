package com.infochimps.wukong.storm;

import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;

import org.apache.log4j.Logger;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.spout.IOpaquePartitionedTridentSpout;

import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.KafkaConfig;
import storm.kafka.trident.TridentKafkaConfig;
import storm.kafka.StringScheme;
import backtype.storm.spout.SchemeAsMultiScheme;

import com.infochimps.storm.trident.KafkaState;
import com.infochimps.storm.wukong.WuFunction;
import com.infochimps.storm.trident.spout.OpaqueTransactionalBlobSpout;
import com.infochimps.storm.trident.spout.WukongRecordizer;
import com.infochimps.storm.trident.spout.IBlobStore;
import com.infochimps.storm.trident.spout.S3BlobStore;
import com.infochimps.storm.trident.spout.FileBlobStore;

public class TopologyBuilder {
    
    static Logger LOG = Logger.getLogger(TopologyBuilder.class);

    public TopologyBuilder() {
    }

    public StormTopology topology() {
	logTopologyInfo();
	TridentTopology top = new TridentTopology();

	Stream input = top.newStream(topologyName(), spout())
	    .parallelismHint(inputParallelism());

	Stream scaledInput;
	if (dataflowParallelism() > inputParallelism()) {
	    scaledInput = input.shuffle();
	} else {
	    scaledInput = input;
	}

	Stream wukongOutput = scaledInput.each(new Fields(spoutTupleName()), dataflow(), new Fields("_wukong"))
	    .parallelismHint(dataflowParallelism());

	wukongOutput.partitionPersist(state(), new Fields("_wukong"), new KafkaState.Updater());
	
	return top.build();
    }

    public KafkaState.Factory state() {
	return new KafkaState.Factory(outputTopic(), zookeeperHosts());
    }
    
    public IOpaquePartitionedTridentSpout spout() {
	if (spoutType().equals(BLOB_SPOUT_TYPE)) {
	    return new OpaqueTransactionalBlobSpout(blobStore(), new WukongRecordizer());
	} else {
	    return new OpaqueTridentKafkaSpout(kafkaSpoutConfig());
	}
    }

    private String spoutTupleName() {
	if (spoutType().equals(BLOB_SPOUT_TYPE)) {
	    return "content";
	} else { 
	    return "str";	// kafka spout
	}
    }

    private IBlobStore blobStore() {
	if (blobStoreType().equals(S3_BLOB_TYPE)) {
	    return new S3BlobStore(blobStorePath(), s3Bucket(), awsKey(), awsSecret());
	} else {
	    return new FileBlobStore(blobStorePath());
	}
    }

    private TridentKafkaConfig kafkaSpoutConfig() {
	TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(KafkaConfig.StaticHosts.fromHostString(kafkaHosts(), inputPartitions()), kafkaInputTopic());
	kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	kafkaConfig.fetchSizeBytes = inputBatchSize();
	kafkaConfig.forceStartOffsetTime(inputOffset());
	return kafkaConfig;
    }

    private WuFunction dataflow() {
	return new WuFunction(dataflowName(), subprocessDirectory(), dataflowEnv());
    }

    public Boolean valid() {
	if (topologyName() == null) {
	    LOG.error("Must set a topology name using the " + TOPOLOGY_NAME + " property");
	    return false;
	}
	if (dataflowName() == null) {
	    LOG.error("Must set a dataflow name using the " + DATAFLOW_NAME + " property");
	    return false;
	};
	if (spoutType().equals(KAFKA_SPOUT_TYPE)) {
	    if (kafkaInputTopic() == null) {
		LOG.error("Must set an input topic name using the " + KAFKA_INPUT_TOPIC + " property when using a Kafka spout");
		return false;
	    };    
	}
	if (spoutType().equals(BLOB_SPOUT_TYPE)) {
	    if (blobStorePath() == null) {
		LOG.error("Must set a path using the " + BLOB_STORE_PATH + " property");
		return false;
	    };
	    if (blobStoreType().equals(S3_BLOB_TYPE)) {
		if (s3Bucket() == null) {
		    LOG.error("Must set an S3 bucket using the " + S3_BUCKET + " property");
		    return false;
		};
		if (awsKey() == null) {
		    LOG.error("Must set an AWS access key using the " + AWS_KEY + " property");
		    return false;
		};
		if (awsSecret() == null) {
		    LOG.error("Must set an AWS secret key using the " + AWS_SECRET + " property");
		    return false;
		};
	    }
	}
	
	if (outputTopic() == null) {
	    LOG.error("Must set a Kafka output topic using the " + OUTPUT_TOPIC + " property");
	    return false;
	};
	return true;
    }

    public static String usageArgs() {
	String s = "\n"
	    + "Dynamically assemble and launch a parametrized Storm topology that\n"
	    + "embeds a Wukong dataflow.  The current overall \"shape\" of the\n"
	    + "topology is\n"
	    + "\n"
	    + "  spout -> wukong dataflow -> state\n"
	    + "\n"
	    + "The available spouts read from Kafka or S3.  The only available state\n"
	    + "is Kafka.\n"
	    + "\n"
	    + "ENVIRONMENT OPTIONS\n"
	    + "\n"
	    + "  " + KAFKA_HOSTS + "		Comma-separated list of Kafka host (and optional port) pairs  (Default: " + DEFAULT_KAFKA_HOSTS + ")\n"
	    + "  " + ZOOKEEPER_HOSTS + "	Comma-separated list of Zookeeper host (and optional port) pairs  (Default: " + DEFAULT_ZOOKEEPER_HOSTS + ")\n"
	    + "\n"
	    + "TOPOLOGY OPTIONS\n"
	    + "\n"
	    + "  " + TOPOLOGY_NAME + "	Name of the Storm topology that will be launched  (Required)\n"
	    + "\n"
	    + "SPOUT OPTIONS\n"
	    + "\n"
	    + "The following options apply to all spouts.\n"
	    + "\n"
	    + "  " + INPUT_PARALLELISM + "	Parallelism hint for the spout (Default: " + DEFAULT_INPUT_PARALLELISM + ")\n"
	    + "\n"
	    + "There are two spout types distinguished by the " + SPOUT_TYPE + " property.\n"
	    + "\n"
	    + "The following options apply for the '" + KAFKA_SPOUT_TYPE + "' spout type:\n"
	    + "\n"
	    + "  " + INPUT_OFFSET + "		Offset from which to start consuming from the input topic.  (-1 = 'current', -2 = 'beginning', default: " + DEFAULT_INPUT_OFFSET + ")\n"
	    + "  " + INPUT_PARTITIONS + "	Number of Storm partitions to use.  Should match the number of partitions on the input topic. (Default: " + DEFAULT_INPUT_PARTITIONS + ")\n"
	    + "  " + INPUT_BATCH + "		Batch size to fetch from Kafka (Default: " + DEFAULT_INPUT_BATCH + ")\n"
	    + "\n"
	    + "The following options apply for the '" + FILE_BLOB_TYPE + "' spout type:\n"
	    + "\n"
	    + "  " + BLOB_STORE_PATH + "	Filesystem directory to read from (Required)\n"
	    + "\n"
	    + "The following options apply for the '" + S3_BLOB_TYPE + "' spout type:\n"
	    + "\n"
	    + "  " + S3_BUCKET + "	S3 bucket (Required)\n"
	    + "  " + BLOB_STORE_PATH + "	Directory within bucket to read from (Required)\n"
	    + "  " + AWS_KEY + "	AWS access key (Required)\n"
	    + "  " + AWS_SECRET + "	AWS secret key (Required)\n"
	    + "\n"
	    + "DATAFLOW OPTIONS\n"
	    + "\n"
	    + "  " + DATAFLOW_NAME + "	Name of the Wukong dataflow to launch within Storm (Required)\n"
	    + "  " + DATAFLOW_ENV + "	Wukong environment (Default: " + DEFAULT_DATAFLOW_ENV + ")\n"
	    + "  " + BOLT_COMMAND + "	The command-line to execute within a Storm bolt (Required)\n"
	    + "  " + DATAFLOW_DIRECTORY + "	The directory within which to execute the command-line (Default: " + DEFAULT_DATAFLOW_DIRECTORY + ")\n"
	    + "  " + DATAFLOW_PARALLELISM + "	Parallelism hint for Wukong dataflow Trident function (Default: " + DEFAULT_INPUT_PARALLELISM + ")\n"
	    + "\n"
	    + "\n"
	    + "STATE OPTIONS\n"
	    + "\n"
	    + "The only implemented state is Kafka.\n"
	    + "\n"
	    + "  " + OUTPUT_TOPIC + "	The Kafka output topic\n"
	    + "\n";
	return s;
    }

    private void logTopologyInfo() {
	logSpoutInfo();
	logDataflowInfo();
	logStateInfo();
    }
    
    private void logSpoutInfo() {
	LOG.info("SPOUT: Reading from offset " + inputOffset() + " of Kafka topic <" + kafkaInputTopic() + "> in batches of " + inputBatchSize() + " with parallelism " + inputParallelism());
    }

    private void logDataflowInfo() {
	LOG.info("WUKONG: Launching dataflow <" + dataflowName() + "> with parallelism " + dataflowParallelism() + " in environment <" + dataflowEnv() + ">" );
    }

    private void logStateInfo() {
	LOG.info("STATE: Writing to Kafka topic <" + outputTopic() + ">");
    }
    
    private String prop(String key, String defaultValue) {
	if (System.getProperty(key) == null) {
	    System.setProperty(key, defaultValue);
	}
	return prop(key);
    }

    private String prop(String key) {
	return System.getProperty(key);
    }
    
    public static String KAFKA_HOSTS			= "wukong.kafka.hosts";
    public static String DEFAULT_KAFKA_HOSTS		= "localhost";
    public List<String> kafkaHosts() {
	ArrayList<String> kh = new ArrayList();
	for (String host : prop(KAFKA_HOSTS, DEFAULT_KAFKA_HOSTS).split(",")) {
	    kh.add(host);
	}
	return kh;
    }
    
    public static String ZOOKEEPER_HOSTS		= "wukong.zookeeper.hosts";
    public static String DEFAULT_ZOOKEEPER_HOSTS	= "localhost";
    public String zookeeperHosts() {
	return prop(ZOOKEEPER_HOSTS, DEFAULT_ZOOKEEPER_HOSTS);
    }
    
    public static String TOPOLOGY_NAME                  = "wukong.topology";
    public String topologyName() {
	return prop(TOPOLOGY_NAME);
    }
    
    public static String DATAFLOW_DIRECTORY             = "wukong.directory";
    public static String DEFAULT_DATAFLOW_DIRECTORY     = System.getProperty("user.dir");
    public String subprocessDirectory() {
	return prop(DATAFLOW_DIRECTORY, DEFAULT_DATAFLOW_DIRECTORY);
    }

    public static String DATAFLOW_NAME			= "wukong.dataflow";
    public String dataflowName() {
	return prop(DATAFLOW_NAME);
    }

    // This is actually used directly by WuFunction but it's listed
    // here for completeness since it is set by the Ruby code.
    public static String BOLT_COMMAND                   = "wukong.command";

    public static String DATAFLOW_ENV			= "wukong.environment";
    public static String DEFAULT_DATAFLOW_ENV	        = "development";
    public String dataflowEnv() {
	return prop(DATAFLOW_ENV, DEFAULT_DATAFLOW_ENV);
    }
    
    public static String DATAFLOW_PARALLELISM		= "wukong.parallelism";
    public int dataflowParallelism() {
	return Integer.parseInt(prop(DATAFLOW_PARALLELISM, Integer.toString(inputParallelism())));
    }

    public static String SPOUT_TYPE                     = "wukong.input.type";
    public static String KAFKA_SPOUT_TYPE               = "kafka";
    public static String BLOB_SPOUT_TYPE                = "blob";
    public String spoutType() {
	if ((prop(SPOUT_TYPE) != null) && prop(SPOUT_TYPE).equals(BLOB_SPOUT_TYPE)) {
	    return BLOB_SPOUT_TYPE;
	} else {
	    return KAFKA_SPOUT_TYPE;
	}
    }

    public static String BLOB_STORE_PATH                = "wukong.input.blob.path";
    public String blobStorePath() {
	return prop(BLOB_STORE_PATH);
    }

    public static String BLOB_TYPE                      = "wukong.input.blob.type";
    public static String FILE_BLOB_TYPE                 = "file";
    public static String S3_BLOB_TYPE                   = "s3";
    public String blobStoreType() {
	if ((prop(BLOB_TYPE) != null) && prop(BLOB_TYPE).equals(S3_BLOB_TYPE)){
	    return S3_BLOB_TYPE;
	} else {
	    return FILE_BLOB_TYPE;
	}
    }

    public static String S3_BUCKET                      = "wukong.input.blob.s3_bucket";
    public String s3Bucket() {
	return prop(S3_BUCKET);
    }

    public static String AWS_KEY                        = "wukong.input.blob.aws_key";
    public String awsKey() {
	return prop(AWS_KEY);
    }
    
    public static String AWS_SECRET                     = "wukong.input.blob.aws_secret";
    public String awsSecret() {
	return prop(AWS_SECRET);
    }

    public static String KAFKA_INPUT_TOPIC        	= "wukong.input.kafka.topic";
    public String kafkaInputTopic() {
	return prop(KAFKA_INPUT_TOPIC);
    }
    
    public static String INPUT_OFFSET			= "wukong.input.offset";
    public static String DEFAULT_INPUT_OFFSET		= "-1";
    public int inputOffset() {
	return Integer.parseInt(prop(INPUT_OFFSET, DEFAULT_INPUT_OFFSET));
    }
    
    public static String INPUT_PARTITIONS		= "wukong.input.partitions";
    public static String DEFAULT_INPUT_PARTITIONS	= "1";
    public int inputPartitions() {
	return Integer.parseInt(prop(INPUT_PARTITIONS, DEFAULT_INPUT_PARTITIONS));
    }
    
    public static String INPUT_BATCH			= "wukong.input.batch";
    public static String DEFAULT_INPUT_BATCH		= "1048576";
    public int inputBatchSize() {
	return Integer.parseInt(prop(INPUT_BATCH, DEFAULT_INPUT_BATCH));
    }
    
    public static String INPUT_PARALLELISM		= "wukong.input.parallelism";
    public static String DEFAULT_INPUT_PARALLELISM	= "1";
    public int inputParallelism() {
	return Integer.parseInt(prop(INPUT_PARALLELISM, DEFAULT_INPUT_PARALLELISM));
    }
    
    public static String OUTPUT_TOPIC			= "wukong.output.kafka.topic";
    public String outputTopic() {
	return prop(OUTPUT_TOPIC);
    }
    
}
