package com.infochimps.wukong.storm;

import java.lang.IllegalArgumentException;

import org.apache.log4j.Logger;

import backtype.storm.spout.SchemeAsMultiScheme;

import storm.trident.spout.IOpaquePartitionedTridentSpout;

import storm.kafka.KafkaConfig;
import storm.kafka.StringScheme;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;

import com.infochimps.storm.trident.spout.OpaqueTransactionalBlobSpout;
import com.infochimps.storm.trident.spout.StartPolicy;
import com.infochimps.storm.trident.spout.WukongRecordizer;
import com.infochimps.storm.trident.spout.IBlobStore;
import com.infochimps.storm.trident.spout.S3BlobStore;
import com.infochimps.storm.trident.spout.FileBlobStore;

public class SpoutBuilder extends Builder {

    static Logger LOG = Logger.getLogger(SpoutBuilder.class);

    @Override
    public Boolean valid() {
	if (spoutType().equals(KAFKA_SPOUT_TYPE)) {
	    if (kafkaInputTopic() == null) {
		LOG.error("Must set an input topic name using the " + KAFKA_INPUT_TOPIC + " property when using a Kafka spout");
		return false;
	    };    
	}
	if (spoutType().equals(BLOB_SPOUT_TYPE)) {
	    if (blobStorePath() == null) {
		LOG.error("Must set a path using the " + BLOB_STORE_PATH + " property when using a blob store spout");
		return false;
	    };
	    if (blobStoreType().equals(S3_BLOB_TYPE)) {
		if (s3Bucket() == null) {
		    LOG.error("Must set an S3 bucket using the " + S3_BUCKET + " property when using the S3 spout");
		    return false;
		};
		if (awsKey() == null) {
		    LOG.error("Must set an AWS access key using the " + AWS_KEY + " property when using the S3 spout");
		    return false;
		};
		if (awsSecret() == null) {
		    LOG.error("Must set an AWS secret key using the " + AWS_SECRET + " property when using the S3 spout");
		    return false;
		};
	    }
	}
	return true;
    }

    @Override
    public void logInfo() {
	if (spoutType().equals(BLOB_SPOUT_TYPE)) {
	    if (blobStoreType().equals(S3_BLOB_TYPE)) {
		LOG.info("SPOUT: Reading from S3 bucket s3://" + s3Bucket() + " at path /" + blobStorePath() + ", using AWS key " + awsKey());
	    } else {
		LOG.info("SPOUT: Reading from local file file:///" + blobStorePath());
	    }
	} else {
	    LOG.info("SPOUT: Reading from offset " + kafkaInputOffset() + " of Kafka topic <" + kafkaInputTopic() + "> in batches of " + kafkaInputBatchSize() + " with parallelism " + inputParallelism());
	}
    }

    public static String usage() {
	String s = "SPOUT OPTIONS\n"
	    + "\n"
	    + "Choose the spout with he following properties.  Each spout has its own further\n"
	    + "configuration\n"
	    + "\n"
            + "  Kafka Spout -- " + SPOUT_TYPE + "=" + KAFKA_SPOUT_TYPE + "\n"
	    + "  BlobStore Spout -- " + SPOUT_TYPE + "=" + BLOB_TYPE + "\n"
	    + "    Filesystem Spout -- " + BLOB_SPOUT_TYPE + "=" + FILE_BLOB_TYPE + "\n"
	    + "    S3 Spout -- " + BLOB_SPOUT_TYPE + "=" + S3_BLOB_TYPE + "\n"
	    + "\n"
	    + "The following options apply for the Kafka spout (" + SPOUT_TYPE + "=" + KAFKA_SPOUT_TYPE + "):\n"
	    + "\n"
	    + "  " + String.format("%10s", INPUT_PARALLELISM) + "	Parallelism hint for the spout (Default: " + DEFAULT_INPUT_PARALLELISM + ")\n"
	    + "  " + String.format("%10s", KAFKA_INPUT_TOPIC) + "  Name of the Kafka topic to read input from"
	    + "  " + String.format("%10s", KAFKA_INPUT_OFFSET) + "  Offset from which to start consuming from the input topic, one of: -1 = 'end', -2 = 'beginning', or an explicit byte offset.  (Default: resume if possible, else '1')\n"
	    + "  " + String.format("%10s", KAFKA_INPUT_PARTITIONS) + "  Number of Storm partitions to use.  Should match the number of partitions on the input topic. (Default: " + DEFAULT_KAFKA_INPUT_PARTITIONS + ")\n"
	    + "  " + String.format("%10s", KAFKA_INPUT_BATCH) + "  Batch size to fetch from Kafka (Default: " + DEFAULT_KAFKA_INPUT_BATCH + ")\n"
	    + "\n"
	    + "The following options apply for all BlobStore spouts (" + SPOUT_TYPE + "=" + BLOB_TYPE + "):\n"
	    + "\n"
	    + "  " + String.format("%10s", BLOB_STORE_PATH) + "  Directory to read from (Required)\n"
	    + "  " + String.format("%10s", BLOB_START) + "  Starting policy, one of: EARLIEST, LATEST, EXPLICIT, or RESUME.  (Default: 'RESUME' if possible, else 'LATEST')\n"
	    + "  " + String.format("%10s", BLOB_MARKER) + "  Required name of marker for an EXPLICIT starting policy\n"
	    + "\n"
	    + "The following options apply for the S3 spout (" + BLOB_SPOUT_TYPE + "=" + S3_BLOB_TYPE + "):\n"
	    + "\n"
	    + "  " + String.format("%10s", S3_BUCKET) + "  S3 bucket (Required)\n"
	    + "  " + String.format("%10s", AWS_KEY) + "  AWS access key (Required)\n"
	    + "  " + String.format("%10s", AWS_SECRET) + "  AWS secret key (Required)\n";
	return s;
    }
    
    public IOpaquePartitionedTridentSpout spout() {
	if (spoutType().equals(BLOB_SPOUT_TYPE)) {
	    return new OpaqueTransactionalBlobSpout(blobStore(), new WukongRecordizer(), blobStart(), blobMarker());
	} else {
	    return new OpaqueTridentKafkaSpout(kafkaSpoutConfig());
	}
    }

    private IBlobStore blobStore() {
	if (blobStoreType().equals(S3_BLOB_TYPE)) {
	    return new S3BlobStore(blobStorePath(), s3Bucket(), s3Endpoint(), awsKey(), awsSecret());
	} else {
	    return new FileBlobStore(blobStorePath());
	}
    }

    private TridentKafkaConfig kafkaSpoutConfig() {
	TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(KafkaConfig.StaticHosts.fromHostString(kafkaHosts(), kafkaInputPartitions()), kafkaInputTopic());
	kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
	kafkaConfig.fetchSizeBytes = kafkaInputBatchSize();
	kafkaConfig.forceStartOffsetTime(kafkaInputOffset());
	return kafkaConfig;
    }
    
    public static String INPUT_PARALLELISM		= "wukong.input.parallelism";
    public static String DEFAULT_INPUT_PARALLELISM	= "1";
    public int inputParallelism() {
	return Integer.parseInt(prop(INPUT_PARALLELISM, DEFAULT_INPUT_PARALLELISM));
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

    public Boolean isBlobSpout() {
	return spoutType().equals(BLOB_SPOUT_TYPE);
    }

    public Boolean isKafkaSpout() {
	return spoutType().equals(KAFKA_SPOUT_TYPE);
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

    public Boolean isS3Spout() {
	return (isBlobSpout() && blobStoreType().equals(S3_BLOB_TYPE));
    }

    public Boolean isFileSpout() {
	return (isBlobSpout() && blobStoreType().equals(FILE_BLOB_TYPE));
    }
	    
    public static String BLOB_START         = "wukong.input.blob.start";
    public static String DEFAULT_BLOB_START = "RESUME";
    public StartPolicy blobStart() {
	try {
	    return StartPolicy.valueOf(prop(BLOB_START, DEFAULT_BLOB_START));
	} catch (IllegalArgumentException e) {
	    return StartPolicy.RESUME;
	}
    }

    public static String BLOB_MARKER = "wukong.input.blob.marker";
    public String blobMarker() {
	return prop(BLOB_MARKER);
    }

    public static String S3_BUCKET                      = "wukong.input.blob.s3_bucket";
    public String s3Bucket() {
	return prop(S3_BUCKET);
    }

    public static String S3_ENDPOINT                    = "wukong.input.blob.s3_endpoint";
    public static String DEFAULT_S3_ENDPOINT            = "s3.amazonaws.com";
    public String s3Endpoint() {
	return prop(S3_ENDPOINT, DEFAULT_S3_ENDPOINT);
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
    
    public static String KAFKA_INPUT_OFFSET		= "wukong.input.kafka.offset";
    public static String DEFAULT_KAFKA_INPUT_OFFSET     = "-1";
    public Integer kafkaInputOffset() {
	return Integer.parseInt(prop(KAFKA_INPUT_OFFSET, DEFAULT_KAFKA_INPUT_OFFSET));
    }
    
    public static String KAFKA_INPUT_PARTITIONS		= "wukong.input.kafka.partitions";
    public static String DEFAULT_KAFKA_INPUT_PARTITIONS	= "1";
    public int kafkaInputPartitions() {
	return Integer.parseInt(prop(KAFKA_INPUT_PARTITIONS, DEFAULT_KAFKA_INPUT_PARTITIONS));
    }
    
    public static String KAFKA_INPUT_BATCH			= "wukong.input.kafka.batch";
    public static String DEFAULT_KAFKA_INPUT_BATCH		= "1048576";
    public int kafkaInputBatchSize() {
	return Integer.parseInt(prop(KAFKA_INPUT_BATCH, DEFAULT_KAFKA_INPUT_BATCH));
    }
    
    
}
