public int main(String[] argv) {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("source", new TestWordSpout(true), 5);
    builder.setBolt("dataflow", new Dataflow(), 3)
	.fieldsGrouping("1", new Fields("word"))
	.fieldsGrouping("2", new Fields("word"));
    builder.setBolt("4", new TestGlobalCount())
	.globalGrouping("1");

    Map conf = new HashMap();
    conf.put(Config.TOPOLOGY_WORKERS, 4);
 
    StormSubmitter.submitTopology("mytopology", conf, builder.createTopology());
     
}


public class DataflowTopology {

    private static StormTopology buildTopology(String deployPackDir, String environment, String dataflowName, String inputTopic, String outputTopic) {
	TridentTopology topology = new TridentTopology();
	topology.newStream(dataflowName, buildSpout(topicName)
    }

    private OpaqueTridentKafkaSpout buildSpout(String topicName, String[] hostNames) {
	List<HostPort> hosts = new ArrayList<HostPort>(hostNames.size());
	for (String hostName: hostNames) {
	    hosts.add(new HostPort(hostName));
	}
	TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(new KafkaConfig.StaticHosts(hosts, 1), topicName);
	kafkaConfig.scheme = new StringScheme();
	// kafkaConfig.fetchSizeBytes = 1024 * 1024 * 5;
	// kafkaConfig.forceStartOffsetTime(-2);
	return new OpaqueTridentKafkaSpout(kafkaConfig);
    }
}
