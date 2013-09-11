require_relative("storm_invocation")
require 'kafka'

module Wukong
  module Storm

    # Implements the runner for wu-storm.
    class StormRunner < Wukong::Local::LocalRunner

      # The default port Kafka is assumed to be running on.
      DEFAULT_KAFKA_PORT = 9092

      include Logging
      include StormInvocation

      usage "DATAFLOW|PROCESSOR"

      description <<-EOF.gsub(/^ {8}/,'')
wu-storm is a commandline tool for launching dynamically assembled
parametrized Storm topologies that embed Wukong dataflows.  The
overall "shape" of the launched topology is

  spout -> wukong dataflow -> state

The default spout and state will read and write to Kafka topics.
Additional spouts and states are also available.

Here's an example which launches `my_flow` as a Storm topology reading
from the Kafka topic `raw` and writing to the Kafka topic `clean`:

  $ wu-storm my_flow --input=raw --output=clean

Here's an example which launches `my_flow` as a Storm topology reading
from the S3 bucket `example-data` and the path `raw` and writing to
the Kafka topic `clean`:

  $ wu-storm my_flow --input=s3://example-data/raw --output=clean

There are several options which apply to any topology like --name,
--nimubs_host, --zookeeper_hosts, --ackers, --parallelism, &c.

Some options only make sense when reading from Kafka like
--kafka_batch, --kafka_partitions, or --input_parallelism.

Other options only make sense when reading from S3 like --aws_key,
--aws_secret, and --aws_region.

Options like --from_beginning, --from_end, and --offset have general
applicability but are interpeted differently based on the spout type.

For a complete list of options try `wu storm --help`.
      EOF

      def kill_first?
        settings[:rm]
      end

      def validate
        begin
          super()
        rescue => e
          raise e if dataflow
          raise Error.new("Must provide a processor or dataflow to run, via either the --run option or as the first argument, or provide an explicit --bolt_command") unless settings[:bolt_command]
        end
        raise Error.new("An explicit --input URI is required to launch a dataflow")  if settings[:input].nil?  || settings[:input].empty?
        raise Error.new("An explicit --output URI is required to launch a dataflow") if settings[:output].nil? || settings[:output].empty?

        if kafka_input? || kafka_output?
          raise Error.new("Must provide a list of comma-separated Kafka hosts")          if settings[:kafka_hosts].nil? || settings[:kafka_hosts].empty?
        end
        
        if s3_input?
          raise Error.new("Must provide an S3 bucket and path")                     if input_uri.path.nil?        || input_uri.path.empty?
          raise Error.new("Must provide an AWS access key (settings[:aws_key])")    if settings[:aws_key].nil?    || settings[:aws_key].empty?
          raise Error.new("Must provide an AWS secret key (settings[:aws_secret])") if settings[:aws_secret].nil? || settings[:aws_secret].empty?
          raise Error.new("Invalid AWS region: <#{settings[:aws_region]}>") unless s3_endpoint
        end
        true
      end

      def run
        log_topology_structure_and_settings
        setup_run
        if kill_first?
          log.debug("Killing topology <#{topology_name}> and waiting <#{settings[:wait]}> seconds...")
          execute_command(storm_kill_commandline)
          sleep settings[:wait].to_i unless settings[:dry_run]
        end
        execute_command!(storm_launch_commandline)
        raise Error.new("Failed to launch topology #{topology_name}!") unless settings[:dry_run] || $?.success?
      end

      protected

      def log_topology_structure_and_settings
        log.info("Using Zookeeper at <#{settings[:zookeeper_hosts]}>")
        
        log.info("Reading from Kafka <#{settings[:kafka_hosts]}/#{settings[:input]}>") if kafka_input?
        log.info("Reading from filesystem at <#{settings[:input]}>")                   if blob_input?
        log.info("Writing to Kafka <#{settings[:kafka_hosts]}/#{settings[:output]}>")  if kafka_output?

        log.info("Dry run:") if settings[:dry_run]
      end
      
      def setup_run
        return unless kafka_input?
        topic      = settings[:input]
        host, port = kafka_host_and_port
        log.info("Ensuring input topic <#{topic}> exists on Kafka broker <#{host}:#{port}>")
        Kafka::Producer.new(host: host, port: port, topic: topic).push([]) unless settings[:dry_run]
      end

      def kafka_host_and_port
        kafka_host = settings[:kafka_hosts].to_s.split(/ *, */).first
        raise Error.new("Could not construct a Kafka host from <#{settings[:kafka_hosts]}>") unless kafka_host
        parts = kafka_host.split(':')
        raise Error.new("Badly formed Kafka host <#{kafka_host}>.  Must be in the format HOST[:PORT]") if parts.size > 2
        [parts[0], (parts[1] || DEFAULT_KAFKA_PORT).to_i]
      end
      
    end
  end
end
