require_relative("storm_invocation")
require 'kafka'

module Wukong
  module Storm

    # Implements the runner for wu-storm.
    class StormRunner < Wukong::Local::LocalRunner

      include Logging
      include StormInvocation

      usage "DATAFLOW|PROCESSOR"

      description <<-EOF.gsub(/^ {8}/,'')
        wu-storm is a commandline tool for launching Storm topologies
        built from Wukong dataflows.

        The following would launch the `twitter_ingestor` Wukong
        dataflow (or processor) as the Storm topology
        `twitter_ingestor` reading from the Kafka topic
        `twitter_stream` and writing to the Kafka topic
        `database_writer`.

          $ wu-storm twitter_ingestor --input=twitter_stream --output=database_writer

        Set the --nimubs_host, --kafka_hosts, and --zookeeper_hosts to
        have Wukong storm find the right services.

        The following would launch the

          $ wu-storm twitter_ingestor --input=s3://twitter_stream --output=database_writer

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
        raise Error.new("An explicit --input topic is required to launch a dataflow")  if settings[:input].nil?  || settings[:input].empty?
        raise Error.new("An explicit --output topic is required to launch a dataflow") if settings[:output].nil? || settings[:output].empty?

        if kafka_input?
          raise Error.new("Must provide a list of comma-separated Kafka hosts")          if settings[:kafka_hosts].nil? || settings[:kafka_hosts].empty?
        end
        
        if s3_input?
          raise Error.new("Must provide an S3 bucket and path")                     if input_uri.path.nil?        || input_uri.path.empty?
          raise Error.new("Must provide an AWS access key (settings[:aws_key])")    if settings[:aws_key].nil?    || settings[:aws_key].empty?
          raise Error.new("Must provide an AWS secret key (settings[:aws_secret])") if settings[:aws_secret].nil? || settings[:aws_secret].empty?
        end
        true
      end

      def run
        log.info("Reading and writing to Kafka at <#{settings[:kafka_hosts]}>")
        log.info("Reading and writing to Zookeeper at <#{settings[:zookeeper_hosts]}>")
        log.info("Dry run:") if settings[:dry_run]
        ensure_input_exists if kafka_input?
        if kill_first?
          log.debug("Killing topology <#{topology_name}>")
          execute_command(storm_kill_commandline)
        end
        execute_command!(storm_launch_commandline)
        raise Error.new("Failed to launch topology #{topology_name}!") unless $?.success?
      end

      protected
      
      def ensure_input_exists
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
        [parts[0], (parts[1] || 9092)]
      end
      
    end
  end
end
