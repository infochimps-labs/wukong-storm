require_relative("storm_invocation")
require 'kafka'

module Wukong
  module Storm

    # Implements the runner for wu-storm.
    class StormRunner < Wukong::Local::LocalRunner

      include Logging
      include StormInvocation

      usage "DATAFLOW"

      description <<-EOF.gsub(/^ {8}/,'')
        wu-storm is a commandline tool for controlling Storm
        topologies created from Wukong dataflows.

        Topologies can be launched from a dataflow or processor name
        reading from a named Kafka topic

          $ wu-storm twitter_ingestor --input=twitter_stream

        If the --rm option is specified then the topology will be
        killed first before being launched (useful for a restart).

          $ wu-storm --rm twitter_ingestor --input=twitter_stream
      EOF

      def kill_first?
        settings[:rm]
      end

      def validate
        begin
          super()
        rescue => e
          raise Error.new("Must provide a processor or dataflow to run, via either the --run option or as the first argument, or provide an explicit --bolt_command") unless settings[:bolt_command]
        end
        raise Error.new("An explicit --input topic is required to launch a dataflow")  if settings[:input].nil?  || settings[:input].empty?
        raise Error.new("An explicit --output topic is required to launch a dataflow") if settings[:output].nil? || settings[:output].empty?
        raise Error.new("Must provide a list of comma-separated Kafka hosts")          if settings[:kafka_hosts].nil? || settings[:kafka_hosts].empty?
        true
      end

      def run
        log.info("Dry run:") if settings[:dry_run]
        ensure_input_exists
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
