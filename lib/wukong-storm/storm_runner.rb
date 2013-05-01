require_relative("storm_invocation")

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
        super()
        raise Error.new("An explicit --input topic is required to launch a dataflow") unless settings[:input]
        raise Error.new("An explicit --output topic is required to launch a dataflow") unless settings[:output]
        true
      end

      def run
        log.info("Dry run:") if settings[:dry_run]
        execute_command(storm_kill_commandline) if kill_first?
        execute_command!(storm_launch_commandline)
        raise Error.new("Failed to launch topology #{dataflow}!") unless $?.success?
      end
      
    end
  end
end
