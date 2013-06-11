require_relative("storm_invocation")

module Wukong
  module Storm

    # Implements the runner for wu-storm.
    class StormRunner < Wukong::Local::LocalRunner

      include Logging
      include StormInvocation

      usage "DATAFLOW|PROCESSOR"

      description <<-EOF.gsub(/^ {8}/,'')
        wu-storm is a commandline tool for controlling Storm
        topologies created from Wukong dataflows.

        Topologies can be launched from a dataflow or processor name
        reading/writing to/from named Kafka topics:

          $ wu-storm twitter_ingestor --input=twitter_stream --output=database_writer

        If the --rm option is specified then the topology will be
        killed first before being launched (useful for a restart).

          $ wu-storm twitter_ingestor --input=twitter_stream --output=database_writer --rm

        Options exist for tuning
          - scaling properties like workers, parallelism, &c.
          - network locations of Storm, Kafka, and Zookeeper
          - exactly what commands to run under the hood

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
        raise Error.new("An explicit --input topic is required to launch a dataflow") unless settings[:input]
        raise Error.new("An explicit --output topic is required to launch a dataflow") unless settings[:output]
        true
      end

      def run
        log.info("Dry run:") if settings[:dry_run]
        if kill_first?
          log.debug("Killing topology <#{topology_name}>")
          execute_command(storm_kill_commandline)
        end
        execute_command!(storm_launch_commandline)
        raise Error.new("Failed to launch topology #{topology_name}!") unless $?.success?
      end
      
    end
  end
end
