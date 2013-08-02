require 'shellwords'
module Wukong
  module Storm

    # This module defines several methods that generate command lines
    # that interact with Storm using the `storm` program.
    module StormInvocation

      # Return the name of the Storm topology from the given settings
      # and/or commandline args.
      #
      # @return [String] the name of the Storm topology
      def topology_name
        settings[:name] || dataflow
      end

      def topology_arg
        args.first
      end

      # Generates a commandline that can be used to launch a new Storm
      # topology based on the given dataflow, input and output topics,
      # and settings.
      #
      # @return [String]
      def storm_launch_commandline
        [
         storm_runner,
         "jar #{wukong_topology_submitter_jar}",
         fully_qualified_class_name,
         native_storm_options,
         storm_topology_options,
        ].flatten.compact.join("\ \t\\\n ")
      end

      # Generates a commandline that can be used to kill a running
      # Storm topology based on the given topology name.
      #
      # @return [String]
      def storm_kill_commandline
        "#{storm_runner} kill #{topology_name} #{storm_kill_options} > /dev/null 2>&1"
      end

      # Generates the commandline that will be used to launch wu-bolt
      # within each bolt of the Storm topology.
      #
      # @return [String]
      def wu_bolt_commandline
        return settings[:bolt_command] if settings[:bolt_command]
        [settings[:command_prefix], 'wu-bolt', topology_arg, non_wukong_storm_params_string].compact.map(&:to_s).reject(&:empty?).join(' ')
      end

      # Return the path to the `storm` program.
      #
      # Will pay attention to `--storm_runner` and `--storm_home`
      # options.
      #
      # @return [String]
      def storm_runner
        explicit_runner = settings[:storm_runner]
        home_runner     = File.join(settings[:storm_home], 'bin/storm')
        default_runner  = 'storm'
        case
        when explicit_runner then explicit_runner
        when File.exist?(home_runner) then home_runner
        else default_runner
        end
      end

      protected

      # Path to the Java jar file containing the submitter class.
      #
      # @return [String]
      #
      # @see #fully_qualified_class_name
      def wukong_topology_submitter_jar
        File.expand_path("wukong-storm.jar", File.dirname(__FILE__))
      end

      # The default Java Submitter class.
      #
      # @see #fully_qualified_class_name
      TOPOLOGY_SUBMITTER_CLASS = "com.infochimps.wukong.storm.TopologySubmitter"

      # Returns the fully qualified name of the Java submitter class.
      #
      # @see TOPOLOGY_SUBMITTER_CLASS
      def fully_qualified_class_name
        TOPOLOGY_SUBMITTER_CLASS
      end

      # Return a String of Java options constructed from mapping the passed
      # in "friendly" options (`--timeout`) to native, Storm options
      # (`topology.message.timeout.secs`).
      #
      # @return [String]
      def native_storm_options
        settings.params_with(:storm).map do |option, value|
          defn = settings.definition_of(option, :description)
          [defn, settings[option.to_sym]]
        end.map { |option, value| java_option(option, value) }
      end

      # Return a String of Java options constructed from mapping the
      # options specific to launching a Storm topology.
      #
      # @return [String]
      def storm_topology_options
        (services_options + topology_options + spout_options + dataflow_options + state_options).reject do |pair|
          key, value = pair
          value.nil? || value.to_s.strip.empty?
        end.map { |pair|  java_option(*pair) }.sort
      end

      def services_options
        [
         ["wukong.kafka.hosts",       settings[:kafka_hosts]],
         ["wukong.zookeeper.hosts",   settings[:zookeeper_hosts]],
        ]
      end

      def topology_options
        [
         ["wukong.topology",          topology_name],
        ]
      end

      def spout_options
        [
         ["wukong.input.offset",      settings[:input_offset]],
         ["wukong.input.partitions",  settings[:input_partitions]],
         ["wukong.input.batch",       settings[:input_batch]],
         ["wukong.input.parallelism", settings[:input_parallelism]],
        ].tap do |opts|
          if blob_input?
            opts << ["wukong.input.type", "blob"]
            if s3_input?
              opts.concat(s3_spout_options)
            else
              opts.concat(file_spout_options)
            end
          else
            opts.concat(kafka_spout_options)
          end
        end
      end

      def s3_spout_options
        [
         ["wukong.input.blob.type",        "s3"],
         ["wukong.input.blob.path",        input_uri.path],
         ["wukong.input.blob.s3_bucket",   input_uri.host],
         ["wukong.input.blob.aws_key",     settings[:aws_key]],
         ["wukong.input.blob.aws_secret",  settings[:aws_secret]],
        ]
      end

      def file_spout_options
        [
         ["wukong.input.blob.type", "file"],
         ["wukong.input.blob.path", input_uri.path],
        ]
      end

      def kafka_spout_options
        [
         ["wukong.input.kafka.topic", settings[:input]],
        ]
      end

      def dataflow_options
        [
         ["wukong.directory",         Dir.pwd],
         ["wukong.dataflow",          topology_arg],
         ["wukong.command",           wu_bolt_commandline],
         ["wukong.parallelism",       settings[:parallelism]],
        ].tap do |opts|
          opts << ["wukong.environment", settings[:environment]] if settings[:environment]
        end
      end

      def state_options
        if kafka_output?
          kafka_state_options
        end
      end

      def kafka_output?
        true
      end

      def kafka_state_options
        [
         ["wukong.output.kafka.topic", settings[:output]],
        ]
      end

      # Return a String of options used when attempting to kill a
      # running Storm topology.
      #
      # @return [String]
      def storm_kill_options
        "-w #{settings[:wait]}"
      end

      # Format the given `option` and `value` into a Java option
      # (`-D`).
      #
      # @param [Object] option
      # @param [Object] value
      # @return [String]
      def java_option option, value
        return unless value
        return if value.to_s.strip.empty?
        "-D#{option}=#{Shellwords.escape(value.to_s)}"
      end

      # Parameters that should be passed onto subprocesses.
      #
      # @return [Configliere::Param]
      def params_to_pass
        settings
      end

      # Return a String stripped of any `wu-storm`-specific params but
      # still including any other params.
      #
      # @return [String]
      def non_wukong_storm_params_string
        params_to_pass.reject do |param, val|
          params_to_pass.definition_of(param, :wukong_storm)
        end.map do |param, val|
          "--#{param}=#{Shellwords.escape(val.to_s)}"
        end.join(" ")
      end

      def input_uri
        @input_uri ||= URI.parse(settings[:input])
      end

      def output_uri
        @output_uri ||= URI.parse(settings[:output])
      end
      
      def blob_input?
        s3_input? || file_input?
      end

      def kafka_input?
        ! blob_input?
      end

      def s3_input?
        input_uri.scheme == 's3'
      end
      
      def file_input?
        input_uri.scheme == 'file'
      end
      
    end
  end
end
