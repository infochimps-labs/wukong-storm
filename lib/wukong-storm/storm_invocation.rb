require 'shellwords'
module Wukong
  module Storm

    # This module defines several methods that generate command lines
    # that interact with Storm using the `storm` program.
    module StormInvocation

      #
      # == Topology Structure & Properties
      #

      # Return the name of the Storm topology from the given settings
      # and/or commandline args.
      #
      # @return [String] the name of the Storm topology
      def topology_name
        settings[:name] || dataflow
      end

      # Name of the Wukong dataflow to be launched.
      #
      # Obtained from either the first non-option argument passed to
      # `wu-storm` or the `--run` option.
      #
      # @return [String]
      def dataflow_name
        args.first || settings[:run]
      end

      # The input URI for the topology.  Will determine the Trident
      # spout that will be used.
      #
      # @return [URI]
      def input_uri
        @input_uri ||= URI.parse(settings[:input])
      end

      # Does this topology read from Kafka?
      #
      # @return [true, false]
      def kafka_input?
        ! blob_input?
      end
      
      # Does this topology read from a filesystem?
      #
      # @return [true, false]
      def blob_input?
        s3_input? || file_input?
      end
      
      # Does this topology read from Amazon's S3?
      #
      # @return [true, false]
      def s3_input?
        input_uri.scheme == 's3'
      end

      # Does this topology read from a local filesystem?
      #
      # @return [true, false]
      def file_input?
        input_uri.scheme == 'file'
      end

      # The input URI for the topology.  Will determine the Trident
      # state that will be used.
      #
      # @return [URI]
      def output_uri
        @output_uri ||= URI.parse(settings[:output])
      end
      
      # Does this topology write to Kafka?
      #
      # @return [true, false]
      def kafka_output?
        true                    # only option right now
      end

      #
      # == Interaction w/Storm ==
      #
      
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
        [settings[:command_prefix], 'wu-bolt', dataflow_name, non_wukong_storm_params_string].compact.map(&:to_s).reject(&:empty?).join(' ')
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
      
      # Return Java `-D` options constructed from mapping the passed
      # in "friendly" options (`--timeout`) to native, Storm options
      # (`topology.message.timeout.secs`).
      #
      # @return [Array<String>] an array of each `-D` option
      def native_storm_options
        settings.params_with(:storm).map do |option, value|
          defn = settings.definition_of(option, :description)
          [defn, settings[option.to_sym]]
        end.map { |option, value| java_option(option, value) }
      end

      # Return Java `-D` options for Wukong-specific options.
      #
      # @return [Array<String>]
      def storm_topology_options
        (services_options + topology_options + spout_options + dataflow_options + state_options).reject do |pair|
          key, value = pair
          value.nil? || value.to_s.strip.empty?
        end.map { |pair|  java_option(*pair) }.sort
      end

      # Return Java `-D` option key-value pairs related to services
      # used by the topology.
      #
      # @return [Array<Array>] an Array of key-value pairs
      def services_options
        [
         ["wukong.kafka.hosts",       settings[:kafka_hosts]],
         ["wukong.zookeeper.hosts",   settings[:zookeeper_hosts]],
        ]
      end

      # Return Java `-D` option key-value pairs related to the overall
      # topology.
      #
      # @return [Array<Array>] an Array of key-value pairs
      def topology_options
        [
         ["wukong.topology",          topology_name],
        ]
      end

      # Return Java `-D` option key-value pairs related to the
      # topology's spout.
      #
      # @return [Array<Array>] an Array of key-value pairs
      def spout_options
        case
        when blob_input?
          blob_spout_options + (s3_input? ? s3_spout_options : file_spout_options)
        else
          kafka_spout_options
        end
      end

      # Return Java `-D` option key-value pairs related to the
      # topology's spout if it is reading from a generic filesystem.
      #
      # @return [Array<Array>] an Array of key-value pairs
      def blob_spout_options
        [
         ["wukong.input.type", "blob"],
        ].tap do |so|
          so << ["wukong.input.blob.marker",       settings[:offset]] if settings[:offset]
          so << case
          when settings[:from_beginning]
            ["wukong.input.blob.start",        "EARLIEST"]
          when settings[:from_end]
            ["wukong.input.blob.start",        "LATEST"]
          when settings[:offset]
            ["wukong.input.blob.start",        "EXPLICIT"]
          else
            ["wukong.input.blob.start",        "RESUME"]
          end
        end
      end

      # Return Java `-D` option key-value pairs related to the
      # topology's spout if it is reading from S3.
      #
      # @return [Array<Array>] an Array of key-value pairs
      def s3_spout_options
        [
         ["wukong.input.blob.type",         "s3"],
         ["wukong.input.blob.path",         input_uri.path.gsub(%r{^/},'')],
         ["wukong.input.blob.s3_bucket",    input_uri.host],
         ["wukong.input.blob.aws_key",      settings[:aws_key]],
         ["wukong.input.blob.aws_secret",   settings[:aws_secret]],
         ["wukong.input.blob.s3_endpoint",  s3_endpoint]
        ]
      end

      # The AWS endpoint used to communicate with AWS for S3 access.
      #
      # Determined by the AWS region the S3 bucket was declared to be
      # in.
      #
      # @see http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
      def s3_endpoint
        case settings[:aws_region]
        when 'us-east-1'       then 's3.amazonaws.com'
        when 'us-west-1'       then 's3-us-west-1.amazonaws.com'
        when 'us-west-2'       then 's3-us-west-2.amazonaws.com'
        when /EU/, 'eu-west-1' then 's3-eu-west-1.amazonaws.com'
        when 'ap-southeast-1'  then 's3-ap-southeast-1.amazonaws.com'
        when 'ap-southeast-2'  then 's3-ap-southeast-2.amazonaws.com'
        when 'ap-northeast-1'  then 's3-ap-northeast-1.amazonaws.com'
        when 'sa-east-1'       then 's3-sa-east-1.amazonaws.com'
        end
      end

      # Return Java `-D` option key-value pairs related to the
      # topology's spout if it is reading from a local file.
      #
      # @return [Array<Array>] an Array of key-value pairs
      def file_spout_options
        [
         ["wukong.input.blob.type", "file"],
         ["wukong.input.blob.path", input_uri.path],
        ]
      end

      # Return Java `-D` option key-value pairs related to the
      # topology's spout if it is reading from Kafka.
      #
      # @return [Array<Array>] an Array of key-value pairs
      def kafka_spout_options
        [
         ["wukong.input.type",              'kafka'],
         ["wukong.input.kafka.topic",       settings[:input]],
         ["wukong.input.kafka.partitions",  settings[:kafka_partitions]],
         ["wukong.input.kafka.batch",       settings[:kafka_batch]],
         
         ["wukong.input.parallelism",       settings[:input_parallelism]],
         case
         when settings[:from_beginning]
           ["wukong.input.kafka.offset",      "-2"]
         when settings[:from_end]
           ["wukong.input.kafka.offset",      "-1"]
         when settings[:offset]
           ["wukong.input.kafka.offset",      settings[:offset]]
         else
           # Do *not* set anything and the spout will attempt to
           # resume and, finding no prior offset, will start from the
           # end, as though we'd passed "-1"
         end
        ]
      end

      # Return Java `-D` option key-value pairs related to the Wukong
      # dataflow run by the topology.
      #
      # @return [Array<Array>] an Array of key-value pairs
      def dataflow_options
        [
         ["wukong.directory",         Dir.pwd],
         ["wukong.dataflow",          dataflow_name],
         ["wukong.command",           wu_bolt_commandline],
         ["wukong.parallelism",       settings[:parallelism]],
        ].tap do |opts|
          opts << ["wukong.environment", settings[:environment]] if settings[:environment]
        end
      end

      # Return Java `-D` option key-value pairs related to the final
      # state used by the topology.
      #
      # @return [Array<Array>] an Array of key-value pairs
      def state_options
        case
        when kafka_output?
          kafka_state_options
        end
      end

      # Return Java `-D` option key-value pairs related to the final
      # state used by the topology when it is writing to Kafka.
      #
      # @return [Array<Array>] an Array of key-value pairs
      def kafka_state_options
        [
         ["wukong.output.kafka.topic", settings[:output]],
        ]
      end

      protected

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
          (params_to_pass.definition_of(param, :wukong_storm) || params_to_pass.definition_of(param, :wukong))
        end.map do |param, val|
          "--#{param}=#{Shellwords.escape(val.to_s)}"
        end.join(" ")
      end

    end
  end
end
