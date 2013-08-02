require 'wukong'

module Wukong

  # Connects Wukong to Storm.
  module Storm
    
    include Plugin

    # Configure the given settings object for use with Wukong::Storm.
    #
    # @param [Configliere::Param] settings the settings to configure
    # @param [String] program the name of the currently executing program
    def self.configure settings, program
      case program
      when 'wu-bolt'
        settings.define :run,                description: 'Name of the processor or dataflow to use. Defaults to basename of the given path', flag: 'r'
        settings.define :delimiter,          description: 'Emitted as a single record to mark the end of the batch ', default: 'X', flag: 't'
      when 'wu-storm'
        settings.define :name,               wukong_storm: true, description: "Name for the launched topology"
        settings.define :command_prefix,     wukong_storm: true, description: "Prefix to insert before all Wukong commands"
        settings.define :bolt_command,       wukong_storm: true, description: "Command-line to run within the spawned Storm bolt"
        settings.define :dry_run,            wukong_storm: true, description: "Echo commands that will be run, but don't run them", type: :boolean, default: false
        settings.define :wait,               wukong_storm: true, description: "How many seconds to wait when killing a topology",  type: Integer, default: 300
        settings.define :rm,                 wukong_storm: true, description: "Will kill any running topology of the same name before launching", type: :boolean, default: false
        settings.define :delimiter,          wukong_storm: true, description: "Batch delimiter to use with wu-bolt"
        settings.define :parallelism,        wukong_storm: true, description: "Parallelism hint for wu-bolt"

        settings.define :input,              wukong_storm: true, description: "Kafka input topic"
        settings.define :input_offset,       wukong_storm: true, description: "Offset to use when starting to read from input topic"
        settings.define :input_partitions,   wukong_storm: true, description: "Number of partitions to use for each input"
        settings.define :input_batch,        wukong_storm: true, description: "Batch size when reading from input"
        settings.define :input_parallelism,  wukong_storm: true, description: "Parallelism (number of simultaneous threads) reading input"

        settings.define :output,             wukong_storm: true, description: "Default Kafka output topic"
        
        settings.define :debug,              wukong_storm: true, storm: true, description: 'topology.debug'
        settings.define :optimize,           wukong_storm: true, storm: true, description: 'topology.optimize'
        settings.define :timeout,            wukong_storm: true, storm: true, description: 'topology.message.timeout.secs'
        settings.define :workers,            wukong_storm: true, storm: true, description: 'topology.workers'
        settings.define :worker_opts,        wukong_storm: true, storm: true, description: 'topology.worker.childopts'
        settings.define :ackers,             wukong_storm: true, storm: true, description: 'topology.acker.executors'
        settings.define :sample_rate,        wukong_storm: true, storm: true, description: 'topology.stats.sample.rate'
        
        settings.define :nimbus_host,        wukong_storm: true, storm: true, description: 'nimbus.host',                default: 'localhost'
        settings.define :nimbus_port,        wukong_storm: true, storm: true, description: 'nimbus.thrift.port',         default: 6627
        settings.define :kafka_hosts,        wukong_storm: true, description: "Comma-separated list of Kafka hosts",     default: 'localhost'
        settings.define :zookeeper_hosts,    wukong_storm: true, description: "Comma-separated list of Zookeeper hosts", default: 'localhost'

        settings.define :storm_home,         wukong_storm: true, description: "Path to Storm installation", env_var: "STORM_HOME", default: "/usr/lib/storm"
        settings.define :storm_runner,       wukong_storm: true, description: "Path to Storm executable.  Use this for non-standard Storm installations"
        
      end
    end

    # Boots the Wukong::Storm plugin.
    #
    # @param [Configliere::Param] settings the settings to boot from
    # @param [String] root the root directory to boot in
    def self.boot settings, root
    end
    
  end
end

require 'wukong-storm/storm_runner'
require 'wukong-storm/bolt_runner'
