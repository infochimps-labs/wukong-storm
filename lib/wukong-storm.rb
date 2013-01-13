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
      return unless program == 'wu-storm'
      settings.define :zookeepers_servers, description: 'storm.zookeeper.servers'
      settings.define :zookeepers_port,    description: 'storm.zookeeper.port'
      settings.define :local_dir,          description: 'storm.local.dir'
      settings.define :scheduler,          description: 'storm.scheduler'
      settings.define :cluster_mode,       description: 'storm.cluster.mode'
      settings.define :local_hostname,     description: 'storm.local.hostname'
      settings.define :run,                description: 'Name of the processor or dataflow to use. Defaults to basename of the given path', flag: 'r'
      settings.define :delimiter,          description: 'Emitted as a single record to mark the end of the batch ', default: '---', flag: 't'
    end

    # Boots the Wukong::Storm plugin.
    #
    # @param [Configliere::Param] settings the settings to boot from
    # @param [String] root the root directory to boot in
    def self.boot settings, root
    end
    
  end
end

require 'wukong-storm/runner'
