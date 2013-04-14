module Wukong
  module Storm

    # Implements the runner for wu-storm.
    class StormRunner < Wukong::Local::LocalRunner

      include Wukong::Logging

      usage "PROCESSOR|FLOW"

      description <<-EOF.gsub(/^ {8}/,'')
        wu-storm is a commandline tool for creating Storm topologies
        from Wukong dataflows.
      EOF

      
      
    end
  end
end

  
