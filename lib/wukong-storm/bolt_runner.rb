require_relative('bolt_driver')

module Wukong
  module Storm

    # Implements the runner for wu-bolt.
    class StormBoltRunner < Wukong::Local::LocalRunner

      include Logging

      usage "PROCESSOR|FLOW"

      description <<-EOF.gsub(/^ {8}/,'')
        wu-bolt is a commandline tool for running Wukong dataflows as
        bolts within a Storm topology.

        wu-bolt behaves like wu-local except it adds a batch
        terminator after the output generated from each input record.
        This allows Storm to differentiate "no output" from "no output
        yet", important for back-propagating acks.

        For example

          $ echo "adds a terminator" | wu-bolt tokenizer.rb
          adds
          a
          terminator
          ---
          $ echo "" | wu-bolt tokenizer.rb
          ---

        If there is ever a suppressed error in pricessing, or a
        skipped record for any reason, wu-bolt will still output the
        batch terminator.
      EOF

      # :nodoc:
      def driver
        BoltDriver
      end
      
    end
  end
end
