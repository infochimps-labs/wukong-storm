require_relative('driver')

module Wukong
  module Storm

    # Implements the runner for wu-storm.
    class StormRunner < Wukong::Local::LocalRunner

      include Wukong::Logging

      usage "PROCESSOR|FLOW"

      description <<-EOF.gsub(/^ {8}/,'')
        wu-storm is a commandline tool for running Wukong processors and flows
        in a storm or trident topology.

        wu-storm operates over STDIN and STDOUT and has a one-to-one message
        guarantee.  For example, when using an identity processor, wu-storm,
        given an event 'foo', will return 'foo|'. The '|' character is the
        specified End-Of-File delimiter.

        If there is ever a suppressed error in pricessing, or a skipped record
        for any reason, wu-storm will still respond with a '|', signifying an
        empty return event.

        If there are multiple messages that have resulted from a single event,
        wu-storm will return them newline separated, followed by the
        delimiter, e.g. 'foo\nbar\nbaz|'.
      EOF

      # :nodoc:
      def driver
        StormDriver
      end
      
    end
  end
end

  
