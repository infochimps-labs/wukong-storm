module Wukong
  module Storm

    # A driver to connect events passed in over STDIN to STDOUT.
    # Differs from the vanilla Wukong::Local::LocalDriver in some
    # Storm-specific ways.
    class StormBoltDriver < Local::StdioDriver
      
      include Logging

      # :nodoc:
      def initialize(label, settings)
        super(label, settings)
        @messages = []
      end

      def receive_line line
        super(line)
        send_messages
      end

      def send_messages
        # message newline message newline message delimiter
        # message newline message newline message newline delimiter newline
        @messages.each do |message|
          $stdout.write(message)
          $stdout.write("\n")
        end
        $stdout.write(settings.delimiter)
        $stdout.write("\n")
        $stdout.flush
        @messages.clear
      end

      def setup()                             ; end #  no syncing
      def process(record) @messages << record ; end

    end
  end
end
