module Wukong
  module Storm

    # A driver to connect events passed in over STDIN to STDOUT.
    # Differs from the vanilla Wukong::Local::LocalDriver in some
    # Storm-specific ways.
    class StormDriver < EM::P::LineAndTextProtocol
      include DriverMethods

      attr_accessor :dataflow, :settings

      def self.start(label, settings = {})
        EM.attach($stdin, self, label, settings)
      end

      def initialize(label, settings)
        super
        @settings = settings
        @dataflow = construct_dataflow(label, settings)
        @messages = []
      end

      def post_init
        setup_dataflow      
      end

      def receive_line line
        driver.send_through_dataflow(line)
        send_messages
      rescue => e
        raise Wukong::Error.new(e)
        EM.stop
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

      def unbind
        EM.stop
      end

      def setup()                             ; end
      def process(record) @messages << record ; end
      def stop()                              ; end

    end
  end
end
