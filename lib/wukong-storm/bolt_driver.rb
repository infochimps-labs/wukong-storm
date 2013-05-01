module Wukong
  module Storm

    # Modifies the behavior of Wukong::Local::StdioDriver by appending
    # a batch delimiter after each set of output records, including
    # when there are 0 output records or if an error occurs.
    class BoltDriver < Local::StdioDriver
      
      include Logging

      #
      # == Startup == 
      #
      
      # Override the behavior of StdioDriver by initializing an empty
      # array of output records.
      def initialize(label, settings)
        super(label, settings)
        @output = []
      end

      # Do *not* sync $stdout as in the StdioDriver.
      def setup()
      end

      #
      # == Reading Input == 
      #
      
      # Called by EventMachine framework after successfully reading a
      # line from $stdin.
      #
      # Relies on StdioDriver, but calls #write_output afterwards to
      # ensure that a delimiter is also sent.
      #
      # @param [String] line
      def receive_line line
        super(line)
        write_output
      end

      #
      # == Handling Output == 
      #

      # Don't write the record to $stdout, but store it in an array of
      # output records instead.
      #
      # @param [Object] record
      # 
      # @see #write_output
      def process(record)
        @output << record
      end
      
      # Writes all output records out in a single batch write with a
      # batch delimiter appended to the end.
      #
      # All output records are newline delimited within the batch.
      #
      # The batch itself includes a newline character after the final
      # batch delimiter.
      #
      # $stdout is flushed after the write and accumulated outputs are
      # cleared.
      #
      # @see #process
      def write_output
        @output.each do |record|
          $stdout.write(record)
          $stdout.write("\n")
        end
        $stdout.write(settings.delimiter)
        $stdout.write("\n")
        $stdout.flush
        @output.clear
      end

    end
  end
end
