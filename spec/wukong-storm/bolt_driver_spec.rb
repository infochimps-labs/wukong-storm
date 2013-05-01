require 'spec_helper'

describe Wukong::Storm::BoltDriver do

  let(:settings) do
    Configliere::Param.new.tap do |s|
      Wukong::Storm.configure(s, 'wu-bolt')
    end
  end
    
  let(:driver)   { Wukong::Storm::BoltDriver.new(:bogus_event_machine_inserted_arg, :identity, settings) }

  describe "setting up a dataflow" do
    context "#post_init hook from EventMachine" do
      after { driver.post_init }
      it "should not sync $stdout" do
        $stdout.should_not_receive(:sync)
      end
    end
  end

  describe "driving a dataflow" do
    context "#receive_line hook from EventMachine" do
      let(:line) { "hello" }
      before { $stdout.stub!(:write)     }
      after  { driver.receive_line(line) }
      it "passes the line to the #send_through_dataflow method" do
        driver.should_receive(:send_through_dataflow).with(line)
      end
      it "calls the #write_output method" do
        driver.should_receive(:write_output)
      end
      it "writes each output record" do
        $stdout.should_receive(:write).with(line)
      end
      it "writes the batch delimiter" do
        $stdout.should_receive(:write).with('---')
      end
      it "writes newlines after each output record and after the batch delimiter" do
        $stdout.should_receive(:write).with("\n").exactly(2).times
      end
      
    end
  end

end
