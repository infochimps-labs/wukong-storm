require 'spec_helper'

describe Wukong::Storm::StormRunner do

  describe "launching a new topology" do
    it "raises an error without a dataflow (or an explicit --bolt_command) to run" do
      expect { storm_runner('--input=foo', '--output=bar') }.to raise_error(Wukong::Error, /processor.*dataflow.*run/i)
    end

    it "raises an error on a non-existing dataflow" do
      expect { storm_runner('definitelyNotGonnaBeThere', '--input=foo', '--output=bar') }.to raise_error(Wukong::Error, /definitelyNotGonnaBeThere/)
    end
    
    it "raises an error without an --input topic" do
      expect { storm_runner('identity', '--output=bar') }.to raise_error(Wukong::Error, /input.*required/i)
    end
    
    it "raises an error without an --output topic" do
      expect { storm_runner('identity', '--input=foo') }.to raise_error(Wukong::Error, /output.*required/i)
    end
    
  end

  describe "killing a running topology before launching a new one" do
    it "will not try to kill a previously running topology first" do
      storm_runner('identity', '--input=foo', '--output=bar') do
        should_not_receive(:execute_command).with(/storm.*kill/)
      end
    end

    it "will try to kill a previously running topology if asked" do
      storm_runner('identity', '--rm', '--input=foo', '--output=bar') do
        should_receive(:execute_command).with(/storm.*kill/)
      end
    end
  end
end

