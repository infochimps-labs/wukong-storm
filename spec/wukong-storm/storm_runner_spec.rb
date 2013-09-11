require 'spec_helper'

describe Wukong::Storm::StormRunner do

  before do
    @producer = double("Kafka::Producer", push: true)
    Kafka::Producer.stub(:new).and_return(@producer)
  end

  describe "validating a topology about to be launched" do
    it "raises an error without a dataflow (or an explicit --bolt_command) to run" do
      expect { storm_runner('--input=foo', '--output=bar') }.to raise_error(Wukong::Error, /processor.*dataflow.*run/i)
    end

    it "raises an error on a non-existing dataflow" do
      expect { storm_runner('definitelyNotGonnaBeThere', '--input=foo', '--output=bar') }.to raise_error(Wukong::Error, /definitelyNotGonnaBeThere/)
    end

    context "reading and writing from Kafka" do
      it "raises an error without an --input topic" do
        expect { storm_runner('identity', '--output=bar') }.to raise_error(Wukong::Error, /input.*required/i)
      end
    
      it "raises an error without an --output topic" do
        expect { storm_runner('identity', '--input=foo') }.to raise_error(Wukong::Error, /output.*required/i)
      end

      it "raises an error when --kafka_hosts is empty or missing" do
        expect { storm_runner('identity', '--input=foo', '--output=bar', '--kafka_hosts=') }.to raise_error(Wukong::Error, /kafka.*host/i)
      end
    end

    context "reading from S3 and writing to Kafka" do
      it "raises an error without a path" do
        expect { storm_runner('identity', '--input=s3://foo', '--output=baz', '--aws_key=key', '--aws_secret=secret') }.to raise_error(Wukong::Error, /s3.*path/i)
      end
      it "raises an error without an AWS access key" do
        expect { storm_runner('identity', '--input=s3://foo/bar', '--output=baz', '--aws_secret=secret') }.to raise_error(Wukong::Error, /aws.*key/i)
      end
      
      it "raises an error without an AWS secret key" do
        expect { storm_runner('identity', '--input=s3://foo/bar', '--output=baz', '--aws_key=key') }.to raise_error(Wukong::Error, /aws.*secret/i)
      end

      it "raises an error on an invalid AWS region" do
        expect { storm_runner('identity', '--input=s3://foo/bar', '--output=baz', '--aws_key=key', '--aws_secret=secret', '--aws_region=us-east-7') }.to raise_error(Wukong::Error, /aws.*region/i)
      end
      
    end
  end

  describe "setting up for a topology about to be launched" do
    context "when reading from Kafka" do
      it "ensures the Kafka input topic exists" do
        Kafka::Producer.should_receive(:new).with(host: 'localhost', port: 9092, topic: 'foo')
        @producer.should_receive(:push).with([])
        storm_runner('identity', '--input=foo', '--output=bar')
      end
    end
  end

  describe "killing a running topology before launching a new one" do
    it "will not try to kill a previously running topology first" do
      storm_runner('identity', '--input=foo', '--output=bar', '--wait=1') do
        should_not_receive(:execute_command).with(/storm.*kill/)
      end
    end

    it "will try to kill a previously running topology if asked" do
      storm_runner('identity', '--rm', '--input=foo', '--output=bar', '--wait=1') do
        should_receive(:execute_command).with(/storm.*kill/)
      end
    end
  end
end

