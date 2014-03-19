require 'fileutils'
require 'spec_helper'

describe Wukong::Storm::StormInvocation do

  before do
    @producer = double("Kafka::Producer", push: true)
    Kafka::Producer.stub(:new).and_return(@producer)
  end
  
  context "without any options" do
    subject { storm_runner('identity', '--input=foo', '--output=bar') }

    its(:topology_name)                 { should == 'identity' }
    its(:dataflow_name)                 { should == 'identity' }
    
    its(:kafka_input?)                  { should be_true }
    its(:kafka_output?)                 { should be_true }

    its(:storm_runner)             { should == 'storm' }
    
    its(:storm_launch_commandline) { should match(/jar .*storm.*.jar/)            }
    its(:storm_launch_commandline) { should match(/com\.infochimps\..*TopologySubmitter/) }

    its(:storm_kill_commandline)   { should match(/storm kill identity -w 300/) }
  end

  describe "Storm runner options" do
    context "--storm_runner" do
      subject { storm_runner('identity', '--input=foo', '--output=bar', '--storm_runner=/opt/bin/storm') }
      its(:storm_runner) { should == '/opt/bin/storm' }
    end
    context "--storm_home" do
      around do |example|
        FileUtils.mkdir_p File.join(pwd, 'bin')
        FileUtils.touch   File.join(pwd, 'bin', 'storm')
        example.run
        FileUtils.rm_r    File.join(pwd, 'bin')
      end
      let(:pwd){ File.expand_path('../..', __FILE__) }
      subject { storm_runner('identity', '--input=foo', '--output=bar', "--storm_home=#{pwd}") }
      its(:storm_runner) { should == File.join(pwd, 'bin/storm') }
    end
  end

  describe "native Storm options" do
    context "when setting --ackers" do
      subject { storm_runner('identity', '--input=foo', '--output=bar', '--ackers=10') }
      its(:storm_launch_commandline)   { should match(/topology.acker.executors=10/) }
    end
    context "when setting --wait" do
      subject { storm_runner('identity', '--input=foo', '--output=bar', '--wait=1') }
      its(:storm_kill_commandline)   { should match(/-w 1/) }
    end
  end

  describe "services options" do
    context "by default" do
      subject { storm_runner('identity', '--input=foo', '--output=bar') }
      its(:storm_launch_commandline) { should match(/\wukong\.kafka\.hosts.*localhost/) }
      its(:storm_launch_commandline) { should match(/\wukong\.zookeeper\.hosts.*localhost/) }
    end
    context "when setting --kafka_hosts" do
      subject { storm_runner('identity', '--input=foo', '--output=bar', '--kafka_hosts=k1.example.com:9092,k2.example.com:9093') }
      its(:storm_launch_commandline) { should match(/\wukong\.kafka\.hosts.*k1.example.com:9092,k2.example.com:9093/) }
    end
    context "when setting --zookeeper_hosts" do
      subject { storm_runner('identity', '--input=foo', '--output=bar', '--zookeeper_hosts=z1.example.com:2181,z2.example.com:3181') }
      its(:storm_launch_commandline) { should match(/\wukong\.zookeeper\.hosts.*z1.example.com:2181,z2.example.com:3181/) }
    end
  end

  describe "topology options" do
    context "by default" do
      subject { storm_runner('identity', '--input=foo', '--output=bar') }
      its(:topology_name) { should == 'identity' }
      its(:dataflow_name) { should == 'identity' }
      its(:storm_launch_commandline) { should match(/\wukong\.topology.*identity/) }
    end
    context "when setting --name" do
      subject { storm_runner('identity', '--input=foo', '--output=bar', '--name=myFlow') }
      its(:topology_name) { should == 'myFlow' }
      its(:dataflow_name) { should == 'identity' }
      its(:storm_launch_commandline) { should match(/\wukong\.topology.*myFlow/) }
    end
  end

  describe "spout options" do
    context "when reading from Kafka" do
      subject { storm_runner('identity', '--input=foo', '--output=bar') }
      its(:storm_launch_commandline) { should match(/\wukong\.input\.type.*kafka/)           }
      its(:storm_launch_commandline) { should match(/\wukong\.input\.kafka\.topic.*foo/)     }
      its(:storm_launch_commandline) { should match(/\wukong\.input\.kafka\.partitions.*1/)  }
      its(:storm_launch_commandline) { should match(/\wukong\.input\.kafka\.batch.*1048576/) }
      its(:storm_launch_commandline) { should match(/\wukong\.input\.parallelism.*1/)        }
      context "when setting --kafka_partitions" do
        subject { storm_runner('identity', '--input=foo', '--output=bar', '--kafka_partitions=10') }
        its(:storm_launch_commandline) { should match(/\wukong\.input\.kafka\.partitions.*10/)  }
      end
      context "when setting --kafka_batch" do
        subject { storm_runner('identity', '--input=foo', '--output=bar', '--kafka_batch=100') }
        its(:storm_launch_commandline) { should match(/\wukong\.input\.kafka\.batch.*10/) }
      end
      context "when setting --input_parallelism" do
        subject { storm_runner('identity', '--input=foo', '--output=bar', '--input_parallelism=10') }
        its(:storm_launch_commandline) { should match(/\wukong\.input\.parallelism.*10/) }
      end
      context "when setting --from_beginning" do
        subject { storm_runner('identity', '--input=foo', '--output=bar', '--from_beginning') }
        its(:storm_launch_commandline) { should match(/\wukong\.input\.kafka\.offset.*-2/) }
      end
      context "when setting --from_end" do
        subject { storm_runner('identity', '--input=foo', '--output=bar', '--from_end')    }
        its(:storm_launch_commandline) { should match(/\wukong\.input\.kafka\.offset.*-1/) }
      end
      context "when setting --offset" do
        subject { storm_runner('identity', '--input=foo', '--output=bar', '--offset=1234')    }
        its(:storm_launch_commandline) { should match(/\wukong\.input\.kafka\.offset.*1234/) }
      end
    end
    context "when reading from a filesystem" do
      context "when reading from a local filesystem" do
        subject { storm_runner('identity', '--input=file:///foo/bar', '--output=baz')    }
        its(:storm_launch_commandline) { should match(/\wukong\.input\.type.*blob/)             }
        its(:storm_launch_commandline) { should match(/\wukong\.input\.blob\.type.*file/)       }
        its(:storm_launch_commandline) { should match(/\wukong\.input\.blob\.path.*\/foo\/bar/) }
        its(:storm_launch_commandline) { should match(/\wukong\.input\.blob\.start.*RESUME/)    }
        context "when setting --from_beginning" do
          subject { storm_runner('identity', '--input=file:///foo/bar', '--output=baz', '--from_beginning')    }
          its(:storm_launch_commandline) { should match(/\wukong\.input\.blob\.start.*EARLIEST/) }
        end
        context "when setting --from_end" do
          subject { storm_runner('identity', '--input=file:///foo/bar', '--output=baz', '--from_end')    }
          its(:storm_launch_commandline) { should match(/\wukong\.input\.blob\.start.*LATEST/) }
        end
        context "when setting --offset" do
          subject { storm_runner('identity', '--input=file:///foo/bar', '--output=baz', '--offset=bing-1')    }
          its(:storm_launch_commandline) { should match(/\wukong\.input\.blob\.start.*EXPLICIT/) }
          its(:storm_launch_commandline) { should match(/\wukong\.input\.blob\.marker.*bing-1/)  }
        end
      end
      context "when reading from S3" do
        subject { storm_runner('identity', '--input=s3://foo/bar', '--output=baz', '--aws_key=key', '--aws_secret=secret') }
        its(:storm_launch_commandline) { should match(/\wukong\.input\.type.*blob/)               }
        its(:storm_launch_commandline) { should match(/\wukong\.input\.blob\.type.*s3/)           }
        its(:storm_launch_commandline) { should match(/\wukong\.input\.blob\.aws_key.*key/)       }
        its(:storm_launch_commandline) { should match(/\wukong\.input\.blob\.aws_secret.*secret/) }
        its(:storm_launch_commandline) { should match(/\wukong\.input\.blob\.s3_endpoint.*s3\.amazonaws\.com/) }
        its(:storm_launch_commandline) { should match(/\wukong\.input\.blob\.s3_bucket.*foo/)     }
        its(:storm_launch_commandline) { should match(/\wukong\.input\.blob\.path.*bar/)          }
        its(:storm_launch_commandline) { should match(/\wukong\.input\.blob\.start.*RESUME/)      }
        context "when setting --from_beginning" do
          subject { storm_runner('identity', '--input=s3://foo/bar', '--output=baz', '--aws_key=key', '--aws_secret=secret', '--from_beginning') }
          its(:storm_launch_commandline) { should match(/\wukong\.input\.blob\.start.*EARLIEST/) }
        end
        context "when setting --from_end" do
          subject { storm_runner('identity', '--input=s3://foo/bar', '--output=baz', '--aws_key=key', '--aws_secret=secret', '--from_end') }
          its(:storm_launch_commandline) { should match(/\wukong\.input\.blob\.start.*LATEST/) }
        end
        context "when setting --offset" do
          subject { storm_runner('identity', '--input=s3://foo/bar', '--output=baz', '--aws_key=key', '--aws_secret=secret', '--offset=bing-1') }
          its(:storm_launch_commandline) { should match(/\wukong\.input\.blob\.start.*EXPLICIT/) }
          its(:storm_launch_commandline) { should match(/\wukong\.input\.blob\.marker.*bing-1/)  }
        end
        context "when setting --aws_region" do
          subject { storm_runner('identity', '--input=s3://foo/bar', '--output=baz', '--aws_key=key', '--aws_secret=secret', '--aws_region=us-west-1') }
          its(:storm_launch_commandline) { should match(/\wukong\.input\.blob\.s3_endpoint.*s3-us-west-1\.amazonaws\.com/) }
        end
      end
    end
  end

  describe "dataflow options" do
    subject { storm_runner('identity', '--input=foo', '--output=bar') }
    its(:storm_launch_commandline)   { should match(/wukong\.directory.*#{Dir.pwd}/) }
    its(:storm_launch_commandline)   { should match(/wukong\.dataflow.*identity/) }
    its(:storm_launch_commandline)   { should match(/wukong\.command.*wu-bolt.*identity/) }
    its(:storm_launch_commandline)   { should match(/wukong\.parallelism.*1/) }
    context "when setting --environment" do
      subject { storm_runner('identity', '--input=foo', '--output=bar', '--environment=production') }
      its(:storm_launch_commandline)   { should match(/wukong\.environment.*production/) }
    end
    context "when setting --command_prefix" do
      subject { storm_runner('identity', '--input=foo', '--output=bar', '--command_prefix="bundle exec"') }
      its(:storm_launch_commandline)   { should match(/wukong\.command.*bundle.*exec.*wu-bolt.*identity/) }
    end
    context "when setting --bolt_command" do
      subject { storm_runner('identity', '--input=foo', '--output=bar', '--bolt_command="uniq -c"') }
      its(:storm_launch_commandline)   { should match(/wukong\.command.*uniq.*-c/) }
    end
    context "when setting --parallelism" do
      subject { storm_runner('identity', '--input=foo', '--output=bar', '--parallelism=10') }
      its(:storm_launch_commandline)   { should match(/wukong\.parallelism.*10/) }
    end
  end

  describe "state options" do
    context "when writing to Kafka" do
      subject { storm_runner('identity', '--input=foo', '--output=bar') }
      its(:storm_launch_commandline)   { should match(/wukong\.output\.kafka\.topic.*bar/) }
    end
  end
  
end
