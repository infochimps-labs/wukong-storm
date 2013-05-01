require 'spec_helper'

describe Wukong::Storm::StormInvocation do

  context "without any options" do
    subject { storm_runner('identity', '--input=foo', '--output=bar') }

    its(:topology)                 { should == 'identity' }

    its(:storm_runner)             { should == '/usr/lib/storm/bin/storm' }
    
    its(:storm_launch_commandline) { should match(/jar .*storm.*.jar/)            }
    its(:storm_launch_commandline) { should match(/com\.infochimps\..*Submitter/) }

    its(:storm_kill_commandline)   { should match(/storm kill identity -w 1/) }
  end

  describe "Storm runner options" do
    context "--storm_runner" do
      subject { storm_runner('identity', '--input=foo', '--output=bar', '--storm_runner=/opt/bin/storm') }
      its(:storm_runner) { should == '/opt/bin/storm' }
    end
    context "--storm_home" do
      subject { storm_runner('identity', '--input=foo', '--output=bar', '--storm_home=/usr/local/share/storm') }
      its(:storm_runner) { should == '/usr/local/share/storm/bin/storm' }
    end
  end

  describe "native Storm options" do
    subject { storm_runner('identity', '--input=foo', '--output=bar', '--ackers=10') }
    its(:storm_launch_commandline)   { should match(/topology.acker.executors=10/) }
  end

  describe "Wukong dataflow options" do
    
    context "--name" do
      subject { storm_runner('identity', '--input=foo', '--output=bar', '--name=myFlow') }
      its(:topology) { should == 'myFlow' }
    end
    
    context "--parallelism" do
      subject { storm_runner('identity', '--input=foo', '--output=bar', '--parallelism=10') }
      its(:storm_launch_commandline)   { should match(/wukong.parallelism=10/) }
    end
    
    context "--wait" do
      subject { storm_runner('identity', '--input=foo', '--output=bar', '--wait=10') }
      its(:storm_kill_commandline)   { should match(/storm kill identity -w 10/) }
    end
    
    describe "--command_prefix" do
      subject { storm_runner('identity', '--input=foo', '--output=bar', command_prefix: "bundle exec") }
      its(:storm_launch_commandline)   { should match(/bundle.+exec.+wu-bolt/) }
    end

    describe "--bolt_command" do
      subject { storm_runner('identity', '--input=foo', '--output=bar', bolt_command: "my_commandline --with=arg") }
      its(:storm_launch_commandline)   { should match(/my_commandline.+--with.+=arg/) }
    end
  end
  
end
