require 'spec_helper'

describe 'wu-storm' do
  context "without any arguments" do
    let(:subject) { command('wu-storm') }
    it {should exit_with(:non_zero) }
    it "displays help on STDERR" do
      should have_stderr(/processor.*dataflow.*run.*bolt_command/i)
    end
  end

  context "in --dry_run mode" do
    let(:subject) { command('wu-storm', 'identity', "--input=foo", "--output=foo", "--dry_run") }
    it { should exit_with(0) }
    it { should have_stdout(/storm.*jar/, /TopologySubmitter/, /wu-bolt.*identity/) }
  end
end
