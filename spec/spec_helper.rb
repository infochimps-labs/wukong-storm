require 'wukong-storm'
require 'wukong/spec_helpers'

RSpec.configure do |config|
  config.mock_with :rspec
  
  include Wukong::SpecHelpers

  config.before(:each) do
    Wukong::Log.level = Log4r::OFF
  end

  def root
    @root ||= Pathname.new(File.expand_path('../..', __FILE__))
  end

  def storm_runner *args, &block
    runner(Wukong::Storm::StormRunner, 'wu-storm', *args) do
      stub(:execute_command)
      instance_eval(&block) if block_given?
    end
  end

  def wu_storm *args
    command('wu-storm', *args)
  end
  
  def wu_bolt *args
    command('wu-bolt', *args)
  end
  
  
end
