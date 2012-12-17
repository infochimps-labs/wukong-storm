require 'spec_helper'

Wu.processor(:test) do

  def process(record)
    # do nothing
  end
  
end

describe 'wu-storm' do
  let(:examples) { File.expand_path('../support/examples.rb', __FILE__)   }

  context 'without any arguments' do    
    subject      { command 'wu-storm'                                     }
    it           { should exit_with(:non_zero)                            }
    it           { should have_stderr('usage: wu-storm')                  }
  end

  context 'with a simple processor' do
    let(:input)  { 'one event'                                            } 
    subject      { command('wu-storm', examples, '--run=simple') < input  }
    it           { should exit_with(0)                                    }
    it           { should have_stdout('one event|')                       }
  end

  context 'with a skipped processor' do
    let(:input)  { 'never see this'                                       }
    subject      { command('wu-storm', examples, '--run=skipped') < input }
    it           { should exit_with(0)                                    }
    it           { should have_stdout('|')                                }
  end

  context 'with a duplicating processor' do
    let(:input)  { 'foo'                                                  }
    subject      { command('wu-storm', examples, '--run=multi') < input   }
    it           { should exit_with(0)                                    }
    it           { should have_stdout("foo\nfoo\nfoo|")                   }
  end

  context 'with a flow' do
    let(:input)  { '{"foo":"bar"}'                                        }
    subject      { command('wu-storm', examples, '--run=flow') < input    }
    it           { should exit_with(0)                                    }
    it           { should have_stdout('I raised the bar|')                }
  end  

  context 'with multiple arguments' do
    let(:input)  { "foo\nbar\nbaz"                                        }
    subject      { command('wu-storm', examples, '--run=simple') < input  }
    it           { should exit_with(0)                                    }
    it           { should have_stdout('foo|bar|baz|')                     }
  end  
end
