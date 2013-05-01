require 'spec_helper'

Wu.processor(:test) do

  def process(record)
    # do nothing
  end
  
end

describe 'wu-bolt' do
  let(:examples) { File.expand_path('../support/examples.rb', __FILE__)   }

  context 'without any arguments' do    
    subject      { wu_bolt                                                }
    it           { should exit_with(:non_zero)                            }
    it           { should have_stderr(/provide a.*dataflow.*run/)         }
  end

  context 'with a simple processor' do
    let(:input)  { 'one event'                                            } 
    subject      { wu_bolt(examples, '--run=simple') < input  }
    it           { should exit_with(0)                                    }
    it           { should have_stdout("one event\n---\n")                 }
  end

  context 'with a skipped processor' do
    let(:input)  { 'never see this'                                       }
    subject      { wu_bolt(examples, '--run=skipped') < input }
    it           { should exit_with(0)                                    }
    it           { should have_stdout("---\n")                            }
  end

  context 'with a duplicating processor' do
    let(:input)  { 'foo'                                                  }
    subject      { wu_bolt(examples, '--run=multi') < input   }
    it           { should exit_with(0)                                    }
    it           { should have_stdout("foo\nfoo\nfoo\n---\n")             }
  end

  context 'with a flow' do
    let(:input)  { '{"foo":"bar"}'                                        }
    subject      { wu_bolt(examples, '--run=flow') < input    }
    it           { should exit_with(0)                                    }
    it           { should have_stdout("I raised the bar\n---\n")          }
  end  

  context 'with multiple arguments' do
    let(:input)  { "foo\nbar\nbaz"                                        }
    subject      { wu_bolt(examples, '--run=simple') < input  }
    it           { should exit_with(0)                                    }
    it           { should have_stdout("foo\n---\nbar\n---\nbaz\n---\n")   }
  end  
end
