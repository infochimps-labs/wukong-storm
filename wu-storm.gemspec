# -*- encoding: utf-8 -*-
require File.expand_path('../lib/wukong-storm/version', __FILE__)

Gem::Specification.new do |gem|
  gem.name          = 'wukong-storm'
  gem.homepage      = 'https://github.com/infochimps-labs/wukong-storm'
  gem.licenses      = ["Apache 2.0"]
  gem.email         = 'coders@infochimps.org'
  gem.authors       = ['Infochimps', 'Travis Dempsey']
  gem.version       = Wukong::Storm::VERSION

  gem.summary       = 'Storm processing for Ruby'
  gem.description   = <<-EOF
EOF

  gem.files         = `git ls-files`.split("\n")
  gem.executables   = ['wu-storm']
  gem.test_files    = gem.files.grep(/^spec/)
  gem.require_paths = ['lib']

  gem.add_dependency('wukong', '3.0.0')
end
