Gem::Specification.new do |gem|
  gem.authors       = ["Ben Riggleman"]
  gem.email         = ["ben.riggleman@gmail.com"]
  gem.description   = %q{Fluentd plugin for Confluent Platform > 3.0.1}
  gem.summary       = %q{Fluentd plugin for Confluent Platform > 3.0.1}
  gem.homepage      = "https://github.com/fluent/fluent-plugin-connect"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "fluent-plugin-connect"
  gem.require_paths = ["lib"]
  gem.version       = '0.0.2'
  gem.required_ruby_version = ">= 2.1.0"

  gem.add_dependency "fluentd", [">= 0.10.58", "< 2"]
  gem.add_dependency 'ltsv', "~> 0"
  gem.add_dependency 'ruby-kafka', '~> 0.3.11'
  gem.add_dependency 'avro', '~> 1.8'
  gem.add_development_dependency "rake", ">= 0.9.2"
  gem.add_development_dependency "test-unit", ["~> 3.0", ">= 3.0.8"]
end
