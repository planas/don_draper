# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'don_draper/version'

Gem::Specification.new do |spec|
  spec.name          = "don_draper"
  spec.version       = DonDraper::VERSION
  spec.authors       = ["Adrià Planas"]
  spec.email         = ["adriaplanas@liquidcodeworks.com"]
  spec.summary       = %q{Dick Whitman => Don Draper}
  spec.homepage      = "https://github.com/planas/don_draper"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.5"
  spec.add_development_dependency "rake"
end
