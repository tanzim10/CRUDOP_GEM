# frozen_string_literal: true

require "bundler/gem_tasks"
require "rspec/core/rake_task"

RSpec::Core::RakeTask.new(:spec)

require "rubocop/rake_task"

RuboCop::RakeTask.new

task default: %i[spec rubocop]

# Rakefile
require 'irb'
require 'crudop'

desc "Console for the gem"
task :console do
  ARGV.clear # This is important to prevent IRB from trying to process the rest of your ARGV as a file
  IRB.start
end

