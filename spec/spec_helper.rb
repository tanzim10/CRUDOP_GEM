# frozen_string_literal: true

ENV["APP_ENV"] = "development" # For gems this is true even for testing
ENV["AWS_DEFAULT_REGION"] ||= "us-east-1"
ENV["AWS_REGION"] ||= ENV["AWS_DEFAULT_REGION"]


require "bundler/setup"
Bundler.setup

$LOAD_PATH.unshift File.expand_path('../../lib', __FILE__)

require "crudop"

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end

