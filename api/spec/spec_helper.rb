# frozen_string_literal: true

require 'rack/test'
require 'rspec'
require 'mock_redis'
require 'pry'
require 'simplecov'
require 'fileutils'

SimpleCov.start
SimpleCov.minimum_coverage 95
SimpleCov.add_filter '/spec/'

ENV['ADMIN_API_KEY'] ||= 'test'
ENV['LOG_DIR'] ||= File.expand_path('../tmp/log', __dir__)
ENV['SKIP_PROOF_OF_WORK'] ||= 'true'
ENV['PROMETHEUS_DIR'] ||= File.expand_path('../tmp/prometheus_metrics', __dir__)

FileUtils.mkdir_p(ENV.fetch('PROMETHEUS_DIR', nil))
Dir.glob(File.join(ENV.fetch('PROMETHEUS_DIR', nil), '*')).each { |f| File.delete(f) }

require_relative '../app'
require_relative '../admin_app'

RSpec.configure do |config|
  config.include Rack::Test::Methods

  config.before do
    REDIS = MockRedis.new
    VotingAPI.set(:redis, REDIS)
    AdminAPI.set(:redis, REDIS)
  end

  config.after do
    Object.send(:remove_const, :REDIS)
  end
end
