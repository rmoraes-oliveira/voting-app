# frozen_string_literal: true

require 'sinatra'
require 'json'
require 'securerandom'
require_relative 'config/settings'
require_relative 'config/metrics'
require_relative 'config/logging'
require_relative 'config/redis_keys'
require_relative 'config/http_helpers'
require_relative 'vote_validator'

class VotingAPI < Sinatra::Base
  configure do
    set :redis, Settings.redis_client
    set :kafka_producer, Settings.kafka_producer
    set :host_authorization, { permitted_hosts: [] }
    set :protection, except: %i[json_csrf remote_token session_hijacking form_token]
  end

  helpers HttpHelpers

  helpers do
    def redis
      settings.redis
    end

    def kafka_producer
      settings.kafka_producer
    end

    def poll_id
      redis.get(RedisKeys.poll_current_poll_id)
    end

    def vote_validator
      @vote_validator ||= VoteValidator.new(redis)
    end
  end

  before do
    @request_started_at = Process.clock_gettime(Process::CLOCK_MONOTONIC)
    APP_LOGGER.debug("#{request.request_method} #{request.path_info} started")
    headers 'Access-Control-Allow-Origin' => '*', 'Access-Control-Allow-Methods' => 'GET, POST'
  end

  after do
    route = (env['sinatra.route'] || '').split.last || request.path_info
    duration = Process.clock_gettime(Process::CLOCK_MONOTONIC) - @request_started_at

    HTTP_REQUESTS_TOTAL.increment(labels: { method: request.request_method, path: route, status: response.status })
    HTTP_REQUEST_DURATION.observe(duration, labels: { method: request.request_method, path: route })

    APP_LOGGER.info("#{request.request_method} #{route} #{response.status} #{(duration * 1000).round(1)}ms")
  end

  get '/health' do
    content_type :json
    status 200
    { status: 'ok' }.to_json
  end

  options '*' do
    headers 'Access-Control-Allow-Headers' => 'Content-Type'
    status 204
  end

  get '/poll' do
    content_type :json

    candidates = redis.smembers(RedisKeys.poll_current_candidates)
    candidate_names = candidates.to_h do |candidate_id|
      [candidate_id, redis.hget(RedisKeys.poll_current_candidate_names, candidate_id)]
    end

    {
      status: 'success',
      poll_id: redis.get(RedisKeys.poll_current_poll_id),
      status_value: redis.get(RedisKeys.poll_current_status),
      started_at: redis.get(RedisKeys.poll_current_started_at),
      ended_at: redis.get(RedisKeys.poll_current_ended_at),
      candidates: candidate_names
    }.to_json
  end

  post '/votes' do
    content_type :json

    request_payload = parsed_request_body
    candidate_id = request_payload['candidate_id']
    challenge_token = request_payload['challenge_token']
    nonce = request_payload['nonce']

    result = vote_validator.call(candidate_id: candidate_id, challenge_token: challenge_token, nonce: nonce)

    unless result.valid?
      APP_LOGGER.warn("vote rejected: #{result.message} (candidate_id=#{candidate_id})")
      halt result.status, { status: 'error', message: result.message }.to_json
    end

    current_poll_id = result.poll_id

    event = {
      event_id: SecureRandom.uuid,
      candidate_id: candidate_id,
      poll_id: current_poll_id,
      timestamp: Time.now.to_i,
      source_ip: request.ip
    }

    begin
      kafka_producer.produce(topic: Settings::KAFKA_TOPIC, payload: event.to_json, key: candidate_id.to_s)
    rescue Rdkafka::RdkafkaError => e
      APP_LOGGER.error("failed to publish vote to Kafka: #{e.message} (event_id=#{event[:event_id]})")
      halt 503, response_error("Failed to send vote to Kafka: #{e.message}")
    end

    VOTES_COUNTER.increment(labels: { candidate_id: candidate_id })

    status 202
    { status: 'success', received_vote: event }.to_json
  end

  get '/metrics' do
    content_type 'text/plain; version=0.0.4'
    Prometheus::Client::Formats::Text.marshal(Prometheus::Client.registry)
  end

  get '/votes/summary/:poll_id' do
    content_type :json

    halt 404, response_error('Poll not found') unless params['poll_id'] == poll_id

    candidates = redis.smembers(RedisKeys.poll_current_candidates)
    summary = candidates.each_with_object({}) do |candidate_id, result|
      total_votes = redis.get(RedisKeys.votes_total(poll_id, candidate_id)).to_i
      result[candidate_id] = {
        name: redis.hget(RedisKeys.poll_current_candidate_names, candidate_id),
        total_votes: total_votes
      }
    end

    hourly_votes = candidates.each_with_object({}) do |candidate_id, result|
      keys = redis.keys("#{RedisKeys.votes_hourly_prefix(poll_id, candidate_id)}*")
      hourly_data = keys.each_with_object({}) do |key, data|
        hour = key.split(':').last
        data[hour] = redis.get(key).to_i
      end
      result[candidate_id] = {
        name: redis.hget(RedisKeys.poll_current_candidate_names, candidate_id),
        hourly_votes: hourly_data
      }
    end

    { status: 'success',
      poll_id: poll_id,
      total_votes: summary.sum { |_, v| v[:total_votes] },
      summary: summary,
      hourly_votes: hourly_votes }.to_json
  end

  get '/votes/challenge' do
    content_type :json

    challenge = SecureRandom.hex(16)
    redis.setex(RedisKeys.challenge(challenge), 300, 'valid')

    { status: 'success', challenge: challenge }.to_json
  end
end
