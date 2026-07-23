# frozen_string_literal: true

require 'sinatra'
require 'json'
require 'securerandom'
require_relative 'config/settings'
require_relative 'config/kafka_lag'
require_relative 'config/redis_keys'
require_relative 'config/http_helpers'

class AdminAPI < Sinatra::Base
  configure do
    set :redis, Settings.redis_client
    set :host_authorization, { permitted_hosts: [] }
    set :protection, except: %i[json_csrf remote_token session_hijacking form_token]
  end

  helpers HttpHelpers

  helpers do
    def redis
      settings.redis
    end
  end

  before do
    api_key = request.env['HTTP_X_API_KEY']
    halt 401, response_error('Unauthorized') unless api_key == Settings::ADMIN_API_KEY
  end

  post '/poll/start' do
    content_type :json

    payload = parsed_request_body
    candidates = payload['candidates']
    poll_id = payload['poll_id'] || SecureRandom.uuid

    halt 400, response_error('Poll is already active') if redis.get(RedisKeys.poll_current_status(poll_id)) == 'active'

    halt 400, response_error('Candidates are required') if candidates.nil? || candidates.empty?
    unless candidates.is_a?(Array) && candidates.all?(Hash)
      halt 400,
           response_error('Candidates must be an array of objects')
    end

    valid_field = ->(value) { value.is_a?(String) || value.is_a?(Integer) }
    halt 400, response_error('Each candidate requires id and name (string or integer)') \
      unless candidates.all? { |c| valid_field.call(c['id']) && valid_field.call(c['name']) }

    candidate_ids = candidates.map { |c| c['id'] }

    redis.multi do |tx|
      tx.del(RedisKeys.poll_current_candidates(poll_id))
      tx.sadd(RedisKeys.poll_current_candidates(poll_id), candidate_ids)
      tx.del(RedisKeys.poll_current_candidate_names(poll_id))
      candidates.each { |c| tx.hset(RedisKeys.poll_current_candidate_names(poll_id), c['id'], c['name']) }
      tx.set(RedisKeys.poll_current_poll_id(poll_id), poll_id)
      tx.set(RedisKeys.poll_current_status(poll_id), 'active')
      tx.set(RedisKeys.poll_current_started_at(poll_id), Time.now.to_i)
      tx.del(RedisKeys.poll_current_ended_at(poll_id))
      tx.sadd(RedisKeys.polls_active, poll_id)

      candidate_ids.each { |id| tx.set(RedisKeys.votes_total(poll_id, id), 0) }
    end

    { status: 'success', message: 'Poll started', poll_id: poll_id }.to_json
  end

  post '/poll/stop/:poll_id' do
    content_type :json

    poll_id = params['poll_id']

    halt 400, response_error('Poll is not active') if redis.get(RedisKeys.poll_current_status(poll_id)) != 'active'

    lag = KafkaLag.total_lag(Settings::KAFKA_TOPIC)
    if lag.positive?
      halt 423, { status: 'error', message: 'Votes still being processed, try again shortly', lag: lag }.to_json
    end

    candidate_ids = redis.smembers(RedisKeys.poll_current_candidates(poll_id))
    ended_at = Time.now.to_i

    results = candidate_ids.to_h do |id|
      [id, {
        name: redis.hget(RedisKeys.poll_current_candidate_names(poll_id), id),
        total_votes: redis.get(RedisKeys.votes_total(poll_id, id)).to_i
      }]
    end

    snapshot = {
      poll_id: poll_id,
      started_at: redis.get(RedisKeys.poll_current_started_at(poll_id)).to_i,
      ended_at: ended_at,
      results: results
    }

    redis.multi do |tx|
      tx.set(RedisKeys.poll_current_status(poll_id), 'stopped')
      tx.set(RedisKeys.poll_current_ended_at(poll_id), ended_at)

      tx.set(RedisKeys.poll_results(poll_id), snapshot.to_json)
      tx.zadd(RedisKeys.poll_results_index, ended_at, poll_id)
      tx.srem(RedisKeys.polls_active, poll_id)
    end

    { status: 'success', message: 'Poll stopped' }.merge(snapshot).to_json
  end

  get '/poll/results/:poll_id' do
    content_type :json

    poll_id = params['poll_id']
    snapshot = redis.get(RedisKeys.poll_results(poll_id))
    halt 404, response_error('Results not found for this poll_id') unless snapshot

    snapshot
  end

  get '/poll/results' do
    content_type :json

    poll_ids = redis.zrevrange(RedisKeys.poll_results_index, 0, -1)
    results = poll_ids.filter_map { |poll_id| redis.get(RedisKeys.poll_results(poll_id)) }
                      .map { |snapshot| JSON.parse(snapshot) }

    { status: 'success', count: results.size, results: results }.to_json
  end
end
