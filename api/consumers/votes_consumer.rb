# frozen_string_literal: true

require 'karafka'
require 'connection_pool'
require_relative '../config/redis_keys'
require_relative '../config/settings'

class VotesConsumer < Karafka::BaseConsumer
  def consume
    votes = messages.filter_map do |message|
      payload = JSON.parse(message.raw_payload)
      event_id = payload.fetch('event_id')
      candidate_id = payload.fetch('candidate_id')
      poll_id = payload.fetch('poll_id')
      timestamp = Time.at(payload.fetch('timestamp'))
      [event_id, candidate_id, poll_id, timestamp, payload]
    rescue JSON::ParserError, KeyError, TypeError => e
      # mensagem malformada: loga e segue em frente
      Karafka.logger.error("skipping malformed message at offset #{message.offset}: #{e.class}: #{e.message}")
      nil
    end

    persist_votes(votes)

    mark_as_consumed(messages.last)
  end

  private

  def persist_votes(votes)
    return if votes.empty?

    # SETNX por event_id evita duplicar voto em reentrega do Kafka
    new_votes = votes.select do |event_id, _, _, _, _|
      redis.set(RedisKeys.votes_dedup(event_id), 1, nx: true, ex: Settings::VOTES_DEDUP_TTL)
    end
    return if new_votes.empty?

    redis.pipelined do |tx|
      new_votes.each do |_, candidate_id, poll_id, timestamp, payload|
        hour_key = timestamp.strftime('%Y-%m-%dT%H')

        tx.incr(RedisKeys.votes_total(poll_id, candidate_id))
        tx.incr(RedisKeys.votes_hourly(poll_id, candidate_id, hour_key))
        tx.expire(RedisKeys.votes_hourly(poll_id, candidate_id, hour_key), 60 * 60 * 24 * 7)
        tx.xadd(RedisKeys.votes_audit, payload)
      end
    end
  rescue Redis::BaseError => e
    Karafka.logger.error(
      {
        event: 'persist_votes_failed',
        error_class: e.class.name,
        error_message: e.message,
        vote_count: votes.size
      }.to_json
    )
    raise
  end

  def redis
    @redis ||= ConnectionPool::Wrapper.new(size: 2, timeout: 5) do
      Redis.new(url: Settings::REDIS_URL, timeout: 5, reconnect_attempts: 1)
    end
  end
end
