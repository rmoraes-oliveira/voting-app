# frozen_string_literal: true

require_relative 'settings'
require_relative 'redis_keys'

# Reconstrói os contadores de votos lendo o histórico do tópico Kafka,
# somando ao que já existe (usa a mesma chave de dedup do VotesConsumer).
module VoteReconciler
  def self.rebuild_from_kafka!(redis)
    candidate_ids = redis.smembers(RedisKeys.poll_current_candidates)
    return {} if candidate_ids.empty?

    current_poll_id = redis.get(RedisKeys.poll_current_poll_id)

    config = {
      'bootstrap.servers' => Settings::KAFKA_BROKERS,
      'group.id' => "reconciliation-#{Time.now.to_i}",
      'auto.offset.reset' => 'earliest'
    }
    consumer = Rdkafka::Config.new(config).consumer
    consumer.subscribe(Settings::KAFKA_TOPIC)

    deltas = Hash.new(0)
    consecutive_empty_polls = 0
    new_audit_entries = []

    loop do
      message = consumer.poll(2000)
      if message
        consecutive_empty_polls = 0
        payload = JSON.parse(message.payload)
        next unless payload['poll_id'] == current_poll_id

        event_id = payload['event_id']
        next unless event_id

        next unless redis.set(RedisKeys.votes_dedup(event_id), 1, nx: true, ex: Settings::VOTES_DEDUP_TTL)

        deltas[payload['candidate_id']] += 1
        new_audit_entries << payload
      else
        consecutive_empty_polls += 1
        break if consecutive_empty_polls >= 5
      end
    end

    redis.multi do |tx|
      deltas.each { |candidate_id, delta| tx.incrby(RedisKeys.votes_total(current_poll_id, candidate_id), delta) }
      new_audit_entries.each { |payload| tx.xadd(RedisKeys.votes_audit, payload) }
    end

    deltas
  ensure
    consumer&.close
  end
end
