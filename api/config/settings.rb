# frozen_string_literal: true

require 'redis'
require 'connection_pool'
require 'rdkafka'

module Settings
  REDIS_URL = ENV.fetch('REDIS_URL', 'redis://localhost:6379/0')
  REDIS_POOL_SIZE = ENV.fetch('REDIS_POOL_SIZE', 10).to_i
  KAFKA_BROKERS = ENV.fetch('KAFKA_BROKERS', 'localhost:9092')
  KAFKA_TOPIC   = ENV.fetch('KAFKA_TOPIC', 'votes')
  # group_id explícito, usado por KafkaLag para consultar o lag deste grupo
  KAFKA_CONSUMER_GROUP_ID = ENV.fetch('KAFKA_CONSUMER_GROUP_ID', 'votes-consumer-group')
  ADMIN_API_KEY = ENV.fetch('ADMIN_API_KEY')
  # TTL de dedup do voto — usado por VotesConsumer e VoteReconciler
  VOTES_DEDUP_TTL = 60 * 60 * 24 * 14
  LAG_CHECK_INTERVAL_SECONDS = ENV.fetch('LAG_CHECK_INTERVAL_SECONDS', 30).to_i
  LAG_ALERT_THRESHOLD = ENV.fetch('LAG_ALERT_THRESHOLD', 500).to_i

  def self.redis_client
    @redis_client ||= ConnectionPool::Wrapper.new(size: REDIS_POOL_SIZE, timeout: 5) { Redis.new(url: REDIS_URL) }
  end

  def self.kafka_producer
    @kafka_producer ||= Rdkafka::Config.new(
      'bootstrap.servers' => KAFKA_BROKERS,
      'linger.ms' => 5,
      'batch.num.messages' => 1000,
      'queue.buffering.max.messages' => 100_000
    ).producer.tap do |producer|
      producer.delivery_callback = lambda do |delivery_report|
        next unless delivery_report.error != 0

        message = "[kafka] delivery failed: topic=#{delivery_report.topic_name} " \
                  "partition=#{delivery_report.partition} error_code=#{delivery_report.error}"
        defined?(APP_LOGGER) ? APP_LOGGER.error(message) : warn(message)
      end
    end
  end
end
