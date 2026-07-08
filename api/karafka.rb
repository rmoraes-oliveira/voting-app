# frozen_string_literal: true

require 'karafka'
require 'time'
require_relative 'config/settings'
require_relative 'config/kafka_lag'
require_relative 'consumers/votes_consumer'

class KarafkaApp < Karafka::App
  setup do |config|
    config.kafka = { 'bootstrap.servers': Settings::KAFKA_BROKERS }
    config.client_id = 'votes-consumer'
    config.group_id = Settings::KAFKA_CONSUMER_GROUP_ID
    config.logger = Logger.new($stdout)

    config.pause_timeout = 1_000
    config.pause_max_timeout = 30_000
    config.pause_with_exponential_backoff = true

    config.logger.formatter = proc do |severity, datetime, _progname, msg|
      "#{{ timestamp: datetime.iso8601, severity: severity, message: msg }.to_json}\n"
    end
  end

  routes.draw do
    topic Settings::KAFKA_TOPIC do
      consumer VotesConsumer
      max_messages 1000

      dead_letter_queue(
        topic: "#{Settings::KAFKA_TOPIC}_dlq",
        max_retries: 5,
        independent: true
      )
    end
  end
end

Karafka.monitor.subscribe('dead_letter_queue.dispatched') do |event|
  consumer = event[:caller]
  message = event[:message]

  Karafka.logger.error(
    {
      event: 'dlq_dispatch',
      source_topic: consumer.topic.name,
      dlq_topic: consumer.topic.dead_letter_queue.topic,
      partition: message.partition,
      offset: message.offset
    }.to_json
  )
end

# Monitora o lag do consumer periodicamente em background
Karafka.monitor.subscribe('app.running') do
  Thread.new do
    loop do
      sleep Settings::LAG_CHECK_INTERVAL_SECONDS

      begin
        lag = KafkaLag.total_lag(Settings::KAFKA_TOPIC)
        level = lag > Settings::LAG_ALERT_THRESHOLD ? :error : :info

        Karafka.logger.public_send(
          level,
          { event: 'consumer_lag_check', topic: Settings::KAFKA_TOPIC, lag: lag,
            threshold: Settings::LAG_ALERT_THRESHOLD }.to_json
        )
      rescue StandardError => e
        Karafka.logger.error(
          { event: 'consumer_lag_check_failed', error_class: e.class.name, error_message: e.message }.to_json
        )
      end
    end
  end
end
