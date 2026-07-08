# frozen_string_literal: true

require_relative 'settings'

# Consulta o lag do consumer group do Karafka.
module KafkaLag
  # rd_kafka_committed usa este valor para "sem offset commitado"
  NO_COMMITTED_OFFSET = -1001

  def self.total_lag(topic)
    config = { 'bootstrap.servers' => Settings::KAFKA_BROKERS }

    admin = Rdkafka::Config.new(config).admin
    partition_count = admin.metadata(topic).topics.find { |t| t[:topic_name] == topic }&.fetch(:partition_count, 0) || 0
    return 0 if partition_count.zero?

    consumer = Rdkafka::Config.new(config.merge('group.id' => Settings::KAFKA_CONSUMER_GROUP_ID)).consumer

    list = Rdkafka::Consumer::TopicPartitionList.new
    list.add_topic(topic, partition_count)
    committed = consumer.committed(list)

    committed.to_h.fetch(topic, []).sum do |partition|
      _low, high = consumer.query_watermark_offsets(topic, partition.partition)
      consumed = partition.offset
      consumed = 0 if consumed.nil? || consumed == NO_COMMITTED_OFFSET
      [high - consumed, 0].max
    end
  ensure
    admin&.close
    consumer&.close
  end
end
