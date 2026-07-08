# frozen_string_literal: true

require_relative '../../config/kafka_lag'

RSpec.describe KafkaLag do
  describe '.total_lag' do
    let(:topic) { 'votes' }
    let(:admin) { instance_double(Rdkafka::Admin, metadata: metadata, close: nil) }
    let(:consumer) { instance_double(Rdkafka::Consumer, committed: committed, close: nil) }
    let(:committed) { instance_double(Rdkafka::Consumer::TopicPartitionList, to_h: committed_partitions) }
    let(:committed_partitions) { {} }

    before do
      allow(Rdkafka::Config).to receive(:new).and_return(double(admin: admin, consumer: consumer))
    end

    context 'when the topic does not exist' do
      let(:metadata) { double(topics: []) }

      it 'returns zero without querying committed offsets', :aggregate_failures do
        expect(described_class.total_lag(topic)).to eq(0)
        expect(consumer).not_to have_received(:committed)
      end
    end

    context 'when the topic has partitions with committed offsets behind the high watermark' do
      let(:metadata) { double(topics: [{ topic_name: topic, partition_count: 2 }]) }
      let(:committed_partitions) do
        {
          topic => [
            Rdkafka::Consumer::Partition.new(0, 90),
            Rdkafka::Consumer::Partition.new(1, 45)
          ]
        }
      end

      before do
        allow(consumer).to receive(:query_watermark_offsets).with(topic, 0).and_return([0, 100])
        allow(consumer).to receive(:query_watermark_offsets).with(topic, 1).and_return([0, 50])
      end

      it 'sums the lag across all partitions' do
        expect(described_class.total_lag(topic)).to eq(15)
      end
    end

    context 'when a partition has no committed offset yet (new consumer group)' do
      let(:metadata) { double(topics: [{ topic_name: topic, partition_count: 1 }]) }
      let(:committed_partitions) do
        { topic => [Rdkafka::Consumer::Partition.new(0, KafkaLag::NO_COMMITTED_OFFSET)] }
      end

      before do
        allow(consumer).to receive(:query_watermark_offsets).with(topic, 0).and_return([0, 30])
      end

      it 'treats the missing offset as zero consumed' do
        expect(described_class.total_lag(topic)).to eq(30)
      end
    end

    context 'when a partition has a nil offset' do
      let(:metadata) { double(topics: [{ topic_name: topic, partition_count: 1 }]) }
      let(:committed_partitions) do
        { topic => [Rdkafka::Consumer::Partition.new(0, nil)] }
      end

      before do
        allow(consumer).to receive(:query_watermark_offsets).with(topic, 0).and_return([0, 10])
      end

      it 'treats the nil offset as zero consumed' do
        expect(described_class.total_lag(topic)).to eq(10)
      end
    end

    context 'when the consumer is fully caught up' do
      let(:metadata) { double(topics: [{ topic_name: topic, partition_count: 1 }]) }
      let(:committed_partitions) do
        { topic => [Rdkafka::Consumer::Partition.new(0, 100)] }
      end

      before do
        allow(consumer).to receive(:query_watermark_offsets).with(topic, 0).and_return([0, 100])
      end

      it 'returns zero lag' do
        expect(described_class.total_lag(topic)).to eq(0)
      end
    end

    context 'when the committed offset is somehow ahead of the high watermark' do
      let(:metadata) { double(topics: [{ topic_name: topic, partition_count: 1 }]) }
      let(:committed_partitions) do
        { topic => [Rdkafka::Consumer::Partition.new(0, 120)] }
      end

      before do
        allow(consumer).to receive(:query_watermark_offsets).with(topic, 0).and_return([0, 100])
      end

      it 'clamps the lag to zero instead of going negative' do
        expect(described_class.total_lag(topic)).to eq(0)
      end
    end

    context 'when closing the admin and consumer clients' do
      let(:metadata) { double(topics: []) }

      it 'always closes the admin client' do
        described_class.total_lag(topic)
        expect(admin).to have_received(:close)
      end
    end
  end
end
