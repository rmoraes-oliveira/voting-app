# frozen_string_literal: true

require_relative '../../config/vote_reconciler'

RSpec.describe VoteReconciler do
  describe '.rebuild_from_kafka!' do
    let(:redis) { REDIS }
    let(:poll_id) { '42' }
    let(:kafka_consumer) { instance_double(Rdkafka::Consumer, subscribe: nil, close: nil) }

    def message_for(payload)
      double(payload: payload.to_json)
    end

    def vote_payload(event_id: SecureRandom.uuid, candidate_id: '1', poll_id: '42')
      { 'event_id' => event_id, 'candidate_id' => candidate_id, 'poll_id' => poll_id }
    end

    before do
      allow(Rdkafka::Config).to receive(:new).and_return(double(consumer: kafka_consumer))
    end

    context 'when there is no active poll (no candidates registered)' do
      it 'returns an empty hash without touching Kafka', :aggregate_failures do
        expect(described_class.rebuild_from_kafka!(redis)).to eq({})
        expect(Rdkafka::Config).not_to have_received(:new)
      end
    end

    context 'when there is an active poll' do
      before do
        redis.set(RedisKeys.poll_current_poll_id, poll_id)
        redis.sadd(RedisKeys.poll_current_candidates, %w[1 2])
      end

      context 'with votes for the current poll' do
        before do
          allow(kafka_consumer).to receive(:poll).and_return(
            message_for(vote_payload(event_id: 'a', candidate_id: '1', poll_id: poll_id)),
            message_for(vote_payload(event_id: 'b', candidate_id: '1', poll_id: poll_id)),
            message_for(vote_payload(event_id: 'c', candidate_id: '2', poll_id: poll_id)),
            nil, nil, nil, nil, nil
          )
        end

        it 'sums vote deltas per candidate' do
          deltas = described_class.rebuild_from_kafka!(redis)

          expect(deltas).to eq('1' => 2, '2' => 1)
        end

        it 'persists the deltas into the vote totals', :aggregate_failures do
          described_class.rebuild_from_kafka!(redis)

          expect(redis.get(RedisKeys.votes_total(poll_id, '1')).to_i).to eq(2)
          expect(redis.get(RedisKeys.votes_total(poll_id, '2')).to_i).to eq(1)
        end

        it 'appends the recovered votes to the audit stream' do
          described_class.rebuild_from_kafka!(redis)

          expect(redis.xlen(RedisKeys.votes_audit)).to eq(3)
        end

        it 'closes the Kafka consumer' do
          described_class.rebuild_from_kafka!(redis)

          expect(kafka_consumer).to have_received(:close)
        end
      end

      context 'with votes belonging to a different poll' do
        before do
          allow(kafka_consumer).to receive(:poll).and_return(
            message_for(vote_payload(event_id: 'old', candidate_id: '1', poll_id: 'old-poll')),
            nil, nil, nil, nil, nil
          )
        end

        it 'ignores them' do
          deltas = described_class.rebuild_from_kafka!(redis)

          expect(deltas).to eq({})
        end
      end

      context 'with a message missing an event_id' do
        before do
          allow(kafka_consumer).to receive(:poll).and_return(
            message_for('candidate_id' => '1', 'poll_id' => poll_id),
            nil, nil, nil, nil, nil
          )
        end

        it 'skips it' do
          deltas = described_class.rebuild_from_kafka!(redis)

          expect(deltas).to eq({})
        end
      end

      context 'with an event_id already deduplicated by the live consumer' do
        before do
          redis.set(RedisKeys.votes_dedup('already-seen'), 1)

          allow(kafka_consumer).to receive(:poll).and_return(
            message_for(vote_payload(event_id: 'already-seen', candidate_id: '1', poll_id: poll_id)),
            nil, nil, nil, nil, nil
          )
        end

        it 'does not double count it' do
          deltas = described_class.rebuild_from_kafka!(redis)

          expect(deltas).to eq({})
        end
      end

      context 'when a burst of messages is interrupted by empty polls' do
        before do
          allow(kafka_consumer).to receive(:poll).and_return(
            message_for(vote_payload(event_id: 'a', candidate_id: '1', poll_id: poll_id)),
            nil, nil,
            message_for(vote_payload(event_id: 'b', candidate_id: '1', poll_id: poll_id)),
            nil, nil, nil, nil, nil
          )
        end

        it 'resets the empty-poll counter and keeps consuming' do
          deltas = described_class.rebuild_from_kafka!(redis)

          expect(deltas).to eq('1' => 2)
        end
      end

      context 'when Kafka never returns any message' do
        before do
          allow(kafka_consumer).to receive(:poll).and_return(nil, nil, nil, nil, nil)
        end

        it 'stops after 5 consecutive empty polls and returns no deltas', :aggregate_failures do
          deltas = described_class.rebuild_from_kafka!(redis)

          expect(deltas).to eq({})
          expect(kafka_consumer).to have_received(:poll).exactly(5).times
        end
      end
    end
  end
end
