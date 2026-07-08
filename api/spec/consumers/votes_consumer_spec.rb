# frozen_string_literal: true

require_relative '../../consumers/votes_consumer'

RSpec.describe VotesConsumer do
  subject(:consumer) { described_class.new }

  let(:redis) { REDIS }

  before do
    allow(consumer).to receive(:redis).and_return(redis)
    allow(consumer).to receive(:mark_as_consumed)
  end

  def message_for(payload, offset: 0)
    double(raw_payload: payload.to_json, offset: offset)
  end

  def vote_payload(event_id: SecureRandom.uuid, candidate_id: '1', poll_id: '42', timestamp: Time.now.to_i)
    { 'event_id' => event_id, 'candidate_id' => candidate_id, 'poll_id' => poll_id, 'timestamp' => timestamp }
  end

  describe '#consume' do
    context 'with a valid vote message' do
      let(:payload) { vote_payload(candidate_id: '1', poll_id: '42') }

      before do
        consumer.messages = [message_for(payload)]
        consumer.consume
      end

      it 'increments the total vote count for the candidate' do
        expect(redis.get(RedisKeys.votes_total('42', '1')).to_i).to eq(1)
      end

      it 'increments the hourly vote count for the candidate' do
        hour_key = Time.at(payload['timestamp']).strftime('%Y-%m-%dT%H')
        expect(redis.get(RedisKeys.votes_hourly('42', '1', hour_key)).to_i).to eq(1)
      end

      it 'appends the vote to the audit stream' do
        expect(redis.xlen(RedisKeys.votes_audit)).to eq(1)
      end

      it 'marks the last message as consumed' do
        expect(consumer).to have_received(:mark_as_consumed)
      end
    end

    context 'with multiple valid votes in the same batch' do
      before do
        consumer.messages = [
          message_for(vote_payload(candidate_id: '1', poll_id: '42'), offset: 0),
          message_for(vote_payload(candidate_id: '1', poll_id: '42'), offset: 1),
          message_for(vote_payload(candidate_id: '2', poll_id: '42'), offset: 2)
        ]
        consumer.consume
      end

      it 'tallies votes separately per candidate', :aggregate_failures do
        expect(redis.get(RedisKeys.votes_total('42', '1')).to_i).to eq(2)
        expect(redis.get(RedisKeys.votes_total('42', '2')).to_i).to eq(1)
      end
    end

    context 'with a duplicate event_id (reprocessed message)' do
      let(:payload) { vote_payload(event_id: 'dup-1', candidate_id: '1', poll_id: '42') }

      before do
        consumer.messages = [message_for(payload, offset: 0)]
        consumer.consume

        consumer.messages = [message_for(payload, offset: 1)]
        consumer.consume
      end

      it 'does not double count the vote' do
        expect(redis.get(RedisKeys.votes_total('42', '1')).to_i).to eq(1)
      end
    end

    context 'with a malformed message' do
      let(:good_payload) { vote_payload(candidate_id: '1', poll_id: '42') }

      before do
        consumer.messages = [
          message_for(good_payload, offset: 0),
          double(raw_payload: 'not-json', offset: 1)
        ]
      end

      it 'skips the malformed message and persists the valid ones' do
        consumer.consume

        expect(redis.get(RedisKeys.votes_total('42', '1')).to_i).to eq(1)
      end

      it 'does not raise' do
        expect { consumer.consume }.not_to raise_error
      end
    end

    context 'with a message missing a required field' do
      before do
        incomplete_payload = { 'event_id' => SecureRandom.uuid, 'candidate_id' => '1' }
        consumer.messages = [double(raw_payload: incomplete_payload.to_json, offset: 0)]
      end

      it 'skips the message without raising', :aggregate_failures do
        expect { consumer.consume }.not_to raise_error
        expect(redis.get(RedisKeys.votes_total('42', '1'))).to be_nil
      end
    end

    context 'when every message in the batch is malformed' do
      before do
        consumer.messages = [double(raw_payload: 'not-json', offset: 0)]
      end

      it 'still marks the batch as consumed' do
        consumer.consume

        expect(consumer).to have_received(:mark_as_consumed)
      end
    end

    context 'when persisting votes fails with a Redis error' do
      let(:payload) { vote_payload(candidate_id: '1', poll_id: '42') }

      before do
        consumer.messages = [message_for(payload)]
        allow(redis).to receive(:pipelined).and_raise(Redis::BaseError.new('connection lost'))
      end

      it 're-raises the error' do
        expect { consumer.consume }.to raise_error(Redis::BaseError, 'connection lost')
      end
    end
  end
end
