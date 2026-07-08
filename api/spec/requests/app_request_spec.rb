# frozen_string_literal: true

RSpec.describe 'App Request' do
  def app
    VotingAPI
  end

  before do
    REDIS.set(RedisKeys.poll_current_status, 'active')
    REDIS.set(RedisKeys.poll_current_poll_id, '42')
    REDIS.set(RedisKeys.poll_current_started_at, Time.now.to_i)
    REDIS.sadd(RedisKeys.poll_current_candidates, [1, 2, 3])
    REDIS.hset(RedisKeys.poll_current_candidate_names, 1, 'João')
    REDIS.hset(RedisKeys.poll_current_candidate_names, 2, 'Maria')
  end

  describe 'POST /votes' do
    let(:producer) { instance_double(Rdkafka::Producer) }

    before do
      VotingAPI.set(:kafka_producer, producer)
      allow(producer).to receive(:produce).and_return(true)
    end

    context 'when the request body is not valid JSON' do
      before do
        post '/votes', 'not-json', { 'CONTENT_TYPE' => 'application/json' }
      end

      it 'returns an error response', :aggregate_failures do
        expect(last_response.status).to eq(400)
        expect(JSON.parse(last_response.body)).to include('status' => 'error')
        expect(JSON.parse(last_response.body)).to include('message' => 'Invalid JSON format')
      end
    end

    context 'when the vote is valid' do
      before do
        post '/votes', { candidate_id: 1 }.to_json, { 'CONTENT_TYPE' => 'application/json' }
      end

      it 'increments the vote count in Prometheus' do
        expect(VOTES_COUNTER.get(labels: { candidate_id: 1 })).to eq(1)
      end

      # it "produces a message to Kafka" do
      #   expect(producer).to have_received(:produce).with(hash_including(topic: Settings::KAFKA_TOPIC, payload: kind_of(String), key: "1"))
      # end

      it 'returns a success response' do
        expect(last_response.status).to eq(202)
      end

      context 'when the challenge token and nonce are provided' do
        before do
          REDIS.set(RedisKeys.challenge('valid_token'), 'some_value')
          post '/votes', { candidate_id: 1, challenge_token: 'valid_token', nonce: '0000abc' }.to_json,
               { 'CONTENT_TYPE' => 'application/json' }
        end

        it 'returns a success response' do
          expect(last_response.status).to eq(202)
        end
      end
    end

    context 'when raise error Rdkafka::RdkafkaError' do
      let(:kafka_error) { Rdkafka::RdkafkaError.new(-1) }

      before do
        allow(producer).to receive(:produce).and_raise(kafka_error)
        post '/votes', { candidate_id: 1 }.to_json, { 'CONTENT_TYPE' => 'application/json' }
      end

      it 'returns a 503 error response', :aggregate_failures do
        expect(last_response.status).to eq(503)
        expect(JSON.parse(last_response.body)).to include('status' => 'error')
        expect(JSON.parse(last_response.body)).to include('message' => "Failed to send vote to Kafka: #{kafka_error.message}")
      end
    end

    context 'when the vote is invalid' do
      let(:candidate_id) { 99 }

      context 'when the candidate does not exist' do
        before do
          post '/votes', { candidate_id: candidate_id }.to_json, { 'CONTENT_TYPE' => 'application/json' }
        end

        it 'returns an error response', :aggregate_failures do
          expect(last_response.status).to eq(400)
          expect(JSON.parse(last_response.body)).to include('status' => 'error')
          expect(JSON.parse(last_response.body)).to include('message' => 'Candidate does not exist')
        end
      end

      context 'when the poll is not active' do
        before do
          REDIS.set(RedisKeys.poll_current_status, 'inactive')
          post '/votes', { candidate_id: 1 }.to_json, { 'CONTENT_TYPE' => 'application/json' }
        end

        it 'returns an error response', :aggregate_failures do
          expect(last_response.status).to eq(403)
          expect(JSON.parse(last_response.body)).to include('status' => 'error')
          expect(JSON.parse(last_response.body)).to include('message' => 'Poll is not active')
        end
      end

      context 'when the challenge fails validation' do
        before do
          ENV['SKIP_PROOF_OF_WORK'] = 'false'
        end

        after { ENV['SKIP_PROOF_OF_WORK'] = 'true' }

        context 'when the challenge token is missing' do
          before do
            post '/votes', { candidate_id: 1, nonce: 'some_nonce' }.to_json, { 'CONTENT_TYPE' => 'application/json' }
          end

          it 'returns an error response', :aggregate_failures do
            expect(last_response.status).to eq(400)
            expect(JSON.parse(last_response.body)).to include('status' => 'error')
            expect(JSON.parse(last_response.body)).to include('message' => 'Challenge token is required')
          end
        end

        context 'when the nonce is missing' do
          before do
            post '/votes', { candidate_id: 1, challenge_token: 'some_token' }.to_json,
                 { 'CONTENT_TYPE' => 'application/json' }
          end

          it 'returns an error response', :aggregate_failures do
            expect(last_response.status).to eq(400)
            expect(JSON.parse(last_response.body)).to include('status' => 'error')
            expect(JSON.parse(last_response.body)).to include('message' => 'Nonce is required')
          end
        end

        context 'when the challenge token does not exist in Redis' do
          before do
            post '/votes', { candidate_id: 1, challenge_token: 'invalid_token', nonce: 'some_nonce' }.to_json,
                 { 'CONTENT_TYPE' => 'application/json' }
          end

          it 'returns an error response', :aggregate_failures do
            expect(last_response.status).to eq(400)
            expect(JSON.parse(last_response.body)).to include('status' => 'error')
            expect(JSON.parse(last_response.body)).to include('message' => 'Invalid or expired challenge')
          end
        end

        context 'when the proof of work hash does not meet the difficulty target' do
          before do
            REDIS.set(RedisKeys.challenge('valid_token'), 'some_value')
            post '/votes', { candidate_id: 1, challenge_token: 'valid_token', nonce: 'wrong_nonce' }.to_json,
                 { 'CONTENT_TYPE' => 'application/json' }
          end

          it 'returns an error response', :aggregate_failures do
            expect(last_response.status).to eq(400)
            expect(JSON.parse(last_response.body)).to include('status' => 'error')
            expect(JSON.parse(last_response.body)).to include('message' => 'Invalid or expired challenge')
          end
        end
      end
    end
  end

  describe 'GET /poll' do
    before do
      get '/poll'
    end

    it 'returns the current poll information', :aggregate_failures do
      expect(last_response.status).to eq(200)
      response_body = JSON.parse(last_response.body)
      expect(response_body).to include('status' => 'success')
      expect(response_body).to include('poll_id' => '42')
      expect(response_body).to include('status_value' => 'active')
      expect(response_body).to include('candidates' => { '1' => 'João', '2' => 'Maria', '3' => nil })
    end
  end

  describe 'GET /health' do
    before do
      get '/health'
    end

    it 'returns a health check response', :aggregate_failures do
      expect(last_response.status).to eq(200)
      expect(JSON.parse(last_response.body)).to include('status' => 'ok')
    end
  end

  describe 'GET /votes/challenge' do
    before do
      get '/votes/challenge'
    end

    it 'returns a challenge token', :aggregate_failures do
      expect(last_response.status).to eq(200)
      response_body = JSON.parse(last_response.body)
      expect(response_body).to include('status' => 'success')
      expect(response_body).to include('challenge')
    end
  end

  describe 'GET /votes/summary/:poll_id' do
    let(:poll_id) { '42' }

    before do
      REDIS.set(RedisKeys.poll_current_status, 'active')
      REDIS.set(RedisKeys.poll_current_poll_id, poll_id)
      REDIS.set(RedisKeys.poll_current_started_at, Time.now.to_i)
      REDIS.sadd(RedisKeys.poll_current_candidates, [1, 2])
      REDIS.hset(RedisKeys.poll_current_candidate_names, 1, 'João')
      REDIS.hset(RedisKeys.poll_current_candidate_names, 2, 'Maria')

      100.times { REDIS.incr(RedisKeys.votes_total(poll_id, 1)) }
      50.times { REDIS.incr(RedisKeys.votes_total(poll_id, 2)) }

      100.times { REDIS.incr(RedisKeys.votes_hourly(poll_id, 1, Time.now.to_i)) }
      50.times { REDIS.incr(RedisKeys.votes_hourly(poll_id, 2, Time.now.to_i)) }

      get "/votes/summary/#{poll_id}"
    end

    it 'returns the vote summary', :aggregate_failures do
      expect(last_response.status).to eq(200)
      response_body = JSON.parse(last_response.body)

      expect(response_body).to include('status' => 'success')
      expect(response_body).to include('poll_id' => poll_id)
      expect(response_body['total_votes']).to eq(150)

      expect(response_body['hourly_votes']).to include(
        '1' => { 'name' => 'João', 'hourly_votes' => { Time.now.to_i.to_s => 100 } },
        '2' => { 'name' => 'Maria', 'hourly_votes' => { Time.now.to_i.to_s => 50 } }
      )

      expect(response_body['summary']).to include(
        '1' => { 'name' => 'João', 'total_votes' => 100 },
        '2' => { 'name' => 'Maria', 'total_votes' => 50 }
      )
    end
  end

  describe 'GET /metrics' do
    before do
      get '/metrics'
    end

    it 'returns Prometheus metrics', :aggregate_failures do
      expect(last_response.status).to eq(200)
      expect(last_response.body).to include('http_requests_total')
      expect(last_response.body).to include('http_request_duration_seconds')
      expect(last_response.body).to include('votes_total')
    end
  end

  describe 'OPTIONS requests' do
    it 'returns 204 for OPTIONS requests' do
      options '/votes'
      expect(last_response.status).to eq(204)
    end
  end
end
