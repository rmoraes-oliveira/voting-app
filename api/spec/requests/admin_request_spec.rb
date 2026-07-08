# frozen_string_literal: true

RSpec.describe 'Admin Request' do
  def app
    AdminAPI
  end

  describe 'POST /poll/start' do
    def start_poll
      post '/poll/start', { poll_id: 42, candidates: candidates }.to_json,
           { 'CONTENT_TYPE' => 'application/json', 'HTTP_X_API_KEY' => ENV.fetch('ADMIN_API_KEY', nil) }
    end

    context 'when there is no active poll' do
      context 'when the poll is started successfully' do
        before { start_poll }

        let(:candidates) { [{ id: 1, name: 'João' }, { id: 2, name: 'Maria' }] }

        it 'starts a new poll and returns a success response', :aggregate_failures do
          expect(last_response.status).to eq(200)
          response_body = JSON.parse(last_response.body)
          expect(response_body).to include('status' => 'success')
          expect(response_body).to include('poll_id' => 42)
        end

        it 'persists the poll data in Redis', :aggregate_failures do
          expect(REDIS.get(RedisKeys.poll_current_status)).to eq('active')
          expect(REDIS.get(RedisKeys.poll_current_poll_id)).to eq('42')
          expect(REDIS.smembers(RedisKeys.poll_current_candidates)).to contain_exactly('1', '2')
          expect(REDIS.hget(RedisKeys.poll_current_candidate_names, 1)).to eq('João')
          expect(REDIS.hget(RedisKeys.poll_current_candidate_names, 2)).to eq('Maria')
        end
      end

      context 'when the request payload is invalid' do
        before { start_poll }
        let(:candidates) { [{ id: 1, name: 'João' }, { id: 2 }] } # Missing name for candidate 2

        it 'returns an error response indicating invalid payload', :aggregate_failures do
          expect(last_response.status).to eq(400)
          response_body = JSON.parse(last_response.body)
          expect(response_body).to include('status' => 'error')
          expect(response_body).to include('message' => 'Each candidate requires id and name (string or integer)')
        end
      end
    end

    context 'when the poll is already active' do
      let(:candidates) { [{ id: 1, name: 'João' }, { id: 2, name: 'Maria' }] }

      before do
        REDIS.set(RedisKeys.poll_current_status, 'active')
        start_poll
      end

      it 'returns an error response indicating the poll is already active', :aggregate_failures do
        expect(last_response.status).to eq(400)
        response_body = JSON.parse(last_response.body)
        expect(response_body).to include('status' => 'error')
        expect(response_body).to include('message' => 'Poll is already active')
      end
    end
  end

  describe 'POST /poll/stop' do
    context 'when the poll is active' do
      before do
        allow(KafkaLag).to receive(:total_lag).and_return(0)

        REDIS.set(RedisKeys.poll_current_status, 'active')
        REDIS.set(RedisKeys.poll_current_poll_id, '42')
        post '/poll/stop', {}.to_json,
             { 'CONTENT_TYPE' => 'application/json', 'HTTP_X_API_KEY' => ENV.fetch('ADMIN_API_KEY', nil) }
      end

      it 'stops the current poll and returns a success response', :aggregate_failures do
        expect(last_response.status).to eq(200)
        response_body = JSON.parse(last_response.body)
        expect(response_body).to include('status' => 'success')
        expect(response_body).to include('message' => 'Poll stopped')
      end

      it 'updates the poll status in Redis', :aggregate_failures do
        expect(REDIS.get(RedisKeys.poll_current_status)).to eq('stopped')
        expect(REDIS.get(RedisKeys.poll_current_ended_at)).not_to be_nil
      end
    end

    context 'when the poll is not active' do
      before do
        REDIS.set(RedisKeys.poll_current_status, 'stopped')
        post '/poll/stop', {}.to_json,
             { 'CONTENT_TYPE' => 'application/json', 'HTTP_X_API_KEY' => ENV.fetch('ADMIN_API_KEY', nil) }
      end

      it 'returns an error response', :aggregate_failures do
        expect(last_response.status).to eq(400)
        response_body = JSON.parse(last_response.body)
        expect(response_body).to include('status' => 'error')
        expect(response_body).to include('message' => 'Poll is not active')
      end
    end

    context 'when there is lag in Kafka' do
      before do
        allow(KafkaLag).to receive(:total_lag).and_return(10)

        REDIS.set(RedisKeys.poll_current_status, 'active')
        REDIS.set(RedisKeys.poll_current_poll_id, '42')
        post '/poll/stop', {}.to_json,
             { 'CONTENT_TYPE' => 'application/json', 'HTTP_X_API_KEY' => ENV.fetch('ADMIN_API_KEY', nil) }
      end

      it 'returns an error response due to lag', :aggregate_failures do
        expect(last_response.status).to eq(423)
        response_body = JSON.parse(last_response.body)
        expect(response_body).to include('status' => 'error')
        expect(response_body).to include('message' => 'Votes still being processed, try again shortly')
        expect(response_body).to include('lag' => 10)
      end
    end
  end

  describe 'Unauthorized access' do
    it 'returns 401 for unauthorized requests', :aggregate_failures do
      post '/poll/start', { poll_id: 42, candidates: [{ id: 1, name: 'João' }] }.to_json,
           { 'CONTENT_TYPE' => 'application/json', 'HTTP_X_API_KEY' => 'invalid_key' }
      expect(last_response.status).to eq(401)
      response_body = JSON.parse(last_response.body)
      expect(response_body).to include('status' => 'error')
      expect(response_body).to include('message' => 'Unauthorized')
    end
  end

  describe 'GET /poll/results/:poll_id' do
    let(:poll_id) { '42' }
    let(:snapshot) do
      { 'poll_id' => poll_id, 'started_at' => 1000, 'ended_at' => 2000, 'results' => { '1' => 10, '2' => 20 } }
    end

    before do
      REDIS.set(RedisKeys.poll_results(poll_id), snapshot.to_json)
      get "/poll/results/#{poll_id}", {}, { 'HTTP_X_API_KEY' => ENV.fetch('ADMIN_API_KEY', nil) }
    end

    it 'returns the stored poll results snapshot for the given poll_id', :aggregate_failures do
      expect(last_response.status).to eq(200)
      expect(JSON.parse(last_response.body)).to eq(snapshot)
    end
  end

  describe 'GET /poll/results' do
    let(:poll_id_1) { '42' }
    let(:poll_id_2) { '43' }
    let(:snapshot_1) do
      { 'poll_id' => poll_id_1, 'started_at' => 1000, 'ended_at' => 2000, 'results' => { '1' => 10, '2' => 20 } }
    end
    let(:snapshot_2) do
      { 'poll_id' => poll_id_2, 'started_at' => 3000, 'ended_at' => 4000, 'results' => { '1' => 5, '2' => 15 } }
    end

    before do
      REDIS.set(RedisKeys.poll_results(poll_id_1), snapshot_1.to_json)
      REDIS.set(RedisKeys.poll_results(poll_id_2), snapshot_2.to_json)
      REDIS.zadd(RedisKeys.poll_results_index, snapshot_1['ended_at'], poll_id_1)
      REDIS.zadd(RedisKeys.poll_results_index, snapshot_2['ended_at'], poll_id_2)
      get '/poll/results', {}, { 'HTTP_X_API_KEY' => ENV.fetch('ADMIN_API_KEY', nil) }
    end

    it 'returns a list of all stored poll results snapshots', :aggregate_failures do
      expect(last_response.status).to eq(200)
      response_body = JSON.parse(last_response.body)
      expect(response_body).to include('status' => 'success', 'count' => 2)
      expect(response_body['results']).to include(snapshot_1, snapshot_2)
    end
  end
end
