# frozen_string_literal: true

RSpec.describe 'Prometheus metrics' do
  let(:candidate_id) { 'metrics_spec_candidate' }
  let(:path) { '/metrics_spec_path' }

  before do
    VOTES_COUNTER.increment(labels: { candidate_id: candidate_id })
    HTTP_REQUESTS_TOTAL.increment(labels: { method: 'GET', path: path, status: 200 })
    HTTP_REQUEST_DURATION.observe(0.01, labels: { method: 'GET', path: path })
  end

  it 'marshals the registry into Prometheus text format', :aggregate_failures do
    output = Prometheus::Client::Formats::Text.marshal(Prometheus::Client.registry)

    expect(output).to match(/votes_total\{candidate_id="#{candidate_id}"\} \d+(\.\d+)?/)
    expect(output).to match(/http_requests_total\{method="GET",path="#{Regexp.escape(path)}",status="200"\} \d+(\.\d+)?/)
    expect(output).to include('http_request_duration_seconds')
  end
end
