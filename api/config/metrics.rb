# frozen_string_literal: true

require 'cgi'
require 'prometheus/client'
require 'prometheus/client/data_stores/direct_file_store'
require 'prometheus/client/formats/text'

Prometheus::Client.config.data_store = Prometheus::Client::DataStores::DirectFileStore.new(
  dir: ENV.fetch('PROMETHEUS_DIR', '/tmp/prometheus_metrics')
)

VOTES_COUNTER = Prometheus::Client.registry.counter(
  :votes_total, docstring: 'Total votes accepted by the API', labels: [:candidate_id]
)

HTTP_REQUESTS_TOTAL = Prometheus::Client.registry.counter(
  :http_requests_total, docstring: 'Total HTTP requests', labels: %i[method path status]
)

HTTP_REQUEST_DURATION = Prometheus::Client.registry.histogram(
  :http_request_duration_seconds, docstring: 'HTTP request duration in seconds',
                                  labels: %i[method path], buckets: [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5]
)
