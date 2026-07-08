# frozen_string_literal: true

require './app'
require './admin_app'
require './config/settings'
require './config/redis_keys'
require './config/vote_reconciler'

# =========================================================
# Boot check: se o Redis não tem estado de votação (ex: instância
# nova, ou perdeu dados após um crash), reconstrói os contadores
# a partir do histórico do tópico Kafka antes de aceitar requests.
# =========================================================
redis = Settings.redis_client

def counters_missing?(redis, poll_id)
  candidates = redis.smembers(RedisKeys.poll_current_candidates)
  return false if candidates.empty?

  candidates.any? { |c| redis.get(RedisKeys.votes_total(poll_id, c)).nil? }
end

poll_id = redis.get(RedisKeys.poll_current_poll_id)

if redis.get(RedisKeys.poll_current_status).nil?
  warn '[boot] Redis sem estado de votação — nenhuma votação configurada ainda.'
elsif counters_missing?(redis, poll_id)
  if redis.set('boot:reconcile:lock', '1', nx: true, ex: 60)
    warn '[boot] Poll configurado mas contadores ausentes — reconstruindo a partir do Kafka...'
    VoteReconciler.rebuild_from_kafka!(redis)
    warn '[boot] Reconciliação concluída.'
  end
end

map '/admin' do
  run AdminAPI
end

map '/' do
  run VotingAPI
end
