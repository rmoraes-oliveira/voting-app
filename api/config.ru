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
  candidates = redis.smembers(RedisKeys.poll_current_candidates(poll_id))
  return false if candidates.empty?

  candidates.any? { |c| redis.get(RedisKeys.votes_total(poll_id, c)).nil? }
end

active_poll_ids = redis.smembers(RedisKeys.polls_active)

if active_poll_ids.empty?
  warn '[boot] Redis sem estado de votação — nenhuma votação ativa configurada ainda.'
else
  polls_with_missing_counters = active_poll_ids.select { |poll_id| counters_missing?(redis, poll_id) }

  if polls_with_missing_counters.any? && redis.set('boot:reconcile:lock', '1', nx: true, ex: 60)
    missing = polls_with_missing_counters.join(', ')
    warn "[boot] Polls configurados mas contadores ausentes (#{missing}) — reconstruindo a partir do Kafka..."
    polls_with_missing_counters.each { |poll_id| VoteReconciler.rebuild_from_kafka!(redis, poll_id) }
    warn '[boot] Reconciliação concluída.'
  end
end

map '/admin' do
  run AdminAPI
end

map '/' do
  run VotingAPI
end
