# frozen_string_literal: true

# Ponto único das chaves Redis usadas pelo sistema.
module RedisKeys
  # Chaves da "poll corrente" são namespaced por poll_id para permitir múltiplas
  # polls ativas simultaneamente, cada uma com seu próprio conjunto de chaves.
  def self.poll_current_status(poll_id)
    "poll:current:status:#{poll_id}"
  end

  def self.poll_current_poll_id(poll_id)
    "poll:current:poll_id:#{poll_id}"
  end

  def self.poll_current_candidates(poll_id)
    "poll:current:candidates:#{poll_id}"
  end

  def self.poll_current_candidate_names(poll_id)
    "poll:current:candidate_names:#{poll_id}"
  end

  def self.poll_current_started_at(poll_id)
    "poll:current:started_at:#{poll_id}"
  end

  def self.poll_current_ended_at(poll_id)
    "poll:current:ended_at:#{poll_id}"
  end

  def self.polls_active
    'polls:active'
  end

  def self.poll_results(poll_id)
    "poll:results:#{poll_id}"
  end

  def self.poll_results_index
    'poll:results:index'
  end

  def self.votes_total(poll_id, candidate_id)
    "votes:total:#{poll_id}:#{candidate_id}"
  end

  def self.votes_hourly(poll_id, candidate_id, hour_key)
    "votes:hourly:#{poll_id}:#{candidate_id}:#{hour_key}"
  end

  def self.votes_hourly_prefix(poll_id, candidate_id)
    "votes:hourly:#{poll_id}:#{candidate_id}:"
  end

  def self.votes_dedup(event_id)
    "votes:dedup:#{event_id}"
  end

  def self.votes_audit
    'votes:audit'
  end

  def self.challenge(token)
    "challenge:#{token}"
  end
end
