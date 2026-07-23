# frozen_string_literal: true

require 'digest'
require_relative 'config/redis_keys'

class VoteValidator
  Result = Struct.new(:valid?, :status, :message, :poll_id)

  def initialize(redis)
    @redis = redis
  end

  def call(candidate_id:, challenge_token:, nonce:, poll_id:)
    return Result.new(false, 400, 'Candidate_id is required') unless candidate_id

    # só ignora PoW fora de produção
    skip_pow = ENV['SKIP_PROOF_OF_WORK'] == 'true' &&
               (ENV['APP_ENV'] || ENV['RACK_ENV'] || 'development') != 'production'
    return Result.new(false, 400, 'Challenge token is required') unless challenge_token || skip_pow
    return Result.new(false, 400, 'Nonce is required') unless nonce || skip_pow
    return Result.new(false, 400, 'Invalid or expired challenge') unless valid_proof_of_work?(challenge_token, nonce,
                                                                                              skip_pow)

    status_value, candidate_is_member = @redis.pipelined do |pipeline|
      pipeline.get(RedisKeys.poll_current_status(poll_id))
      pipeline.sismember(RedisKeys.poll_current_candidates(poll_id), candidate_id)
    end

    return Result.new(false, 403, 'Poll is not active') unless status_value == 'active'
    return Result.new(false, 400, 'Candidate does not exist') unless candidate_is_member

    Result.new(true, nil, nil, poll_id)
  end

  private

  def valid_proof_of_work?(challenge_token, nonce, skip_pow)
    return true if skip_pow
    return false unless challenge_token && nonce

    # garante uso único: DEL retorna 1 se a chave existia (challenge válido e ainda não usado)
    return false unless @redis.del(RedisKeys.challenge(challenge_token)) == 1

    Digest::SHA256.hexdigest("#{challenge_token}#{nonce}").start_with?('0000')
  end
end
