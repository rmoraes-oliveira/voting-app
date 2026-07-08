# frozen_string_literal: true

# Helpers de request/response compartilhados entre VotingAPI e AdminAPI
module HttpHelpers
  def parsed_request_body
    request.body.rewind
    JSON.parse(request.body.read)
  rescue JSON::ParserError
    halt 400, response_error('Invalid JSON format')
  end

  def response_error(message)
    { status: 'error', message: message }.to_json
  end
end
