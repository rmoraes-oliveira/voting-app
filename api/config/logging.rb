# frozen_string_literal: true

require 'logger'
require 'fileutils'

LOG_DIR = ENV.fetch('LOG_DIR', '/app/log')
FileUtils.mkdir_p(LOG_DIR)
LOG_FILE = File.open(File.join(LOG_DIR, 'app.log'), File::WRONLY | File::APPEND | File::CREAT)
LOG_FILE.sync = true

APP_LOGGER = Logger.new(LOG_FILE)
APP_LOGGER.level = Logger.const_get(ENV.fetch('LOG_LEVEL', 'INFO').upcase)
APP_LOGGER.formatter = proc do |severity, datetime, _progname, msg|
  "#{{ timestamp: datetime.iso8601, severity: severity, message: msg }.to_json}\n"
end
