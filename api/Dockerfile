FROM ruby:3.2-slim

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
     build-essential \
     librdkafka-dev \
     libssl-dev \
     libffi-dev \
     git \
     curl \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Gemfile Gemfile.lock ./
RUN gem install bundler:4.0.10 && bundle install --jobs 4 --retry 3

COPY . .

EXPOSE 9292

CMD ["bundle", "exec", "falcon", "serve"]
