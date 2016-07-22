#!/usr/bin/env ruby

require 'bundler'
Bundler.require

class MpdProducer
  TOPIC = 'mpd-topic2'.freeze

  SCHEMA = {
    type: 'record',
    name: 'User',
    namespace: 'example.avro.mpd.User',
    fields: [
      { name: 'id',         type: 'int',            default: -1 },
      { name: 'last_name',  type: 'string',         default: '' },
      { name: 'first_name', type: 'string',         default: '' },
      { name: 'job',        type: 'string',         default: 'none' },

      # Union type that includes null makes field optional,
      # but will *not* use default value for values written with this
      # schema, if field is missing. The value will be null.
      # Also of note, the type of a default value for a union field
      # must be of the same type as the *first* member listed in the union.
      { name: 'is_cool',    type: %w[boolean null], default: false },

      { name: 'timestamp',  type: 'int',            default: 0 },
      {
        name: 'color',      type: ['null', {
          type: 'enum',
          name: 'Colors',
          symbols: %w[RED BLUE GREEN YELLOW WHITE BLACK]
        }], default: 'null'
      }
    ]
  }.to_json

  def initialize(client_id: 'mpd1', seed_brokers: ['localhost:9092'])
    @kafka = Kafka.new(
      seed_brokers: seed_brokers,
      client_id: client_id
    )

    #@producer = @kafka.producer(
    #  max_buffer_size: 5_000,           # Allow at most 5K messages to be buffered.
    #  max_buffer_bytesize: 100_000_000, # Allow at most 100MB to be buffered.
    #  required_acks: :all, # all brokers must ack the message delivery. 0 for fire-and-forget, 1 for the lead broker
    #  max_retries: 3, # default 2
    #  retry_backoff: 5 # seconds to wait between retries. default 1
    #)
    # or
    # Async producers can be safely used in a multithreaded environment.
    @producer = @kafka.async_producer(
      #delivery_threshold: 100, # deliver at each 100 messages received
      #delivery_interval: 30, # every 30 seconds

      # Not required, and there are tradeoffs.
      # :gzip is other alternative, uses more CPU than Snappy but higher compression
      compression_codec: :snappy
    )

    do_at_exit(@producer)
  end

  def buffer_message(msg)
    writer << msg
  end

  def deliver_messages(topic: TOPIC, partition_key: ['poo', 'derp'].sample)
    @writer.close # Don't forget this!@
    result = @buffer.string

    @producer.produce(
      result,
      topic: topic,
      # partition_key: partition_key # caveat: multiple keys can hash to a single partition
      # partition: [0,1].sample
    )

    retry_count = 0
    begin
      # may deliver to multiple brokers
      @producer.deliver_messages # returns immediately with async_producer
    rescue Kafka::DeliveryFailed
      retry_count += 1
      retry if retry_count < 3

      # or just ignore, messages kept in buffer and will be retried on next deliver_messages call
    end

    @writer = nil
  end

  private

  def writer
    return @writer if @writer
    parsed_schema = Avro::Schema.parse(SCHEMA)
    d_writer = Avro::IO::DatumWriter.new(parsed_schema)
    @buffer = StringIO.new # Can be any IO object
    @writer = Avro::DataFile::Writer.new(@buffer, d_writer, parsed_schema)
  end

  def do_at_exit(producer)
    at_exit do
      # only for async_producer. Tries to deliver undelivered messages and blocks until acked or retries have run out(?)
      producer.shutdown
    end
  end
end



producer = MpdProducer.new

# Records must use string keys to match Avro schema
record = {
  'id'         => 666,
  'last_name'  => 'dungan',
  'first_name' => 'mike',
  'job'        => 'code monkey',
  'is_cool'    => false, # required on write side, even through default is set
  'timestamp'  => Time.now.utc.to_i,
  'color'      => 'RED'
}

record2 = {
  'id'         => 123,
  'last_name'  => 'darling',
  'first_name' => 'josie',
  'job'        => 'cat',
  # 'is_cool'    => true,
  'timestamp'  => Time.now.utc.to_i#,
  # 'color'      => 'BLUE'
}

producer.buffer_message(record)
producer.buffer_message(record2)
producer.deliver_messages

record3 = {
  'id'         => 777,
  'last_name'  => 'derp',
  'first_name' => 'jack',
  'job'        => 'cool guy',
  'is_cool'    => true,
  'timestamp'  => Time.now.utc.to_i,
  'color'      => 'BLACK'
}

producer.buffer_message(record3)
producer.deliver_messages

