#!/usr/bin/env ruby

require 'bundler'
Bundler.require

class MpdConsumer
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
      { name: 'is_cool',    type: %w[boolean null], default: false },

      # Field not present in writer schema. Avro::IO::DatumReader
      # defaults to using schema used by writer, but can be overridden
      # in call to .new (see below)
      { name: 'is_awesome', type: 'boolean',        default: true },

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

  def initialize(topic: TOPIC, group_id: 'mpd-consumers', client_id: 'mpd1', seed_brokers: ['localhost:9092'])
    @kafka = Kafka.new(
      seed_brokers: seed_brokers,
      client_id: client_id,
      logger: nil # Logger.new($stderr)
    )

    @consumer = @kafka.consumer(group_id: group_id, offset_commit_threshold: 0)

    @consumer.subscribe(topic)
  end

  def consume
    begin
      @consumer.each_message do |msg|
        p msg.partition
        data = from_avro(msg)
        p data # always an array
        p data.first
        puts
      end
    rescue RuntimeError => e # for multiple consumers of a single group/topic/partition, allows backup consumer if one dies
      p e
      retry
    end
  end

  private

  def from_avro(msg)
    buffer = StringIO.new(msg.value)

    # Use schema received from writer to read data
    # reader = Avro::DataFile::Reader.new(buffer, Avro::IO::DatumReader.new)

    # Use explicit (probably updated) schema for reading. Need to be compatible with
    # writer schema to be a valid read.
    reader = Avro::DataFile::Reader.new(buffer, Avro::IO::DatumReader.new(nil, Avro::Schema.parse(SCHEMA)))

    # either
    # reader.each do |datum|
    #   p datum
    # end
    # or just spit it into an array
    reader.to_a
  end
end

group_id = ARGV[0] || 'mpd-consumers'
consumer = MpdConsumer.new(group_id: group_id)

consumer.consume # loops forever
