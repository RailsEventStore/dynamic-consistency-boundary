require "bundler/inline"

gemfile true do
  source "https://rubygems.org"
  gem "ruby_event_store", "~> 2.16"
  gem "ruby_event_store-active_record", "~> 2.16"
  gem "aggregate_root", "~> 2.16"
  gem "ostruct"
  gem "activerecord"
  gem "pg"
end

require_relative "dcb_event_store"

ActiveRecord::Base.establish_connection(
  adapter: "postgresql",
  host: "127.0.0.1",
  database: "postgres"
)
begin
  ActiveRecord::Base.connection.drop_database("dcb")
rescue StandardError
  nil
end
ActiveRecord::Base.connection.create_database("dcb")

ActiveRecord::Base.establish_connection(
  adapter: "postgresql",
  host: "127.0.0.1",
  database: "dcb"
)
ActiveRecord::Base.logger = nil
ActiveRecord::Schema.define do
  enable_extension "pg_catalog.plpgsql"

  create_table(:event_store_events, id: :bigserial, force: false) do |t|
    t.references :event, null: false, type: :uuid, index: { unique: true }
    t.string :event_type, null: false, index: true
    t.jsonb :metadata
    t.jsonb :data, null: false
    t.datetime :created_at,
               null: false,
               type: :timestamp,
               precision: 6,
               index: true
    t.datetime :valid_at,
               null: true,
               type: :timestamp,
               precision: 6,
               index: true
  end

  create_table(
    :event_store_events_in_streams,
    id: :bigserial,
    force: false
  ) do |t|
    t.string :stream, null: false
    t.integer :position, null: true
    t.references :event,
                 null: false,
                 type: :uuid,
                 index: true,
                 foreign_key: {
                   to_table: :event_store_events,
                   primary_key: :event_id
                 }
    t.datetime :created_at,
               null: false,
               type: :timestamp,
               precision: 6,
               index: true

    t.index %i[stream position], unique: true
    t.index %i[stream event_id], unique: true
  end
end

def setup_event_store
  DcbEventStore.new(
    RubyEventStore::Client.new(
      mapper:
        RubyEventStore::Mappers::PipelineMapper.new(
          RubyEventStore::Mappers::Pipeline.new(
            {
              Symbol => {
                serializer: ->(v) { v.to_s },
                deserializer: ->(v) { v.to_sym }
              },
              Time => {
                serializer: ->(v) do
                  v.iso8601(RubyEventStore::TIMESTAMP_PRECISION)
                end,
                deserializer: ->(v) { Time.iso8601(v) }
              },
              ActiveSupport::TimeWithZone => {
                serializer: ->(v) do
                  v.iso8601(RubyEventStore::TIMESTAMP_PRECISION)
                end,
                deserializer: ->(v) { Time.iso8601(v).in_time_zone },
                stored_type: ->(*) { "ActiveSupport::TimeWithZone" }
              },
              Date => {
                serializer: ->(v) { v.iso8601 },
                deserializer: ->(v) { Date.iso8601(v) }
              },
              DateTime => {
                serializer: ->(v) { v.iso8601 },
                deserializer: ->(v) { DateTime.iso8601(v) }
              },
              BigDecimal => {
                serializer: ->(v) { v.to_s },
                deserializer: ->(v) { BigDecimal(v) }
              }
            }.merge(
              if defined?(OpenStruct)
                {
                  OpenStruct => {
                    serializer: ->(v) { v.to_h },
                    deserializer: ->(v) { OpenStruct.new(v) }
                  }
                }
              else
                {}
              end
            )
              .reduce(
                RubyEventStore::Mappers::Transformation::PreserveTypes.new
              ) do |preserve_types, (klass, options)|
                preserve_types.register(klass, **options)
              end,
            RubyEventStore::Mappers::Transformation::SymbolizeMetadataKeys.new
          )
        ),
      repository:
        RubyEventStore::ActiveRecord::EventRepository.new(serializer: JSON)
    )
  )
end
