class DcbEventStore
  def initialize(store = RubyEventStore::Client.new)
    @store = store
  end

  def execute(projection)
    projection.run(@store)
  end

  def append(event_or_events, query = nil, append_condition = nil)
    events = Array(event_or_events)
    if (append_condition && query&.last&.event_id != append_condition)
      raise RubyEventStore::WrongExpectedEventVersion
    end
    @store.append(events)
    events.each do |event|
      Array(event.class.tags.call(event)).each do |tag|
        @store.link(
          event.event_id,
          stream_name: tag,
          expected_version: expected_version(append_condition, tag)
        )
      end
    end
  end

  def read
    @store.read
  end

  private

  def expected_version(append_condition, stream_name)
    return :auto unless append_condition

    @store.position_in_stream(append_condition, stream_name)
  rescue RubyEventStore::EventNotFoundInStream
    :auto
  end
end
