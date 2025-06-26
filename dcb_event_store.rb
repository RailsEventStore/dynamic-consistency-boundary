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
        @store.link(event.event_id, stream_name: tag)
      end
    end
  end

  def read
    @store.read
  end
end
