module Api
  Error = Class.new(StandardError)

  def initialize(event_store = DcbEventStore.new)
    @store = event_store
  end
  attr_reader :store

  def call(command)
    method_name =
      command.class.to_s.gsub(/([a-z\d])([A-Z])/, '\1_\2').downcase.to_sym
    execution = method(method_name)
    execution.call(**command.to_h)
  end

  def buildDecisionModel(**projections)
    model =
      projections.reduce(OpenStruct.new) do |state, (key, projection)|
        state[key] = store.execute(projection).fetch(:result)
        state
      end
    types = projections.values.flat_map(&:handled_events).uniq
    query = store.read.of_type(types)
    streams = projections.values.flat_map(&:streams).uniq
    query = query.stream(streams) unless streams.empty?
    append_condition = query.last&.event_id
    [model, query, append_condition]
  end

  def reset!
    klasses = RubyEventStore::ActiveRecord::WithDefaultModels.new.call
    klasses.reverse.each(&:delete_all)
  end
end
