require "bundler/inline"

gemfile true do
  source "https://rubygems.org"
  gem "ruby_event_store", "~> 2.16"
  gem "ostruct"
end

require_relative "dcb_event_store"
require_relative "test"

# based on https://dcb.events/examples/prevent-record-duplication/

# event type definitions:

class OrderPlaced < RubyEventStore::Event
  def self.tags =
    ->(event) do
      [
        "order:#{event.data[:order_id]}",
        "idempotency:#{event.data[:idempotency_token]}"
      ]
    end
end

# commands
PlaceOrder = Data.define(:order_id, :idempotency_token)

# projections for decision models:

def idempotency_token_was_used_projection(idempotency_token)
  RubyEventStore::Projection
    .from_stream("idempotency:#{idempotency_token}")
    .init(-> { { result: false } })
    .when(OrderPlaced, ->(state, _) { state[:result] = true })
end

# command handlers:

class Api
  Error = Class.new(StandardError)

  def initialize(event_store)
    @event_store = event_store
  end

  def call(command)
    method_name =
      command.class.to_s.gsub(/([a-z\d])([A-Z])/, '\1_\2').downcase.to_sym
    execution = method(method_name)
    execution.call(**command.to_h)
  end

  def buildDecisionModel(**projections)
    model =
      projections.reduce(OpenStruct.new) do |state, (key, projection)|
        state[key] = @event_store.execute(projection).fetch(:result)
        state
      end
    query =
      @event_store
        .read
        .stream(projections.values.map(&:streams).uniq)
        .of_type(projections.values.map(&:handled_events).uniq)
    append_condition = query.last&.event_id
    [model, query, append_condition]
  end

  def place_order(order_id:, idempotency_token:)
    state, query, append_condition =
      buildDecisionModel(
        idempotency_token_was_used:
          idempotency_token_was_used_projection(idempotency_token)
      )
    raise Error, "Re-submission" if state.idempotency_token_was_used

    @event_store.append(
      OrderPlaced.new(
        data: {
          order_id: order_id,
          idempotency_token: idempotency_token
        }
      ),
      query,
      append_condition
    )
  end
end

# test cases:

Test
  .new("Place order with previously used idempotency token")
  .given(
    OrderPlaced.new(data: { order_id: "o12345", idempotency_token: "11111" })
  )
  .when(PlaceOrder.new(order_id: "o54321", idempotency_token: "11111"))
  .expect_error("Re-submission")
  .run

Test
  .new("Place order with new idempotency token")
  .given(
    OrderPlaced.new(data: { order_id: "o12345", idempotency_token: "11111" })
  )
  .when(PlaceOrder.new(order_id: "o54321", idempotency_token: "22222"))
  .expect_event(
    OrderPlaced.new(data: { order_id: "o54321", idempotency_token: "22222" })
  )
  .run
