require "bundler/inline"

gemfile true do
  source "https://rubygems.org"
  gem "ruby_event_store", "~> 2.16"
  gem "ostruct"
end

require_relative "dcb_event_store"
require_relative "api"
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

class PreventRecordDuplication
  include Api

  def place_order(order_id:, idempotency_token:)
    state, query, append_condition =
      buildDecisionModel(
        idempotency_token_was_used:
          idempotency_token_was_used_projection(idempotency_token)
      )
    raise Error, "Re-submission" if state.idempotency_token_was_used

    store.append(
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
  .run(PreventRecordDuplication.new)

Test
  .new("Place order with new idempotency token")
  .given(
    OrderPlaced.new(data: { order_id: "o12345", idempotency_token: "11111" })
  )
  .when(PlaceOrder.new(order_id: "o54321", idempotency_token: "22222"))
  .expect_event(
    OrderPlaced.new(data: { order_id: "o54321", idempotency_token: "22222" })
  )
  .run(PreventRecordDuplication.new)
