require "bundler/inline"

gemfile true do
  source "https://rubygems.org"
  gem "ruby_event_store", "~> 2.16"
  gem "ostruct"
end

require_relative "dcb_event_store"
require_relative "test"

# based on https://dcb.events/examples/invoice-number/

# event type definitions:

class InvoiceCreated < RubyEventStore::Event
  def self.tags = ->(event) { "invoice:#{event.data[:invoice_number]}" }
end

# commands
CreateInvoice = Data.define(:invoice_data)
ImprovedCreateInvoice = Data.define(:invoice_data)

# projections for decision models:

def next_invoice_number_projection
  RubyEventStore::Projection
    .from_all_streams
    .init(-> { { result: 1 } })
    .when(InvoiceCreated, ->(state, _) { state[:result] += 1 })
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

  def create_invoice(invoice_data:)
    state, query, append_condition =
      buildDecisionModel(next_invoice_number: next_invoice_number_projection)

    @event_store.append(
      InvoiceCreated.new(
        data: {
          invoice_number: state.next_invoice_number,
          invoice_data: invoice_data
        }
      ),
      query,
      append_condition
    )
  end

  def improved_create_invoice(invoice_data:)
    query = @event_store.read.of_type(InvoiceCreated)
    last_event = query.last
    next_invoice_number =
      last_event ? last_event.data.fetch(:invoice_number) + 1 : 1
    append_condition = last_event&.event_id

    @event_store.append(
      InvoiceCreated.new(
        data: {
          invoice_number: next_invoice_number,
          invoice_data: invoice_data
        }
      ),
      query,
      append_condition
    )
  end
end

# test cases:

Test
  .new("Create first invoice")
  .when(CreateInvoice.new(invoice_data: { foo: :bar }))
  .expect_event(
    InvoiceCreated.new(data: { invoice_number: 1, invoice_data: { foo: :bar } })
  )
  .run

Test
  .new("Create second invoice")
  .given(
    InvoiceCreated.new(data: { invoice_number: 1, invoice_data: { foo: :bar } })
  )
  .when(CreateInvoice.new(invoice_data: { bar: :baz }))
  .expect_event(
    InvoiceCreated.new(data: { invoice_number: 2, invoice_data: { bar: :baz } })
  )
  .run

# Better performance

Test
  .new("Create first invoice")
  .when(ImprovedCreateInvoice.new(invoice_data: { foo: :bar }))
  .expect_event(
    InvoiceCreated.new(data: { invoice_number: 1, invoice_data: { foo: :bar } })
  )
  .run

Test
  .new("Create second invoice")
  .given(
    InvoiceCreated.new(data: { invoice_number: 1, invoice_data: { foo: :bar } })
  )
  .when(ImprovedCreateInvoice.new(invoice_data: { bar: :baz }))
  .expect_event(
    InvoiceCreated.new(data: { invoice_number: 2, invoice_data: { bar: :baz } })
  )
  .run
