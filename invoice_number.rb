require "bundler/inline"

gemfile true do
  source "https://rubygems.org"
  gem "ruby_event_store", "~> 2.16"
  gem "ostruct"
end

require_relative "dcb_event_store"
require_relative "api"
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

class InvoiceNumber
  include Api

  def create_invoice(invoice_data:)
    state, query, append_condition =
      buildDecisionModel(next_invoice_number: next_invoice_number_projection)

    store.append(
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
    query = store.read.of_type(InvoiceCreated)
    last_event = query.last
    next_invoice_number =
      last_event ? last_event.data.fetch(:invoice_number) + 1 : 1
    append_condition = last_event&.event_id

    store.append(
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
  .run(InvoiceNumber.new)

Test
  .new("Create second invoice")
  .given(
    InvoiceCreated.new(data: { invoice_number: 1, invoice_data: { foo: :bar } })
  )
  .when(CreateInvoice.new(invoice_data: { bar: :baz }))
  .expect_event(
    InvoiceCreated.new(data: { invoice_number: 2, invoice_data: { bar: :baz } })
  )
  .run(InvoiceNumber.new)

# Better performance

Test
  .new("Create first invoice")
  .when(ImprovedCreateInvoice.new(invoice_data: { foo: :bar }))
  .expect_event(
    InvoiceCreated.new(data: { invoice_number: 1, invoice_data: { foo: :bar } })
  )
  .run(InvoiceNumber.new)

Test
  .new("Create second invoice")
  .given(
    InvoiceCreated.new(data: { invoice_number: 1, invoice_data: { foo: :bar } })
  )
  .when(ImprovedCreateInvoice.new(invoice_data: { bar: :baz }))
  .expect_event(
    InvoiceCreated.new(data: { invoice_number: 2, invoice_data: { bar: :baz } })
  )
  .run(InvoiceNumber.new)
