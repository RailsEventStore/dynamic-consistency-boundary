require "bundler/inline"

gemfile true do
  source "https://rubygems.org"
  gem "ruby_event_store", "~> 2.16"
  gem "ostruct"
end

require_relative "dcb_event_store"
require_relative "test"

def minutes_ago(minutes)
  Time.now - (minutes * 60)
end

# based on https://dcb.events/examples/dynamic-product-price/

# event type definitions:

class ProductDefined < RubyEventStore::Event
  def self.tags = ->(event) { "product:#{event.data[:product_id]}" }
end

class ProductPriceChanged < RubyEventStore::Event
  def self.tags = ->(event) { "product:#{event.data[:product_id]}" }
end

class ProductOrdered < RubyEventStore::Event
  def self.tags = ->(event) { "product:#{event.data[:product_id]}" }
end

class MultipleProductsOrdered < RubyEventStore::Event
  def self.tags =
    ->(event) do
      event.data[:items].map { |item| "product:#{item[:product_id]}" }
    end
end

# commands
OrderProduct = Data.define(:product_id, :displayed_price)
OrderMultipleProducts = Data.define(:items)

# projections for decision models:

def product_price_projection(product_id)
  productPriceGracePeriod = minutes_ago(10)
  RubyEventStore::Projection
    .from_stream("product:#{product_id}")
    .init(
      -> do
        {
          result:
            OpenStruct.new(last_valid_old_price: nil, valid_new_prices: [])
        }
      end
    )
    .when(
      ProductDefined,
      ->(state, event) do
        if event.timestamp >= productPriceGracePeriod
          state[:result].last_valid_old_price = nil
          state[:result].valid_new_prices = [event.data.fetch(:price)]
        else
          state[:result].last_valid_old_price = event.data[:price]
          state[:result].valid_new_prices = []
        end
      end
    )
    .when(
      ProductPriceChanged,
      ->(state, event) do
        if event.timestamp >= productPriceGracePeriod
          state[:result].valid_new_prices << event.data.fetch(:new_price)
        else
          state[:result].last_valid_old_price = event.data[:new_price]
        end
      end
    )
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

  def order_product(product_id:, displayed_price:)
    state, query, append_condition =
      buildDecisionModel(product_price: product_price_projection(product_id))

    if (
         state.product_price.last_valid_old_price != displayed_price &&
           !state.product_price.valid_new_prices.include?(displayed_price)
       )
      raise Error, "invalid price for product #{product_id}"
    end

    @event_store.append(
      ProductOrdered.new(
        data: {
          product_id: product_id,
          price: displayed_price
        }
      ),
      query,
      append_condition
    )
  end

  def order_multiple_products(items:)
    state, query, append_condition =
      buildDecisionModel(
        **items
          .map do |item|
            [item[:product_id], product_price_projection(item[:product_id])]
          end
          .to_h
      )

    items.each do |item|
      product_price = state[item[:product_id]]

      if (
           product_price.last_valid_old_price != item[:displayed_price] &&
             !product_price.valid_new_prices.include?(item[:displayed_price])
         )
        raise Error, "invalid price for product #{item[:product_id]}"
      end
    end

    @event_store.append(
      MultipleProductsOrdered.new(
        data: {
          items:
            items.map do |item|
              { product_id: item[:product_id], price: item[:displayed_price] }
            end
        }
      ),
      query,
      append_condition
    )
  end
end

# test cases:

# Feature 1: Order single product

Test
  .new("Order product with invalid displayed price")
  .given(ProductDefined.new(data: { product_id: "p1", price: 123 }))
  .when(OrderProduct.new(product_id: "p1", displayed_price: 100))
  .expect_error("invalid price for product p1")
  .run

Test
  .new("Order product with valid displayed price")
  .given(ProductDefined.new(data: { product_id: "p1", price: 123 }))
  .when(OrderProduct.new(product_id: "p1", displayed_price: 123))
  .expect_event(ProductOrdered.new(data: { product_id: "p1", price: 123 }))
  .run

# Feature 2: Changing product prices

Test
  .new("Order product with a displayed price that was never valid")
  .given(
    ProductDefined.new(
      data: {
        product_id: "p1",
        price: 123
      },
      metadata: {
        timestamp: minutes_ago(20)
      }
    )
  )
  .when(OrderProduct.new(product_id: "p1", displayed_price: 100))
  .expect_error("invalid price for product p1")
  .run

Test
  .new("Order product with a price that was changed more than 10 minutes ago")
  .given(
    ProductDefined.new(
      data: {
        product_id: "p1",
        price: 123
      },
      metadata: {
        timestamp: minutes_ago(20)
      }
    ),
    ProductPriceChanged.new(
      data: {
        product_id: "p1",
        new_price: 134
      },
      metadata: {
        timestamp: minutes_ago(20)
      }
    )
  )
  .when(OrderProduct.new(product_id: "p1", displayed_price: 123))
  .expect_error("invalid price for product p1")
  .run

Test
  .new("Order product with initial valid price")
  .given(
    ProductDefined.new(
      data: {
        product_id: "p1",
        price: 123
      },
      metadata: {
        timestamp: minutes_ago(20)
      }
    )
  )
  .when(OrderProduct.new(product_id: "p1", displayed_price: 123))
  .expect_event(ProductOrdered.new(data: { product_id: "p1", price: 123 }))
  .run

Test
  .new("Order product with a price that was changed less than 10 minutes ago")
  .given(
    ProductDefined.new(
      data: {
        product_id: "p1",
        price: 123
      },
      metadata: {
        timestamp: minutes_ago(20)
      }
    ),
    ProductPriceChanged.new(
      data: {
        product_id: "p1",
        new_price: 134
      },
      metadata: {
        timestamp: minutes_ago(9)
      }
    )
  )
  .when(OrderProduct.new(product_id: "p1", displayed_price: 123))
  .expect_event(ProductOrdered.new(data: { product_id: "p1", price: 123 }))
  .run

Test
  .new("Order product with valid new price")
  .given(
    ProductDefined.new(
      data: {
        product_id: "p1",
        price: 123
      },
      metadata: {
        timestamp: minutes_ago(20)
      }
    ),
    ProductPriceChanged.new(
      data: {
        product_id: "p1",
        new_price: 134
      },
      metadata: {
        timestamp: minutes_ago(9)
      }
    )
  )
  .when(OrderProduct.new(product_id: "p1", displayed_price: 134))
  .expect_event(ProductOrdered.new(data: { product_id: "p1", price: 134 }))
  .run

# Feature 3: Multiple products (shopping cart)

Test
  .new("Multi: Order product with a displayed price that was never valid")
  .given(
    ProductDefined.new(
      data: {
        product_id: "p1",
        price: 123
      },
      metadata: {
        timestamp: minutes_ago(20)
      }
    )
  )
  .when(
    OrderMultipleProducts.new(
      items: [{ product_id: "p1", displayed_price: 100 }]
    )
  )
  .expect_error("invalid price for product p1")
  .run

Test
  .new(
    "Multi: Order product with a price that was changed more than 10 minutes ago"
  )
  .given(
    ProductDefined.new(
      data: {
        product_id: "p1",
        price: 123
      },
      metadata: {
        timestamp: minutes_ago(20)
      }
    ),
    ProductPriceChanged.new(
      data: {
        product_id: "p1",
        new_price: 134
      },
      metadata: {
        timestamp: minutes_ago(20)
      }
    )
  )
  .when(
    OrderMultipleProducts.new(
      items: [{ product_id: "p1", displayed_price: 123 }]
    )
  )
  .expect_error("invalid price for product p1")
  .run

Test
  .new("Multi: Order product with initial valid price")
  .given(
    ProductDefined.new(
      data: {
        product_id: "p1",
        price: 123
      },
      metadata: {
        timestamp: minutes_ago(20)
      }
    )
  )
  .when(
    OrderMultipleProducts.new(
      items: [{ product_id: "p1", displayed_price: 123 }]
    )
  )
  .expect_event(
    MultipleProductsOrdered.new(
      data: {
        items: [{ product_id: "p1", price: 123 }]
      }
    )
  )
  .run

Test
  .new(
    "Multi: Order product with a price that was changed less than 10 minutes ago"
  )
  .given(
    ProductDefined.new(
      data: {
        product_id: "p1",
        price: 123
      },
      metadata: {
        timestamp: minutes_ago(20)
      }
    ),
    ProductPriceChanged.new(
      data: {
        product_id: "p1",
        new_price: 134
      },
      metadata: {
        timestamp: minutes_ago(9)
      }
    )
  )
  .when(
    OrderMultipleProducts.new(
      items: [{ product_id: "p1", displayed_price: 123 }]
    )
  )
  .expect_event(
    MultipleProductsOrdered.new(
      data: {
        items: [{ product_id: "p1", price: 123 }]
      }
    )
  )
  .run

Test
  .new("Multi: Order product with valid new price")
  .given(
    ProductDefined.new(
      data: {
        product_id: "p1",
        price: 123
      },
      metadata: {
        timestamp: minutes_ago(20)
      }
    ),
    ProductPriceChanged.new(
      data: {
        product_id: "p1",
        new_price: 134
      },
      metadata: {
        timestamp: minutes_ago(9)
      }
    ),
    ProductDefined.new(
      data: {
        product_id: "p2",
        price: 321
      },
      metadata: {
        timestamp: minutes_ago(8)
      }
    )
  )
  .when(
    OrderMultipleProducts.new(
      items: [
        { product_id: "p1", displayed_price: 123 },
        { product_id: "p2", displayed_price: 321 }
      ]
    )
  )
  .expect_event(
    MultipleProductsOrdered.new(
      data: {
        items: [
          { product_id: "p1", price: 123 },
          { product_id: "p2", price: 321 }
        ]
      }
    )
  )
  .run
