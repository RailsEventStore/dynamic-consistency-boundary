require "bundler/inline"

gemfile true do
  source "https://rubygems.org"
  gem "ruby_event_store", "~> 2.16"
  gem "ostruct"
end

require_relative "dcb_event_store"
require_relative "test"

def days_ago(days)
  seconds = 24 * 60 * 60
  Time.now - (days * seconds)
end

# based on https://dcb.events/examples/unique-username/

# event type definitions:

class AccountRegistered < RubyEventStore::Event
  def self.tags = ->(event) { "username:#{event.data[:username]}" }
end

class AccountClosed < RubyEventStore::Event
  def self.tags = ->(event) { "username:#{event.data[:username]}" }
end

class UsernameChanged < RubyEventStore::Event
  def self.tags =
    ->(event) do
      [
        "username:#{event.data[:old_username]}",
        "username:#{event.data[:new_username]}"
      ]
    end
end

# commands
RegisterAccount = Data.define(:username)

# projections for decision models:

def is_username_claimed_projection(username)
  RubyEventStore::Projection
    .from_stream("username:#{username}")
    .init(-> { { result: false } })
    .when(AccountRegistered, ->(state, _) { state[:result] = true })
    .when(
      AccountClosed,
      ->(state, event) { state[:result] = event.timestamp >= days_ago(3) }
    )
    .when(
      UsernameChanged,
      ->(state, event) do
        state[:result] = event.data[:new_username] == username ||
          event.timestamp <= days_ago(3)
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

  def register_account(username:)
    state, query, append_condition =
      buildDecisionModel(
        is_username_claimed: is_username_claimed_projection(username)
      )

    if (state.is_username_claimed)
      raise Error, "Username #{username} is claimed"
    end

    @event_store.append(
      AccountRegistered.new(data: { username: username }),
      query,
      append_condition
    )
  end
end

# test cases:

# Feature 1: Globally unique username

Test
  .new("Register account with claimed username")
  .given(AccountRegistered.new(data: { username: "u1" }))
  .when(RegisterAccount.new(username: "u1"))
  .expect_error("Username u1 is claimed")
  .run

Test
  .new("Register account with unused username")
  .when(RegisterAccount.new(username: "u1"))
  .expect_event(AccountRegistered.new(data: { username: "u1" }))
  .run

# Feature 2: Release usernames

Test
  .new("Register account with username of closed account")
  .given(
    AccountRegistered.new(data: { username: "u1" }),
    AccountClosed.new(data: { username: "u1" })
  )
  .when(RegisterAccount.new(username: "u1"))
  .expect_event(AccountRegistered.new(data: { username: "u1" }))
  .run

# Feature 3: Allow changing of usernames

Test
  .new(
    "Register account with a username that was previously used and then changed"
  )
  .given(
    AccountRegistered.new(data: { username: "u1" }),
    UsernameChanged.new(data: { old_username: "u1", new_username: "u1changed" })
  )
  .when(RegisterAccount.new(username: "u1"))
  .expect_event(AccountRegistered.new(data: { username: "u1" }))
  .run

Test
  .new("Register account with a username that another username was changed to")
  .given(
    AccountRegistered.new(data: { username: "u1" }),
    UsernameChanged.new(data: { old_username: "u1", new_username: "u1changed" })
  )
  .when(RegisterAccount.new(username: "u1changed"))
  .expect_error("Username u1changed is claimed")
  .run

# Feature 4: Username retention

Test
  .new("Register username of closed account before retention period")
  .given(
    AccountRegistered.new(
      data: {
        username: "u1"
      },
      metadata: {
        timestamp: days_ago(4)
      }
    ),
    AccountClosed.new(
      data: {
        username: "u1"
      },
      metadata: {
        timestamp: days_ago(3)
      }
    )
  )
  .when(RegisterAccount.new(username: "u1"))
  .expect_error("Username u1 is claimed")
  .run

Test
  .new("Register changed username before retention period")
  .given(
    AccountRegistered.new(
      data: {
        username: "u1"
      },
      metadata: {
        timestamp: days_ago(4)
      }
    ),
    UsernameChanged.new(
      data: {
        old_username: "u1",
        new_username: "u1changed"
      },
      metadata: {
        timestamp: days_ago(3)
      }
    )
  )
  .when(RegisterAccount.new(username: "u1changed"))
  .expect_error("Username u1 is claimed")
  .run

Test
  .new("Register username of closed account after retention period")
  .given(
    AccountRegistered.new(
      data: {
        username: "u1"
      },
      metadata: {
        timestamp: days_ago(4)
      }
    ),
    AccountClosed.new(
      data: {
        username: "u1"
      },
      metadata: {
        timestamp: days_ago(4)
      }
    )
  )
  .when(RegisterAccount.new(username: "u1"))
  .expect_event(AccountRegistered.new(data: { username: "u1" }))
  .run

Test
  .new("Register changed username after retention period")
  .given(
    AccountRegistered.new(
      data: {
        username: "u1"
      },
      metadata: {
        timestamp: days_ago(4)
      }
    ),
    UsernameChanged.new(
      data: {
        old_username: "u1",
        new_username: "u1changed"
      },
      metadata: {
        timestamp: days_ago(4)
      }
    )
  )
  .when(RegisterAccount.new(username: "u1changed"))
  .expect_event(AccountRegistered.new(data: { username: "u1" }))
  .run
