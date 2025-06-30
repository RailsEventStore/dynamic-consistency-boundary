require_relative "setup"
require_relative "api"
require_relative "test"

def days_ago(days)
  (Date.today - days).to_time
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
RegisterAccountWithRetentionPeriod = Data.define(:username)

# projections for decision models:

def is_username_claimed_projection(username)
  RubyEventStore::Projection
    .from_stream("username:#{username}")
    .init(-> { { result: false } })
    .when(AccountRegistered, ->(state, _) { state[:result] = true })
    .when(AccountClosed, ->(state, event) { state[:result] = false })
    .when(
      UsernameChanged,
      ->(state, event) do
        state[:result] = event.data[:new_username] == username
      end
    )
end

def is_username_claimed_with_retention_period_projection(username)
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
          event.timestamp >= days_ago(3)
      end
    )
end

# command handlers:

class UniqueUsername
  include Api

  def register_account(username:)
    state, query, append_condition =
      buildDecisionModel(
        is_username_claimed: is_username_claimed_projection(username)
      )

    if (state.is_username_claimed)
      raise Error, "Username #{username} is claimed"
    end

    store.append(
      AccountRegistered.new(data: { username: username }),
      query,
      append_condition
    )
  end

  def register_account_with_retention_period(username:)
    state, query, append_condition =
      buildDecisionModel(
        is_username_claimed:
          is_username_claimed_with_retention_period_projection(username)
      )

    if (state.is_username_claimed)
      raise Error, "Username #{username} is claimed"
    end

    store.append(
      AccountRegistered.new(data: { username: username }),
      query,
      append_condition
    )
  end
end

# test cases:
event_store = setup_event_store

# Feature 1: Globally unique username

Test
  .new("Register account with claimed username")
  .given(AccountRegistered.new(data: { username: "u1" }))
  .when(RegisterAccount.new(username: "u1"))
  .expect_error("Username u1 is claimed")
  .run(UniqueUsername.new(event_store))

Test
  .new("Register account with unused username")
  .when(RegisterAccount.new(username: "u1"))
  .expect_event(AccountRegistered.new(data: { username: "u1" }))
  .run(UniqueUsername.new(event_store))

# Feature 2: Release usernames

Test
  .new("Register account with username of closed account")
  .given(
    AccountRegistered.new(data: { username: "u1" }),
    AccountClosed.new(data: { username: "u1" })
  )
  .when(RegisterAccount.new(username: "u1"))
  .expect_event(AccountRegistered.new(data: { username: "u1" }))
  .run(UniqueUsername.new(event_store))

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
  .run(UniqueUsername.new(event_store))

Test
  .new("Register account with a username that another username was changed to")
  .given(
    AccountRegistered.new(data: { username: "u1" }),
    UsernameChanged.new(data: { old_username: "u1", new_username: "u1changed" })
  )
  .when(RegisterAccount.new(username: "u1changed"))
  .expect_error("Username u1changed is claimed")
  .run(UniqueUsername.new(event_store))

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
  .when(RegisterAccountWithRetentionPeriod.new(username: "u1"))
  .expect_error("Username u1 is claimed")
  .run(UniqueUsername.new(event_store))

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
  .when(RegisterAccountWithRetentionPeriod.new(username: "u1"))
  .expect_error("Username u1 is claimed")
  .run(UniqueUsername.new(event_store))

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
  .when(RegisterAccountWithRetentionPeriod.new(username: "u1"))
  .expect_event(AccountRegistered.new(data: { username: "u1" }))
  .run(UniqueUsername.new(event_store))

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
  .when(RegisterAccountWithRetentionPeriod.new(username: "u1"))
  .expect_event(AccountRegistered.new(data: { username: "u1" }))
  .run(UniqueUsername.new(event_store))
