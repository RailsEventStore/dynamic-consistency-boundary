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

# based on https://dcb.events/examples/opt-in-token/

# event type definitions:

class SignUpInitiated < RubyEventStore::Event
  def self.tags =
    ->(event) do
      ["email:#{event.data[:email_address]}", "opt:#{event.data[:otp]}"]
    end
end

class SignUpConfirmed < RubyEventStore::Event
  def self.tags =
    ->(event) do
      ["email:#{event.data[:email_address]}", "opt:#{event.data[:otp]}"]
    end
end

# commands
ConfirmSignUp = Data.define(:email_address, :otp)

# projections for decision models:

def pending_sign_up_projection(email_address, otp)
  RubyEventStore::Projection
    .from_stream(["email:#{email_address}", "otp:#{otp}"])
    .init(-> { { result: nil } })
    .when(
      SignUpInitiated,
      ->(state, event) do
        state[:result] = OpenStruct.new(
          event.data.merge(
            otp_used: false,
            otp_expired: event.timestamp <= minutes_ago(60)
          )
        )
      end
    )
    .when(SignUpConfirmed, ->(state, event) { state[:result].otp_used = true })
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

  def confirm_sign_up(email_address:, otp:)
    state, query, append_condition =
      buildDecisionModel(
        pending_sign_up: pending_sign_up_projection(email_address, otp)
      )
    unless state.pending_sign_up
      raise Error, "No pending sign-up for this OTP / email address"
    end
    raise Error, "OTP was already used" if state.pending_sign_up.otp_used
    raise Error, "OTP expired" if state.pending_sign_up.otp_expired

    @event_store.append(
      SignUpConfirmed.new(
        data: {
          email_address: email_address,
          otp: otp,
          name: state.pending_sign_up.name
        }
      ),
      query,
      append_condition
    )
  end
end

# test cases:

# Feature 1: Simple One-Time Password (OTP)

Test
  .new("Confirm SignUp for non-existing OTP")
  .when(ConfirmSignUp.new(email_address: "john.doe@example.com", otp: "000000"))
  .expect_error("No pending sign-up for this OTP / email address")
  .run

Test
  .new("Confirm SignUp for OTP assigned to different email address")
  .given(
    SignUpInitiated.new(
      data: {
        email_address: "john.doe@example.com",
        otp: "111111",
        name: "John Doe"
      }
    )
  )
  .when(ConfirmSignUp.new(email_address: "jane.doe@example.com", otp: "111111"))
  .expect_error("No pending sign-up for this OTP / email address")
  .run

Test
  .new("Confirm SignUp for already used OTP")
  .given(
    SignUpInitiated.new(
      data: {
        email_address: "john.doe@example.com",
        otp: "222222",
        name: "John Doe"
      }
    ),
    SignUpConfirmed.new(
      data: {
        email_address: "john.doe@example.com",
        otp: "222222",
        name: "John Doe"
      }
    )
  )
  .when(ConfirmSignUp.new(email_address: "john.doe@example.com", otp: "222222"))
  .expect_error("OTP was already used")
  .run

Test
  .new("Confirm SignUp for valid OTP")
  .given(
    SignUpInitiated.new(
      data: {
        email_address: "john.doe@example.com",
        otp: "444444",
        name: "John Doe"
      }
    )
  )
  .when(ConfirmSignUp.new(email_address: "john.doe@example.com", otp: "444444"))
  .expect_event(
    SignUpConfirmed.new(
      data: {
        email_address: "john.doe@example.com",
        otp: "444444",
        name: "John Doe"
      }
    )
  )
  .run

# Feature 2: Expiring OTP

Test
  .new("Confirm SignUp for expired OTP")
  .given(
    SignUpInitiated.new(
      data: {
        email_address: "john.doe@example.com",
        otp: "333333",
        name: "John Doe"
      },
      metadata: {
        timestamp: minutes_ago(61)
      }
    )
  )
  .when(ConfirmSignUp.new(email_address: "john.doe@example.com", otp: "333333"))
  .expect_error("OTP expired")
  .run
