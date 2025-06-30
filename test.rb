class Test
  def initialize(description)
    @description = description
  end

  def given(*events)
    @events = events
    self
  end

  def when(command)
    @command = command
    self
  end

  def expect_error(error)
    @expected_error = error
    self
  end

  def expect_event(event)
    @expected_event = event
    self
  end

  attr_reader :description, :events, :command, :expected_event, :expected_error

  def run(api)
    api.reset!
    api.store.append(events) unless events&.empty?
    api.call(command)
    if expected_error
      fail("Expected error not raised")
    else
      check_expected_event(api.store.read.last)
    end
  rescue Api::Error => e
    check_expected_error(e)
  rescue => e
    fail(e.message)
  end

  private

  def pass(message = nil)
    log("PASS", description, message && ":", message)
  end

  def fail(message = nil)
    log("FAIL", description, message && ":", message)
  end

  def log(*values)
    puts values.compact.join(" ")
  end

  def check_expected_event(actual_event)
    if expected_event && actual_event.instance_of?(expected_event.class) &&
         actual_event.data == expected_event.data
      pass("Event published as expected")
    else
      fail("Expected event not published")
    end
  end

  def check_expected_error(actual_error)
    if !expected_event && actual_error.message == expected_error
      pass("Expected error: " + expected_error)
    else
      fail("Unexpected error: " + actual_error.message)
    end
  end
end
