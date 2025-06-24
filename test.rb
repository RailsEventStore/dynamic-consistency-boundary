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
    api.store.append(events) unless events&.empty?
    api.call(command)
    check_expected_event(api.store.read.last)
  rescue Api::Error => e
    check_expected_error(e)
  rescue => e
    fail(e.message)
  end

  private

  def pass(message = nil)
    log("PASS", description, (message && "Expected error:"), message)
  end

  def fail(message = nil)
    log("FAIL", description, (message && "Error:"), message)
  end

  def log(*values)
    puts values.join(" ")
  end

  def check_expected_event(actual_event)
    if expected_event && actual_event.instance_of?(expected_event.class) &&
         actual_event.data == expected_event.data
      pass
    else
      fail
    end
  end

  def check_expected_error(actual_error)
    if !expected_event && actual_error.message == expected_error
      pass(expected_error)
    else
      fail(actual_error.message)
    end
  end
end
