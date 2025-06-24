require "bundler/inline"

gemfile true do
  source "https://rubygems.org"
  gem "ruby_event_store", "~> 2.16"
  gem "ostruct"
end

# based on https://dcb.events/examples/course-subscriptions/

# event type definitions:

class CourseDefined < RubyEventStore::Event
  def self.tags = ->(event) { "course:#{event.data[:course_id]}" }
end

class CourseCapacityChanged < RubyEventStore::Event
  def self.tags = ->(event) { "course:#{event.data[:course_id]}" }
end

class StudentSubscribedToCourse < RubyEventStore::Event
  def self.tags =
    ->(event) do
      ["student:#{event.data[:student_id]}", "course:#{event.data[:course_id]}"]
    end
end

# projections for decision models:

def course_exists_projection(course_id)
  RubyEventStore::Projection
    .from_stream("course:#{course_id}")
    .init(-> { { result: false } })
    .when(CourseDefined, ->(state, _) { state[:result] = true })
end

def course_capacity_projection(course_id)
  RubyEventStore::Projection
    .from_stream("course:#{course_id}")
    .init(-> { { result: 0 } })
    .when(
      CourseDefined,
      ->(state, event) { state[:result] = event.data.fetch(:capacity) }
    )
    .when(
      CourseCapacityChanged,
      ->(state, event) { state[:result] = event.data.fetch(:new_capacity) }
    )
end

def student_already_subscribed_projection(student_id, course_id)
  RubyEventStore::Projection
    .from_stream(["student:#{student_id}"])
    .init(-> { { result: false } })
    .when(
      StudentSubscribedToCourse,
      ->(state, event) do
        state[:result] |= event.data.fetch(:course_id) == course_id
      end
    )
end

def number_of_course_subscriptions_projection(course_id)
  RubyEventStore::Projection
    .from_stream("course:#{course_id}")
    .init(-> { { result: 0 } })
    .when(StudentSubscribedToCourse, ->(state, _) { state[:result] += 1 })
end

def number_of_student_subscriptions_projection(student_id)
  RubyEventStore::Projection
    .from_stream("student:#{student_id}")
    .init(-> { { result: 0 } })
    .when(StudentSubscribedToCourse, ->(state, _) { state[:result] += 1 })
end

# command handlers:

class Api
  Error = Class.new(StandardError)

  def initialize(event_store)
    @event_store = event_store
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

  def define_course(course_id:, capacity:)
    state, query, append_condition =
      buildDecisionModel(course_exists: course_exists_projection(course_id))

    if (state.course_exists)
      raise Error, "Course with id #{course_id} already exists"
    end

    @event_store.append(
      CourseDefined.new(data: { course_id: course_id, capacity: capacity }),
      query,
      append_condition
    )
  end

  def changeCourseCapacity(course_id:, new_capacity:)
    state, query, append_condition =
      buildDecisionModel(
        course_exists: course_exists_projection(course_id),
        course_capacity: course_capacity_projection(course_id)
      )

    raise Error, "Course #{course_id} does not exist" if (!state.course_exists)
    if (state.course_capacity === new_capacity)
      raise Error,
            "New capacity #{new_capacity} is the same as the current capacity"
    end

    @event_store.append(
      CourseCapacityChanged.new(
        data: {
          course_id: course_id,
          new_capacity: new_capacity
        }
      ),
      query,
      append_condition
    )
  end

  def subscribeStudentToCourse(course_id:, student_id:)
    state, query, append_condition =
      buildDecisionModel(
        course_exists: course_exists_projection(course_id),
        course_capacity: course_capacity_projection(course_id),
        number_of_course_subscriptions:
          number_of_course_subscriptions_projection(course_id),
        number_of_student_subscriptions:
          number_of_student_subscriptions_projection(student_id),
        student_already_subscribed:
          student_already_subscribed_projection(student_id, course_id)
      )

    raise Error, "Course #{course_id} does not exist" if (!state.course_exists)
    if (state.number_of_course_subscriptions >= state.course_capacity)
      raise Error, "Course #{course_id} is already fully booked"
    end
    if (state.student_already_subscribed)
      raise Error, "Student already subscribed to this course"
    end
    if (state.number_of_student_subscriptions >= 5)
      raise Error, "Student already subscribed to 5 courses"
    end

    @event_store.append(
      StudentSubscribedToCourse.new(
        data: {
          student_id: student_id,
          course_id: course_id
        }
      ),
      query,
      append_condition
    )
  end
end

class DcbEventStore
  def initialize
    @store = RubyEventStore::Client.new
  end

  def execute(projection)
    projection.run(@store)
  end

  def append(event_or_events, query = nil, append_condition = nil)
    events = Array(event_or_events)
    if (append_condition && query&.last&.event_id != append_condition)
      raise RubyEventStore::WrongExpectedEventVersion
    end
    @store.append(events)
    events.each do |event|
      Array(event.class.tags.call(event)).each do |tag|
        @store.link(event.event_id, stream_name: tag)
      end
    end
  end

  def read
    @store.read
  end
end

# test utilities

class Test
  def self.run(
    description,
    given: nil,
    expected_error: nil,
    expected_event: nil,
    &block
  )
    puts description
    store = DcbEventStore.new
    store.append(given) if given
    block.call(Api.new(store))
    last_event = store.read.last
    puts(
      if expected_event && last_event.class == expected_event.class &&
           last_event.data === expected_event.data
        "OK"
      else
        "FAIL"
      end
    )
  rescue Api::Error => e
    if !expected_event && e.message == expected_error
      puts "OK. Expected error: #{expected_error}"
    else
      puts "FAIL. Error: #{e.message}"
    end
  rescue => e
    puts "FAIL. Unexpected error: #{e.message}"
  end
end

# test cases:

Test.run(
  "Define course with existing id",
  given: [CourseDefined.new(data: { course_id: "c1", capacity: 10 })],
  expected_error: "Course with id c1 already exists"
) { |api| api.define_course(course_id: "c1", capacity: 15) }

Test.run(
  "Define course with new id",
  expected_event: CourseDefined.new(data: { course_id: "c1", capacity: 15 })
) { |api| api.define_course(course_id: "c1", capacity: 15) }

Test.run(
  "Change capacity of a non-existing course",
  expected_error: "Course c0 does not exist"
) { |api| api.changeCourseCapacity(course_id: "c0", new_capacity: 15) }

Test.run(
  "Change capacity of a course to a new value",
  given: [CourseDefined.new(data: { course_id: "c1", capacity: 12 })],
  expected_event:
    CourseCapacityChanged.new(data: { course_id: "c1", new_capacity: 15 })
) { |api| api.changeCourseCapacity(course_id: "c1", new_capacity: 15) }

Test.run(
  "Subscribe student to non-existing course",
  expected_error: "Course c0 does not exist"
) { |api| api.subscribeStudentToCourse(student_id: "s1", course_id: "c0") }

Test.run(
  "Subscribe student to fully booked course",
  given: [
    CourseDefined.new(data: { course_id: "c1", capacity: 3 }),
    StudentSubscribedToCourse.new(data: { student_id: "s1", course_id: "c1" }),
    StudentSubscribedToCourse.new(data: { student_id: "s2", course_id: "c1" }),
    StudentSubscribedToCourse.new(data: { student_id: "s3", course_id: "c1" })
  ],
  expected_error: "Course c1 is already fully booked"
) { |api| api.subscribeStudentToCourse(student_id: "s4", course_id: "c1") }

Test.run(
  "Subscribe student to the same course twice",
  given: [
    CourseDefined.new(data: { course_id: "c1", capacity: 10 }),
    StudentSubscribedToCourse.new(data: { student_id: "s1", course_id: "c1" })
  ],
  expected_error: "Student already subscribed to this course"
) { |api| api.subscribeStudentToCourse(student_id: "s1", course_id: "c1") }

Test.run(
  "Subscribe student to more than 5 courses",
  given: [
    CourseDefined.new(data: { course_id: "c6", capacity: 10 }),
    StudentSubscribedToCourse.new(data: { student_id: "s1", course_id: "c1" }),
    StudentSubscribedToCourse.new(data: { student_id: "s1", course_id: "c2" }),
    StudentSubscribedToCourse.new(data: { student_id: "s1", course_id: "c3" }),
    StudentSubscribedToCourse.new(data: { student_id: "s1", course_id: "c4" }),
    StudentSubscribedToCourse.new(data: { student_id: "s1", course_id: "c5" })
  ],
  expected_error: "Student already subscribed to 5 courses"
) { |api| api.subscribeStudentToCourse(student_id: "s1", course_id: "c6") }

Test.run(
  "Subscribe student to course with capacity",
  given: [CourseDefined.new(data: { course_id: "c1", capacity: 10 })],
  expected_event:
    StudentSubscribedToCourse.new(data: { student_id: "s1", course_id: "c1" })
) { |api| api.subscribeStudentToCourse(student_id: "s1", course_id: "c1") }
