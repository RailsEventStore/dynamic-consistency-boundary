require_relative "setup"

# based on https://dcb.events/examples/event-sourced-aggregate/

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

# commands
DefineCourse = Data.define(:course_id, :capacity)
ChangeCourseCapacity = Data.define(:course_id, :new_capacity)
SubscribeStudentToCourse = Data.define(:course_id, :student_id)

class CourseAggregate
  include AggregateRoot

  def initialize
    @number_of_subscriptions = 0
  end

  def create(id, title, capacity)
    apply CourseDefined.new(
            data: {
              course_id: id,
              title: title,
              capacity: capacity
            }
          )
  end

  def change_capacity(new_capacity)
    if new_capacity == capacity
      raise Error,
            "Course #{course_id} already has a capacity of #{new_capacity}"
    end
    if new_capacity < number_of_subscriptions
      raise Error,
            "Course #{course_id} already has #{number_of_subscriptions} active subscriptions, can't set the capacity below that"
    end

    apply CourseCapacityChanged.new(
            data: {
              course_id: course_id,
              new_capacity: new_capacity
            }
          )
  end

  def subscribe_student(student_id)
    if number_of_subscriptions == capacity
      raise Error, "Course #{course_id} is already fully booked"
    end
    apply StudentSubscribedToCourse.new(
            data: {
              student_id: student_id,
              course_id: course_id
            }
          )
  end

  attr_reader :course_id

  on CourseDefined do |event|
    @course_id = event.data[:course_id]
    @title = event.data[:title]
    @capacity = event.data[:capacity]
  end

  on CourseCapacityChanged do |event|
    @capacity = event.data[:new_capacity]
  end

  on StudentSubscribedToCourse do |_event|
    @number_of_subscriptions += 1
  end

  private

  attr_reader :title, :capacity, :number_of_subscriptions
end

# DCB flavored repository

class DcbCourseRepository
  def initialize(event_store)
    @event_store = event_store
  end

  def load(course_id)
    stream_name = "course:#{course_id}"
    aggregate = CourseAggregate.new
    query(stream_name).reduce { |_, ev| aggregate.apply(ev) }
    aggregate.version = aggregate.unpublished_events.to_a.last.event_id
    aggregate
  end

  def save(aggregate)
    stream_name = "course:#{aggregate.course_id}"
    event_store.append(
      aggregate.unpublished_events.to_a,
      query(stream_name),
      aggregate.version == -1 ? nil : aggregate.version
    )
    aggregate.version = aggregate.unpublished_events.to_a.last.event_id
  end

  private

  attr_reader :event_store

  def query(stream_name)
    event_store.read.stream(stream_name)
  end
end

event_store = setup_event_store
# create and save a new instance:
repository = DcbCourseRepository.new(event_store)
course = CourseAggregate.new
course.create("c1", "Course 01", 10)
repository.save(course)

# update an existing instance:
course2 = repository.load("c1")
course2.change_capacity(15)
repository.save(course2)

# read data here
puts "Stored events:\n---\n"
event_store
  .read
  .stream("course:c1")
  .each do |event|
    puts [event.class, event.timestamp, event.data].map(&:inspect).join("\n")
    puts "---\n"
  end
