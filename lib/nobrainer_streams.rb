require "nobrainer_streams/version"
require "nobrainer_streams/rethinkdb_monkeypatch"

module NoBrainer::Streams
  extend ActiveSupport::Concern

  included do
    on_unsubscribe :stop_all_streams
  end

  private

  def stream_from(query, options = {}, callback = nil)
    if query.respond_to? :to_rql
      nobrainer_stream_from(query, options, callback)
    else
      super(query, options, callback)
    end
  end

  def stop_all_streams
    nobrainer_stop_all_streams
    super
  end

  def nobrainer_stream_from(query, options = {}, callback = nil)
    callback ||= -> (changes) do
      transmit changes, via: "streamed from #{query.inspect}"
    end

    deserialize = -> (changes) do
      klass = query.model
      old_val = changes['old_val']
      new_val = changes['new_val']
      changes['old_val'] = klass.new(old_val)  if old_val
      changes['new_val'] = klass.new(new_val)  if new_val
      callback.call(changes)
    end

    # defer_subscription_confirmation!
    connection = NoBrainer::Streams::streams_connection
    cursor = query.to_rql.changes(options).async_run(connection, ConcurrentAsyncHandler, &deserialize)
    nobrainer_cursors << cursor
  end

  def nobrainer_stop_all_streams
    nobrainer_cursors.each do |cursor|
      begin
        logger.info "Closing cursor: #{cursor.inspect}"
        cursor.close
      rescue => e
        logger.error "Could not close cursor: #{e.message}\n#{e.backtrace.join("\n")}"
      end
    end
  end
  
  def nobrainer_cursors
    @_nobrainer_cursors ||= []
  end

  def self.streams_connection
    @@streams_connection ||= NoBrainer::ConnectionManager.get_new_connection.raw
  end

  class ConcurrentAsyncHandler < RethinkDB::AsyncHandler
    def run(&action)
      options[:query_handle_class] = AsyncQueryHandler
      yield
    end

    def handler
      callback
    end

    class AsyncQueryHandler < RethinkDB::QueryHandle
      def guarded_async_run(&b)
        Concurrent.global_io_executor.post(&b)
      end
    end
  end
end
