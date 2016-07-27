require 'rethinkdb'

module RethinkDB
  # Extend this class to define a new way of running rethinkdb queries
  # asynchronously
  class AsyncHandler
    # The callback is set to be the user defined callback by the #async_run method
    attr_accessor :callback
    attr_accessor :connection
    attr_accessor :options

    # This method is called with a block that runs a rethinkdb connection
    # synchronously
    def run(&action)
      raise "Must override AsyncHandler#run"
    end

    # This method should return a handler that will deal with incoming messages
    def handler
      raise "Must override AsyncHandler#handler"
    end
  end

  # AsyncHandler that uses EventMachine to dispatch events
  class EMHandler < AsyncHandler
    def run
      if !EM.reactor_running?
        raise RuntimeError, "RethinkDB::RQL::em_run can only be called inside `EM.run`"
      end

      EM_Guard.register(connection)
      options[:query_handle_class] = EMQueryHandle
      yield
    end

    def handler
      callback
    end
  end

  class QueryHandle
    # Override this method with an async dispatch, making sure that
    # when the block is run @closed == false
    def guarded_async_run(&b)
      raise "Must override QueryHandle#guarded_async_run"
    end

    def callback(res)
      begin
        if @handler.stopped?
          @closed = true
          @conn.stop(@token)
          return
        elsif res
          is_cfeed = (res['n'] & [Response::ResponseNote::SEQUENCE_FEED,
                                  Response::ResponseNote::ATOM_FEED,
                                  Response::ResponseNote::ORDER_BY_LIMIT_FEED,
                                  Response::ResponseNote::UNIONED_FEED]) != []

          case res['t']
          when Response::ResponseType::SUCCESS_PARTIAL,
               Response::ResponseType::SUCCESS_SEQUENCE
            guarded_async_run do
              handle_open
              if res['t'] == Response::ResponseType::SUCCESS_PARTIAL
                @conn.register_query(@token, @all_opts, self) if !@conn.closed?
                @conn.dispatch([Query::QueryType::CONTINUE], @token) if !@conn.closed?
              end
              Shim.response_to_native(res, @msg, @all_opts).each do |row|
                if is_cfeed
                  if (row.has_key?('new_val') && row.has_key?('old_val') &&
                      @handler.respond_to?(:on_change))
                    handle(:on_change, row['old_val'], row['new_val'])
                  elsif (row.has_key?('new_val') && !row.has_key?('old_val') &&
                         @handler.respond_to?(:on_initial_val))
                    handle(:on_initial_val, row['new_val'])
                  elsif (row.has_key?('old_val') && !row.has_key?('new_val') &&
                         @handler.respond_to?(:on_uninitial_val))
                    handle(:on_uninitial_val, row['old_val'])
                  elsif row.has_key?('error') && @handler.respond_to?(:on_change_error)
                    handle(:on_change_error, row['error'])
                  elsif row.has_key?('state') && @handler.respond_to?(:on_state)
                    handle(:on_state, row['state'])
                  else
                    handle(:on_unhandled_change, row)
                  end
                else
                  handle(:on_stream_val, row)
                end
              end
              if res['t'] == Response::ResponseType::SUCCESS_SEQUENCE ||
                  @conn.closed?
                handle_close
              end
            end
          when Response::ResponseType::SUCCESS_ATOM
            guarded_async_run do
              return if @closed
              handle_open
              val = Shim.response_to_native(res, @msg, @all_opts)
              if val.is_a?(Array)
                handle(:on_array, val)
              else
                handle(:on_atom, val)
              end
              handle_close
            end
          when Response::ResponseType::WAIT_COMPLETE
            guarded_async_run do
              return if @closed
              handle_open
              handle(:on_wait_complete)
              handle_close
            end
          else
            exc = nil
            begin
              exc = Shim.response_to_native(res, @msg, @all_opts)
            rescue Exception => e
              exc = e
            end
            guarded_async_run do
              return if @closed
              handle_open
              handle(:on_error, e)
              handle_close
            end
          end
        else
          guarded_async_run {
            return if @closed
            handle_close
          }
        end
      rescue Exception => e
        guarded_async_run do
          return if @closed
          handle_open
          handle(:on_error, e)
          handle_close
        end
      end
    end
  end

  class EMQueryHandle < QueryHandle
    def guarded_async_run(&b)
      EM.next_tick {
        b.call if !@closed
      }
    end

    def callback(res)
      if !EM.reactor_running?
          @closed = true
          @conn.stop(@token)
          return
      end
      super(res)
    end
  end

  class RQL
    def parse(*args, &b)
      conn = nil
      opts = nil
      block = nil
      async_handler = nil
      args = args.map{|x| x.is_a?(Class) ? x.new : x}
      args.each {|arg|
        case arg
        when RethinkDB::Connection
          raise ArgumentError, "Unexpected second Connection #{arg.inspect}." if conn
          conn = arg
        when Hash
          raise ArgumentError, "Unexpected second Hash #{arg.inspect}." if opts
          opts = arg
        when Proc
          raise ArgumentError, "Unexpected second callback #{arg.inspect}." if block
          block = arg
        when Handler
          raise ArgumentError, "Unexpected second callback #{arg.inspect}." if block
          block = arg
        when AsyncHandler
          raise ArgumentError, "Unexpected second AsyncHandler #{arg.inspect}." if async_handler
          async_handler = arg
        else
          raise ArgumentError, "Unexpected argument #{arg.inspect} " +
            "(got #{args.inspect})."
        end
      }
      conn = @@default_conn if !conn
      opts = {} if !opts
      block = b if !block
      if (tf = opts[:time_format])
        opts[:time_format] = (tf = tf.to_s)
        if tf != 'raw' && tf != 'native'
          raise ArgumentError, "`time_format` must be 'raw' or 'native' (got `#{tf}`)."
        end
      end
      if (gf = opts[:group_format])
        opts[:group_format] = (gf = gf.to_s)
        if gf != 'raw' && gf != 'native'
          raise ArgumentError, "`group_format` must be 'raw' or 'native' (got `#{gf}`)."
        end
      end
      if (bf = opts[:binary_format])
        opts[:binary_format] = (bf = bf.to_s)
        if bf != 'raw' && bf != 'native'
          raise ArgumentError, "`binary_format` must be 'raw' or 'native' (got `#{bf}`)."
        end
      end
      if !conn
        raise ArgumentError, "No connection specified!\n" \
        "Use `query.run(conn)` or `conn.repl(); query.run`."
      end
      {conn: conn, opts: opts, block: block, async_handler: async_handler}
    end

    def em_run(*args, &b)
      async_run(*args, EMHandler, &b)
    end

    def async_run(*args, &b)
      unbound_if(@body == RQL)
      args = parse(*args, &b)
      if args[:block].is_a?(Proc)
        args[:block] = CallbackHandler.new(args[:block])
      end
      if !args[:block].is_a?(Handler)
        raise ArgumentError, "No handler specified."
      end

      async_handler = args[:async_handler]
      if !async_handler.is_a?(AsyncHandler)
        raise ArgumentError, "No async handler specified."
      end

      # If the user has defined the `on_state` method, we assume they want states.
      if args[:block].respond_to?(:on_state)
        args[:opts] = args[:opts].merge(include_states: true)
      end

      async_handler.callback = args[:block]
      async_handler.connection = args[:conn]
      async_handler.options = args[:opts]

      async_handler.run do
        async_handler.connection.run(@body, async_handler.options, async_handler.handler)
      end
    end
  end

  class Connection
    def run(msg, opts, b)
      query_handle_class = opts.delete(:query_handle_class) || QueryHandle
      reconnect(:noreply_wait => false) if @auto_reconnect && !is_open()
      raise ReqlRuntimeError, "Connection is closed." if !is_open()

      global_optargs = {}
      all_opts = @default_opts.merge(opts)
      if all_opts.keys.include?(:noreply)
        all_opts[:noreply] = !!all_opts[:noreply]
      end

      token = new_token
      q = [Query::QueryType::START,
           msg,
           Hash[all_opts.map {|k,v|
                  [k.to_s, (v.is_a?(RQL) ? v.to_pb : RQL.new.expr(v).to_pb)]
                }]]

      if b.is_a? Handler
        callback = query_handle_class.new(b, msg, all_opts, token, self)
        register_query(token, all_opts, callback)
        dispatch(q, token)
        return callback
      else
        res = run_internal(q, all_opts, token)
        return res if !res
        if res['t'] == Response::ResponseType::SUCCESS_PARTIAL
          value = Cursor.new(Shim.response_to_native(res, msg, opts),
                             msg, self, opts, token, true)
        elsif res['t'] == Response::ResponseType::SUCCESS_SEQUENCE
          value = Cursor.new(Shim.response_to_native(res, msg, opts),
                             msg, self, opts, token, false)
        else
          value = Shim.response_to_native(res, msg, opts)
        end

        if res['p']
          real_val = {
            "profile" => res['p'],
            "value" => value
          }
        else
          real_val = value
        end

        if b
          begin
            b.call(real_val)
          ensure
            value.close if value.is_a?(Cursor)
          end
        else
          real_val
        end
      end
    end
  end
end
