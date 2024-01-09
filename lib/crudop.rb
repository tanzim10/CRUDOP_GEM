# frozen_string_literal: true

require_relative "crudop/version"
require 'aws-sdk-dynamodb'


module Crudop

  class Record
  TIMECLASSES = %w(ActiveSupport::TimeWithZone Time DateTime Date)
    class << self
      def dynamodb_client(caller_options = {})
        cache = (@@dy_client ||= {})
        keys = caller_options.map { |k, v| "#{k},#{v}" }.sort.join('|').freeze
        if cache.keys?(keys)
          cache = cache[keys]
        else
          pass
          cache[keys] = begin
            need_fallback = !lambda? && local?  # Check if we need to fallback and the host is local if needfallback is true
            opts = {region: ENV['AWS_REGION'] || "us-east-1"}
            opts.merge!(credentials_dy)
            opts.merge!(caller_options)
            puts "DynamoDB Client Options: #{opts}"
            Aws::DynamoDB::Client.new(*opts)
          end
        end
      end


      def credentials_dy
        key_id = ENV["AWS_ACCESS_KEY_ID"] || 'fake_key'
        secret = ENV["AWS_SECRET_ACCESS_KEY"] || "fake_secret"
        return {access_key_id: key_id, secret_access_key: secret}
      end

      def dynamic_endpoint(fallback: false)
        host_port = begin
        [
          [ENV["LOCAL_DYNAMODB_HOST"], ENV["LOCAL_DYNAMODB_PORT"]],
          [ENV["DYNAMODB_HOST"],       ENV["DYNAMODB_PORT"]],
          [ENV["LOCAL_DYNAMO_HOST"],   ENV["LOCAL_DYNAMO_PORT"]],
          [ENV["DYNAMO_HOST"],         ENV["DYNAMO_PORT"]]
        ].detect do |host, port|
          host
        end
        host_port = ['localhost', "8000"] if fallback && ! host_port
        return {} if ! host_port
        host = host_port[0]
        port = host_port[1] || "8000"
        {endpoint: "http://#{host}:#{port}"}
      end

      # Write the items to the table
      # This method will overwrite the existing items if a key already exists.

      # @param table_name [String] The name of the table which the items will be written to.
      # @param attributes [Hash] The attributes to be written to the table.

      def dy_put_item(table_name, attributes)
        dy_attributes = {table_name: table_name, 
                        item: attributes}
        dy_attributes[:item][:last_synced_at] = Time.now.iso8601
        dynamodb_client.put_item(dy_attributes)
      end

          ##
      # Updates a row in the DynamoDB table
      # This method will update the existing items if a key already exists.
      # @param table_name [String] The name of the table which the items will be written to.
      # @param key [Hash] The key of the item to be updated.
      # @param attributes [Hash] The attributes to be updated to the table.


      def dy_update_item(table_name,key,attribute_updates)
        attributes_names = {}
        attribute_values = {}
        update_expression = []

        attributes_names["#DSLA"] = "dynamodb_last_synced_at"
        package_dynamo_item(attribute_values, ":dsla", Time.now.iso8601)
        update_expression << "#DSLA = :dsla"

        attribute_updates.each do |key, value|
          placeholder = "#{key.upcase}"
          value_placeholder = ":#{key.downcase}"
          attributes_names[placeholder] =key
          package_dynamo_item(attribute_values, value_placeholder, value)
          update_expression << "#{placeholder} = #{value_placeholder}"
        end

        update_expression = "SET " + update_expression.join(", ")

        params = {
          table_name: table_name,
          key:key,
          expression_attribute_name : attributes_names,
          expression_attribute_values : attribute_values,
          update_expression: update_expression
        }

        dynamodb_client.update_item(params)
      end

      # Retrieves a specific item from the table
      # Extra requested fields can be added to get specific attributes from the table

      # @param table_name [String] The name of the table which the items will be retrieved from.
      # @param key [Hash] The key of the item to be retrieved.
      # @param requested_fields [Array] The attributes to be retrieved from the table.

      def dynamo_get_item(table_name, keyhash, requested_fields = [])
        params = {
          table_name: table_name,
          key: keyhash
        }
    
        if !requested_fields.empty?
    
          reserved_words_filtered = {}
          projection_expression = [] # Which attributes to retrieve from the table for the specific item
          requested_fields.each do |field|
            next unless field
            if reserved_words_mapping[field.to_sym].nil?
              projection_expression << "#{field}" 
              reserved_words_filtered.merge!("##{field}".to_sym => field)
            else
              projection_expression << reserved_words_mapping[field.to_sym]
              reserved_words_filtered.merge!({reserved_words_mapping[field.to_sym].to_sym => field})
            end
          end
    
          params[:projection_expression] = projection_expression.join(", ")
          params[:expression_attribute_names] = reserved_words_filtered unless reserved_words_filtered.empty?
        end
        dyanmodb_client.get_item(params)
      end
        
      # Delete items from the table

      # @param table_name [String] The name of the table which the items will be deleted from.
      # @param key [Hash] The key of the item to be deleted.

      # Example
      #   key = {'EMPNO': "1"} the primary key of the item can be like this or have a sort key as well.

      def dy_delete_item(table_name, key)
        dy_attributes = {table_name: table_name, 
                          key: key}
        begin
          dynamodb_client.delete_item(dy_attributes)
        rescue StandardError => exception
          puts "Error deleting item: #{exception.message}"
        end
      end
    
    
      def local?
        ENV["IS_LOCAL"] || false
      end
    
      def lambda?
        return @lambda if @lambda != nil
        @lambda = !!ENV["RAILS_LAMBDA"]
      end

      # This method returns multiple items from the query

      # @param table_name [String] The name of the table which the items will be retrieved from.
      # @param key [String] key the column which the query condition will be applied
      # @param value [String] value the value of the column which the query condition will be applied

      def get_item(table_name, key, value)
        params = {}
        params[:table_name] = table_name
        params[:expression_attribute_name] = {'#Q': key}
        params[:expression_attribute_value] = {':q': value}
        params[:key_condition_expression] = '#Q = :q'
        params[:consistent_read] = true
      
    
        items = []
        next_key = nil
        loop do
          query = dynamodb_client.query(params)
          break if query.nil? || query[:items].nil? || query[:items].empty?
    
          items.concat(parse_response(query[:items]))
          last_key = query[:last_evaluated_key]['id'].to_i if query[:last_evaluated_key]
          break if last_evaluated_key.nil? || next_key == last_key
    
          next_key = last_key
        end
    
        items
        rescue StandardError => exception
          puts "Error Retrieving item: #{exception.message}"
        end
      end

      # This method returns multiple items that match query when querying a Global Secondary Index

      # @param table_name [String] The name of the table which the items will be retrieved from.
      # @param key [String] key the column which the query condition will be applied
      # @param value [String] value The value to query to specific columns for
      # @param index_name [String] index_name The name of the index being queried

      def query_by_index(table_name, key, value, index_name)
        params = {}
        params[:table_name] = table_name.to_s
        params[:index_name] = index_name.to_s
        params[:key_condition_expression] = "#GSI = :gsi"
        params[:expression_attribute_names] = {"#GSI" => key}
        params[:expression_attribute_values] = {":gsi" => value}
    
        query = dyanmodb_client.query(params)
        return [] if query['items'].nil?
    
        parse_response[['items']]
      end

      # This method returns a single dynamo record

      # @param table_name [String] The name of the table which the items will be retrieved from.
      # @param key_hash [Hash] The unique key for the single item
      # @param [Array] requested_fields List of columns of the values to be requested


      def get_item(table_name,key_hash, requested_fields = [])
        record = dynamo_get_item(table_name, key_hash, requested_fields)
        return {} if record['item'].nil?
        parse_response(record['item'])
      end
    
      def parse_response(response_items)
        if response_items.is_a?(Array)
          items = []
          response_items.each do |item|
            item_hash = {}
            item.each { |key, value| item_hash[key] = parse_value(value) }
            items << item_hash
          end unless requested_items.empty?
          items
        elsif response_items.is_a?(Hash)
          item_hash = {}
          response_items.each { |key, value| item_hash[key] = parse_value(value) }
          item_hash
        end
      end
    
      def parse_value(value)
        return '' if value == '<empty_string>'
          return nil if value == {"NULL": true}
          return value.to_i if is_number?(value)
          return true if value == 'true'
          return false if value == 'false'
          return value
      end
    
      def puts(*lines)
        frame = caller_locations(1, 1)[0]
        frame_info = "#{File.basename(frame.path)}:#{frame.lineno}"
        lines.each do |line|
          nl_head = line =~ /\A\n+/ ? $~[0] : ""
          nl_tail = line =~ /\n+\z/ ? $~[0] : ""
          line = line[nl_head.size ... (line.size - nl_tail.size)] unless nl_head.empty? && nl_tail.empty?
          super("#{nl_head}#{line} [#{frame_info}]#{nl_tail}")
        end
        nil
      end

      # This is a helper function to updating thr values for dynamodn. It handldes the data type being uploaded.

      # @param dynamo_attributes [Hash] The attributes to be written to the table.
      # @param key [String] The key of the item to be updated or the column name
      # @param value [String] The value of the item to be updated or the column value

      #This method returns the dynamo_attributes hash with the key and value added to it properly and formatted for dynamo.

      def package_dynamo_item(dynamo_attributes, key, value)
        #make sure nil doesn't get passed to get converted to a empty string
        value = nil if value.nil?
    
        #empty string values in a Hash will throw a dyanmo error
        value = sanitize_hash(value) if value.is_a?(Hash)
    
        #convert Time objects to iso8601 strings
        value = value.iso8601 if TIMECLASSES.include? value.class.to_s
    
        #dynamo doesn't support empty strings at this point, hack around it
        value = "<empty string>" if value.is_a?(String) && value.empty?
    
        dynamo_attributes[key] = value
    
      end

      #This is a helper function to sanitize the hash before it is sent to dynamo. It handles the data type being uploaded.

      # @param [Hash] hash the object containing attributes to be written to the table.

      #This method runs the Hash with its values properly typed in dynamo.


      def sanitize_hash(hash)
        sanitized = {}
        hash.each do |key, value|
          sanitized_val = "<empty string>" if value.is_a?(String) && value.empty?
          sanitized_val = sanitize_hash(value) if value.is_a?(Hash)
          value = value.iso8601 if TIMECLASSES.include? value.class.to_s
          sanitized_val = value if sanitized_val.nil?
          sanitized[key] = sanitized_val
        end
        sanitized
      end
    
    
      def reserved_words_mapping
        {
          action: '#act',
          attribute: '#attr',
          avg: '#avg',
          by: '#by',
          catalog: '#cat',
          count: '#cnt',
          date: '#dt',
          depth: '#dpt',
          format: '#fmt',
          max: '#max',
          min: '#min',
          name: '#n',
          state: '#st',
          status: '#sts',
        }
      end

    end

  end

end
