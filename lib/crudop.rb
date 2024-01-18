require_relative "crudop/version"
require "aws-sdk-dynamodb"

module Crudop
  #This class handles the CRUD Operation for Dynamo
  class Dynamo
    #Constant for Time Class
    TIMECLASSES = %w[ActiveSupport::TimeWithZone Time DateTime Date].freeze
    class << self
      # Returns a DynamoDB client with proper configuration
      def dynamodb_client(caller_options = {})
        cache = (@@dy_client ||= {})
        keys = caller_options.map { |k, v| "#{k},#{v}" }.sort.join('|').freeze
        return cache[keys] if cache.key?(keys)

        # need_fallback =  !lambda? && local?
        opts = { region: ENV["AWS_REGION"] || "us-east-1" }
        opts.merge!(credentials_dy)
        opts.merge!(caller_options)
        puts "DynamoDB Client Options: #{opts}"
        cache[keys] = Aws::DynamoDB::Client.new(**opts)
      end

      def credentials_dy
        key_id = ENV["AWS_ACCESS_KEY_ID"] || "fake_key_id"
        secret = ENV["AWS_SECRET_ACCESS_KEY"] || "fake_secret"
        return { access_key_id: key_id, secret_access_key: secret }
      end

      # Write the items to the table
      # This method will overwrite the existing items if a key already exists.

      # @param table_name [String] The name of the table which the items will be written to.
      # @param attributes [Hash] The attributes to be written to the table.

      def dy_put_item(tablename, attributes)
        dy_attributes = {table_name: tablename, 
                        item: attributes}
        dy_attributes[:item][:last_synced_at] = Time.now.iso8601
        dynamodb_client.put_item(dy_attributes)
      end

      # # Get items from the table
      # @param table_name [String] The name of the table which the items will be retrieved from.
      # @param key [Hash] The key of the item to be retrieved.
      # Example key: { "EMPNO" => 1, "FirstName" => "Tanzim"}     # Use an actual integer for a numeric attribute 
      # The primary key and sort key needs to be defined to retrieve an item from the table.

      def dy_get_item(table_name, key)
        dy_attributes = {table_name: table_name, 
                          key: key}
        begin
          dynamodb_client.get_item(dy_attributes)
        rescue StandardError => exception
          puts "Error getting item: #{exception.message}"
        end
      end

      # # Delete items from the table

      # # @param table_name [String] The name of the table which the items will be deleted from.
      # # @param key [Hash] The key of the item to be deleted.

      # # Example
      # #   key = {'EMPNO': "1"} the primary key of the item can be like this or have a sort key as well.

      def dy_delete_item(table_name, key)
        dy_attributes = {table_name: table_name, 
                          key: key}
        begin
          dynamodb_client.delete_item(dy_attributes)
        rescue StandardError => exception
          puts "Error deleting item: #{exception.message}"
        end
      end


     
      def dy_update_item(table_name, key, attribute_updates)
        attribute_names = {}
        attribute_values = {}
        update_expressions = []
      
        # Use placeholder for DynamoDB reserved words or special characters
        attribute_updates.each do |attr_key, attr_val|
          placeholder = "##{attr_key.upcase}"
          value_placeholder = ":val#{attr_key.downcase}"
          attribute_names[placeholder] = attr_key
          package_dynamo_item(attribute_values, value_placeholder, attr_val)
          update_expressions << "#{placeholder} = #{value_placeholder}"
        end
      
        update_expression = "SET " + update_expressions.join(", ")
      
        params = {
          table_name: table_name,
          key: key,
          expression_attribute_names: attribute_names,
          expression_attribute_values: attribute_values,
          update_expression: update_expression
        }
      
        dynamodb_client.update_item(params)
      end
      


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
      
      #  Scan the table and return all the items
      # @param table_name [String] The name of the table which the items will be retrieved from.
      # @return [Array] An array of items from the table.


      def scan_dynamodb_table(table_name)
        client = Aws::DynamoDB::Client.new
        items = []
        scan_parameters = { table_name: table_name }

        begin
          loop do
            response = client.scan(scan_parameters)
            items.concat(response.items)
            break unless response.last_evaluated_key

            scan_parameters[:exclusive_start_key] = response.last_evaluated_key
          end
        rescue Aws::DynamoDB::Errors::ServiceError => error
          puts "Unable to scan the table: #{error.message}"
        end

        items
      end



    end
  end
end
