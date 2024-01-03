# frozen_string_literal: true

require_relative "crudop/version"
require 'aws-sdk-dynamodb'


module Crudop

  class Configuration
    attr_accessor :aws_access_key_id, :aws_secret_access_key, :aws_region

    def initialize
      @aws_access_key_id = nil
      @aws_secret_access_key = nil
      @aws_region = nil
    end
    
  end

  def self.configuration
    @configuration ||= Configuration.new
  end

  def self.configure
    yield(configuration)
  end


  class Record
    #Returns a Configured AWS DyanmoDB Client
    def self.client
      Aws::DynamoDB::Client.new(
        region: Crudop.configuration.aws_region,
        credentials: Aws::Credentials.new(
          Crudop.configuration.aws_access_key_id,
          Crudop.configuration.aws_secret_access_key
          )
        )
    end

    def self.create(table_name, item)
      client.put_item({table_name: table_name, item: item})
    end

    def self.read(table_name, key)
      client.get_item(table_name: table_name, key: key).item
    end

    def self.update(table_name, key, attributes_updates)
      client.update_item(table_name: table_name, key: key, attribute_updates: attributes_updates) 
    end
    
    def self.delete(table_name, key)
      client.delete_item(table_name: table_name, key: key)
    end

  end
end
