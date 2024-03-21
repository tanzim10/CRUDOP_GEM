require "spec_helper"
require "crudop"

RSpec.describe Crudop::Dynamo do
  let (:test_table_name) {'dynamo'}
  let (:Pkey) {'test_key'}
  let (:Skey) {'sort_key'}
  let (:pval) {'test'}
  let (:sval) {'sort'}
  let (:test_item) {{id: '123', timestamp: 456, data: 'test_data'}}

  
  describe ".dynamodb_client" do
    it "returns a DynamoDB client" do
      expect(Crudop::Dynamo.dynamodb_client).to be_a(Aws::DynamoDB::Client)
    end
  end
  

  describe ".dy_put_item" do
    it "creates an item in the table" do
      dynamo_client_double = instance_double(Aws::DynamoDB::Client)
      allow(Crudop::Dynamo).to receive(:dynamodb_client).and_return(dynamo_client_double)
      
      allow(dynamo_client_double).to receive(:put_item).and_return('mock_response')
  
      Crudop::Dynamo.dy_put_item(test_table_name, test_item)
  
      expect(dynamo_client_double).to have_received(:put_item).with(hash_including(table_name: test_table_name, item: test_item))
    end
  end
  

  describe ".dy_update_item" do

    let(:table_name) { 'dynamo' }
    let(:key) { 'test_key' }
    let(:key_hash) { { 'test_key' => key } }
    let(:new_attributes) {{ 'some_old_field' => 'new field', 'name' => 'updated name', 'attribute' => 'updated attribute' }}
    let(:dynamo_client_double) { instance_double(Aws::DynamoDB::Client) }
  
    before do
      # Stub the dynamodb_client to return the double
      allow(Crudop::Dynamo).to receive(:dynamodb_client).and_return(dynamo_client_double)
  
      # Stub the update_item method on the DynamoDB client double
      allow(dynamo_client_double).to receive(:update_item).and_return('mock_response')
    end
  
    it "updates existing columns and adds new columns without replacing existing records" do
      # Perform the update operation
      Crudop::Dynamo.dy_update_item(table_name, key_hash, new_attributes)
    
      # Verify update_item was called with expected arguments
      expect(dynamo_client_double).to have_received(:update_item) do |args|
        expect(args[:table_name]).to eq(table_name)
        expect(args[:key]).to eq(key_hash)
    
        # Checking that each attribute is correctly referenced in the update expression
        expect(args[:update_expression]).to match(/#SOME_OLD_FIELD\s*=\s*:valsome_old_field/)
        expect(args[:update_expression]).to match(/#NAME\s*=\s*:valname/)
        expect(args[:update_expression]).to match(/#ATTRIBUTE\s*=\s*:valattribute/)
    
        # Checking that expression attribute names and values contain expected mappings
        expect(args[:expression_attribute_names]).to include("#SOME_OLD_FIELD" => "some_old_field")
        expect(args[:expression_attribute_values]).to include(":valsome_old_field" => anything)
      end
    end    
  end
  



  describe ".sanitize_hash" do
    let(:time_now) { Time.now }
    let(:test_hash) do
      {
        'hash' => {'empty_string' => ''},
        'time' => time_now,
        'regular' => 'regular',
        'empty' => ''
      }
    end
  
    it "returns a DynamoDB valid hash with sanitized values" do
      sanitized_hash = Crudop::Dynamo.sanitize_hash(test_hash)
  
      expect(sanitized_hash['hash']).to eq({'empty_string' => "<empty string>"})
      expect(sanitized_hash['time']).to eq(time_now.iso8601)
      expect(sanitized_hash['regular']).to eq('regular')
      expect(sanitized_hash['empty']).to eq("<empty string>")
    end
  end


  describe ".dy_delete_item" do
    let(:dynamo_client_double) { instance_double(Aws::DynamoDB::Client) }
    let(:table_name) { 'Employee' }
    let(:key) { { "EMPNO" => 1 } }
    let(:mock_response) { double("response") }
  
    before do
      allow(Crudop::Dynamo).to receive(:dynamodb_client).and_return(dynamo_client_double)
      allow(dynamo_client_double).to receive(:delete_item).and_return(mock_response)
    end
  
    it "deletes an item from the table" do
      result = Crudop::Dynamo.dy_delete_item(table_name, key)
      expect(result).to eq(mock_response)
      expect(dynamo_client_double).to have_received(:delete_item).with(table_name: table_name, key: key)
    end
  end
  
  describe '.dy_get_item' do
    let(:dynamodb_client) { instance_double(Aws::DynamoDB::Client) }
    let(:table_name) { 'Employee' }
    let(:key) { { "EMPNO" => 1, "FirstName" => "Tanzim" } }
    let(:mock_response) {{ "EMPNO" => 1, "FirstName" => "Tanzim" }}
  
    before do
      allow(Crudop::Dynamo).to receive(:dynamodb_client).and_return(dynamodb_client)
      # Adjust the stub to explicitly expect keyword arguments
      allow(dynamodb_client).to receive(:get_item).and_return(mock_response)
    end
  
    it 'retrieves an item from the DynamoDB table' do
      result = Crudop::Dynamo.dy_get_item(table_name, key)
      expect(result).to eq(mock_response)
      expect(dynamodb_client).to have_received(:get_item).with(table_name: table_name, key: key)
    end
  end
  
end




