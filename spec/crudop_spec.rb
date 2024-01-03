# frozen_string_literal: true

RSpec.describe Crudop do
  let(:dynamodb_client) { instance_double(Aws::DynamoDB::Client) }

  before do
    allow(Aws::DynamoDB::Client).to receive(:new).and_return(dynamodb_client)
  end

  describe '.create' do
    let(:table_name) {'MyTable'}
    let(:item) {{FirstName: 'Test User F name', LastName: 'Test User Last Name'}}

    it 'creates a new record' do
      expected_args = { table_name: table_name, item: item }
      expect(dynamodb_client).to receive(:put_item).with(expected_args)
      Crudop::Record.create(table_name, item)
    end
  end

  describe '.update' do
    let(:table_name) {'MyTable'} 
    let(:key) {{ 'EMPNO': '1' }}
    let(:attribute_updates) {{FirstName: 'Updated User F name', LastName: 'Updated User Last Name'}}
   

    it 'updates an existing record' do
      expected_args = { table_name: table_name, key: key, attribute_updates: attribute_updates} 
      expect(dynamodb_client).to receive(:update_item).with(expected_args)
      Crudop::Record.update(table_name, key, attribute_updates)
    end
  end

  describe '.read' do
    let(:table_name) {'MyTable'} 
    let(:key) {{ 'EMPNO': '1' }}
  
    it 'reads an existing record' do
      expected_response = double('response', item: key)
      expect(dynamodb_client).to receive(:get_item).with(table_name: table_name, key: key).and_return(expected_response)
      result = Crudop::Record.read(table_name, key)

      expect(result).to eq(key)
    end
  end
  

  describe '.delete' do
    let(:table_name) {'MyTable'} 
    let(:key) {{ 'EMPNO': '1' }}
  
    it 'deletes an existing record' do
      expect(dynamodb_client).to receive(:delete_item).with(table_name: table_name, key: key)
      Crudop::Record.delete(table_name, key)
    end
  end
    
  
end
