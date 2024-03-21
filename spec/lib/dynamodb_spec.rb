require "spec_helper"
require "crudop"

RSpec.describe Crudop::Dynamo do

  include_context "table setup"

  
  let(:test_record) do
    Crudop::Dynamo.dy_get_item(test_table_name, { "test_key" => @key })[:item]
  end
  
  describe ".dynamodb_client" do
    it "returns a DynamoDB client" do
      expect(Crudop::Dynamo.dynamodb_client).to be_a(Aws::DynamoDB::Client)
    end
  end
  

  describe ".dy_put_item" do

    it "creates an item in the table" do
      Crudop::Dynamo.dy_put_item('dynamo', { 'test_key' => 'test' })
      expect(test_record). to be_nill
    end
  end

  describe ".dy_update_item" do
    let(:key_hash) { { 'test_key' => key } }
    let(:new_attributes) {{ 'some_old_field' => 'new field', 'name'=> 'updated name', 'attribute' => 'updated attribute' }}
  
    before(:each) do
      #updating the test record
      Crudop::Dynamo.dy_put_item(@test_table_name, @test_item)
    end
  
    context "does not replace exising record" do
  
      before(:each) do
        Crudop::Dynamo.dy_update_item(@test_table_name, key_hash, new_attributes)
      end
  
      it "will update exisiting columns" do
        expect(test_record['name']).to eq(new_attributes['name'])
      end
  
      it "will add the new columns " do
        expect(test_record['some_old_field']).to eq(new_attributes['some_old_field'])
        expect(test_record['attribute']).to eq(new_attributes['attribute'])
      end
  
      it "does not change columns that were not updated" do
        expect(test_record['attribute']).to eql @test_item['attribute']
      end
  
    end
  end

  


  describe ".sanitize_dynamo_hash" do

    let(:test_hash) do
      {
        'hash' => {'test1'=>'test1'},
        'time' => Time.now,
        'regular'=> 'regular',
      }
    end
    
    it "returns the DynamoDB valid hash" do
      hash = Crudop::Dynamo.sanitize_dynamo_hash(test_hash)
      expect(hash['hash']). to eql test_hash['hash']
      expect(hash['time']). to eql test_hash['time'].iso8601
      expect(hash['regular']). to eql test_hash['regular']
    end
  
  end



end




