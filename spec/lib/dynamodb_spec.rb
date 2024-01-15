require "spec_helper"
require "crudop"

RSpec.describe Crudop::Dynamodb do

  include_context "table setup"
  include Crudop

  
  let(:test_record) do
    crudop = Dynamodb.new()
    crudop.dy_get_item(test_table_name, { "test_key"=>key })[:item]
  end

  describe ".dynamodb_client" do

    it "returns a dynamodb client" do
      crudop = Dynamodb.new()
      expect(crudop.dynamodb_client).to be_a(Aws::DynamoDB::Client)
    end
    
  end

  describe ".dy_put_item" do

    it "creates an item in the table" do
      puts Crudop::Dynamodb.methods.sort.inspect
        Crudop::Dynamodb.dy_put_item('dynamo', { 'test_key' => 'test' })
        expect(test_record). to be_truthy
    end
  end

  describe ".dy_update_item" do
    let(:key_hash) { { 'test_key' => key } }
    let(:new_attributes) {{ 'some_old_field' => 'new field', 'name'=> 'updated name', 'attribute' => 'updated attribute' }}
  
    before(:each) do
      #updating the test record
      Crudop::Dynamodb.dy_put_item(@test_table_name, @test_item)
    end
  
    context "does not replace exising record" do
  
      before(:each) do
        Crudop::Dynamodb.dy_update_item(@test_table_name, key_hash, new_attributes)
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

  describe ".dy_get_item" do
    before(:each) do
      #upload test records the dynamo, add reserved words to record to test from reading
      @test_item[:name] = 'test'
      @test_item[:job] = 'test'
      #character is reserved word
      @test_item[:character] = 'test'
      Crudop::Dynamodb.dy_put_item(@test_table_name, @test_item)
    end
  
    it "returns the record of the dynamo table" do
      expect(test_record['test_key']).to eql key
      expect(test_record['name']).to eql @test_item['name']
      expect(test_record['attribute']).to eql @test_item['attribute']
      expect(test_record).to have_key('dynamo_last_synced_at')
    end
  
    context 'reserved word - name' do
      let(:item) { @test_item.merge(name: 'test') }
      before(:each) { Crudop::Dynamodb.dy_put_item(@test_table_name, item) }
  
      it 'does not error when name is requested' do
        expect { Crudop::Dynamodb.dy_get_item(@test_table_name, { 'test_key'=>@key },['name']) }.to_not raise_error
      end
    end
  
    context 'reserved word - state' do
      let(:item) { @test_item.merge(state: 'test') }
      before(:each) { Crudop::Dynamodb.dy_put_item(@test_table_name, item) }
  
      it 'does not error when status is requested' do
        expect { Crudop::Dynamodb.dy_get_item(@test_table_name, { 'test_key'=>@key },['state']) }.to_not raise_error
      end
    end
  
    context 'reserved word - action' do
      let(:item) { @test_item.merge(action: 'test') }
      before(:each) { Crudop::Dynamodb.dy_put_item(@test_table_name, item) }
  
      it 'does not error when action is requested' do
        expect { Crudop::Dynamodb.dy_get_item(@test_table_name, { 'test_key'=>key },['action']) }.to_not raise_error
      end
    end
  
    context 'reserved word - attribute' do
      let(:item) { @test_item.merge(attribute: 'test') }
      before(:each) { Crudop::Dynamodb.dy_put_item(@test_table_name, item) }
  
      it 'does not error when status is requested' do
        expect { Crudop::Dynamodb.dy_get_item(@test_table_name, { 'test_key'=>key },['attribute']) }.to_not raise_error
      end
    end
  
    context 'reserved word - avg' do
      let(:item) { @test_item.merge(avg: 'test') }
      before(:each) { Crudop::Dynamodb.dy_put_item(@test_table_name, item) }
  
      it 'does not error when avg is requested' do
        expect { Crudop::Dynamodb.dy_get_item(@test_table_name, { 'test_key'=>key },['avg']) }.to_not raise_error
      end
    end
  
    context 'reserved word - by' do
      let(:item) { @test_item.merge(by: 'test') }
      before(:each) { Crudop::Dynamodb.dy_put_item(@test_table_name, item) }
  
      it 'does not error when by is requested' do
        expect { Crudop::Dynamodb.dy_get_item(@test_table_name, { 'test_key'=>key },['by']) }.to_not raise_error
      end
    end
  
    context 'reserved word - catalog' do
      let(:item) { @test_item.merge(catalog: 'test') }
      before(:each) { Crudop::Dynamodb.dy_put_item(@test_table_name, item) }
  
      it 'does not error when catalog is requested' do
        expect { Crudop::Dynamodb.dy_get_item(@test_table_name, { 'test_key'=>key },['catalog']) }.to_not raise_error
      end
    end
  
    context 'reserved word - count' do
      let(:item) { @test_item.merge(count: 'test') }
      before(:each) { Crudop::Dynamodb.dy_put_item(@test_table_name, item) }
  
      it 'does not error when count is requested' do
        expect { Crudop::Dynamodb.dy_get_item(@test_table_name, { 'test_key'=>key },['count']) }.to_not raise_error
      end
    end
  
    context 'reserved word - date' do
      let(:item) { @test_item.merge(date: 'test') }
      before(:each) { Crudop::Dynamodb.dy_put_item(@test_table_name, item) }
  
      it 'does not error when date is requested' do
        expect { Crudop::Dynamodb.dy_get_item(@test_table_name, { 'test_key'=>key },['date']) }.to_not raise_error
      end
    end
  
    context 'reserved word - depth' do
      let(:item) { @test_item.merge(depth: 'test') }
      before(:each) { Crudop::Dynamodb.dy_put_item(@test_table_name, item) }
  
      it 'does not error when depth is requested' do
        expect { Crudop::Dynamodb.dy_get_item(@test_table_name, { 'test_key'=>key },['depth']) }.to_not raise_error
      end
    end
  
    context 'reserved word - format' do
      let(:item) { @test_item.merge(format: 'test') }
      before(:each) { Crudop::Dynamodb.dy_put_item(@test_table_name, item) }
  
      it 'does not error when format is requested' do
        expect { Crudop::Dynamodb.dy_get_item(@test_table_name, { 'test_key'=>key },['format']) }.to_not raise_error
      end
    end
  
    context 'reserved word - max' do
      let(:item) { @test_item.merge(max: 'test') }
      before(:each) { Crudop::Dynamodb.dy_put_item(@test_table_name, item) }
  
      it 'does not error when max is requested' do
        expect { Crudop::Dynamodb.dy_get_item(@test_table_name, { 'test_key'=>key },['max']) }.to_not raise_error
      end
    end
  
    context 'reserved word - min' do
      let(:item) { @test_item.merge(min: 'test') }
      before(:each) { Crudop::Dynamodb.dy_put_item(@test_table_name, item) }
  
      it 'does not error when min is requested' do
        expect { Crudop::Dynamodb.dy_get_item(@test_table_name, { 'test_key'=>key },['min']) }.to_not raise_error
      end
    end
  
    context 'reserved word - status' do
      let(:item) { @test_item.merge(status: 'test') }
      before(:each) { Crudop::Dynamodb.dy_put_item(@test_table_name, item) }
  
      it 'does not error when status is requested' do
        expect { Crudop::Dynamodb.dy_get_item(@test_table_name, { 'test_key'=>key },['status']) }.to_not raise_error
      end
    end
  
    context 'reserved word - character' do
      let(:item) { @test_item.merge(character: 'test') }
      before(:each) { Crudop::Dynamodb.dy_put_item(@test_table_name, item) }
  
      it 'should not error if a reserved word that is not handled is queried' do
        expect { Crudop::Dynamodb.dy_get_item(@test_table_name, { 'test_key'=>key },['character']) }.to_not raise_error
      end
    end
  end


  describe ".dy_delete_item" do

    before(:each) do
      #upload test records the dynamo
      @test_item[:name] = 'test'
      @test_item[:job] = 'test'
      Crudop::Dynamodb.dy_put_item(@test_table_name, @test_item)
    end
  
    it "deletes the record from the dynamo table" do
      Crudop::Dynamodb.dy_delete_item(@test_table_name, { "test_key"=>key })
      expect(test_record).to be_nil
    end
  end

  describe ".package_dynamo_item" do
    let(:attributes) { {} }
  
    context "value nil" do
  
      it "sets the value as nill" do
        Crudop::Dynamodb.package_dynamo_item(attributes, "nil_key", nil)
        expect(attributes).to have_key('nil_key')
        expect(attributes['nil_key']).to be_nil
      end
    end

    context "value is a Hash" do
  
      let(:value) { { 'test'=>'test', 'test1'=>'test1', 'test2'=>'test2' } }
  
      it "sets the hash value" do
        Crudop::Dynamodb.package_dynamo_item(attributes, 'hash_key', value)
        expect(attributes).to have_key('hash_key')
        expect(attributes['hash_key']).to eql value
      end
  
    end
  
  
    context "value is a time object" do
  
      context "Time" do
        let(:value) { Time.now }
        it "sets to iso8601 time" do
          Crudop::Dynamodb.package_dynamo_item(attributes, 'time_key', value)
          expect(attributes).to have_key('time_key')
          expect(attributes['time_key']).to eql value.iso8601
        end
      end
  
      context "Date" do
        let(:value) { Date.today }
        it "sets to iso8601 time" do
          Crudop::Dynamodb.package_dynamo_item(attributes, 'time_key', value)
          expect(attributes).to have_key('time_key')
          expect(attributes['time_key']).to eql value.iso8601
        end
      end
  
      context "Date" do
        let(:value) { DateTime.now }
        it "sets to iso8601 time" do
          Crudop::Dynamodb.package_dynamo_item(attributes, 'time_key', value)
          expect(attributes).to have_key('time_key')
          expect(attributes['time_key']).to eql value.iso8601
        end
      end
  
      context "ActiveSupport::TimeWithZone" do
        it "sets to iso8601 time" do
          # mocks active support classes
          module ActiveSupport; class TimeWithZone < Time; end; end
          value = ActiveSupport::TimeWithZone.now
          Crudop::Dynamodb.package_dynamo_item(attributes, 'time_key', value)
          expect(attributes).to have_key('time_key')
          expect(attributes['time_key']).to eql value.iso8601
        end
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
    
    it "returns the dynamodb valid hash" do
      hash = Crudop::Dynamodb.sanitize_dynamo_hash(test_hash)
      expect(hash['hash']). to eql test_hash['hash']
      expect(hash['time']). to eql test_hash['time'].iso8601
      expect(hash['regular']). to eql test_hash['regular']
    end
  
  end

  describe "#query_by_index" do

    let(:index_id) { 123 }
    let(:index_name) { 'test_index' }
    let(:index_column) { 'some id' }
    let(:table_name) {'index_table_dev'}
    let(:item) { { 'id' => 1, "#{ index_column }"=> index_id, 'wahoo'=> 'test' } }
    let(:index_queried_item) { Crudop::Dynamodb.query_by_index(table_name, 'some_id',index_id, index_name) }

    before(:each) do
      Crudop::Dynamodb.dy_put_item(table_name, item)
    end

    context 'valid item' do
      it 'returns the Array' do
        expect(index_queried_item).to be_a(Array)
      end

      context 'item' do
        let(:queried_item) { index_queried_item.first }

        it 'returns the item' do
          expect(queried_item['id']).to eql 1
          expect(queried_item['index_column']).to eql index_id
          expect(queried_item['wahoo']).to eql 'test'
        end
      end

    end

    context 'invalid item' do
      let(:empty_query) { Crudop::Dynamodb.query_by_index(table_name, 'some_id', 1234, index_name) }
      it 'returns an empty array' do
        expect(empty_query).to eql []
      end
    end


  end

  describe "get_item" do
    let(:test_hash) { { 'some_key'=>1,'some_other_key'=>'key' } }
    let(:test_array) { ['item', 'other item', 'another'] }
    let(:timestamp) { Time.now.iso8601 }
    let(:test_item) do
      item = {}
      Crudop::Dynamodb.package_dynamo_item(item, 'test_key','get_item_test')
      Crudop::Dynamodb.package_dynamo_item(item, 'name','Testing Get Item')
      Crudop::Dynamodb.package_dynamo_item(item, 'hash', test_hash)
      Crudop::Dynamodb.package_dynamo_item(item, 'array', test_array)
      Crudop::Dynamodb.package_dynamo_item(item, 'timestamp', timestamp)
      Crudop::Dynamodb.package_dynamo_item(item, 'nil', nil)
      item
    end


    before(:each) do
      item = {}
      Crudop::Dynamodb.package_dynamo_item(item, 'test_key','get_item_test')
      Crudop::Dynamodb.package_dynamo_item(item, 'name','Testing Get Item')
      Crudop::Dynamodb.package_dynamo_item(item, 'hash', test_hash)
      Crudop::Dynamodb.package_dynamo_item(item, 'array', test_array)
      Crudop::Dynamodb.package_dynamo_item(item, 'timestamp', timestamp)
      Crudop::Dynamodb.package_dynamo_item(item, 'nil', nil)
      item
      Crudop::Dynamodb.dy_put_item(@test_table_name, item)
    end

    context 'valid item' do

      let(:get_item) { Crudop::Dynamodb.get_item(@test_table_name, { 'test_key' => 'get_item_test' }) }

      it 'returns a hash' do
        expect(get_item).to be_a Hash
      end

      it 'has correct attributes' do
        expect(get_item['test_key']).to eql 'get_item_test'
        expect(get_item['name']).to eql 'Testing Get Item'
        expect(get_item['hash']).to eql test_hash
        expect(get_item['array']).to eql test_array
        expect(get_item['nil']).to be_nil
        expect(get_item['timestamp']).to eql timestamp
      end
    end

    context 'item not found' do

      let(:get_item) { Crudop::Dynamodb.get_item(@test_table_name, { 'test_key' => 'missing_item' }) }

      it 'returns an empty hash' do
        expect(get_item).to eql({})
      end

    end

    context 'tables' do

      def test_table(method, expected_name)
        # Test environment
        allow(ENV).to receive(:[]).with("TEST").and_return("true")
        allow(ENV).to receive(:[]).with("TARGET_ENVIRONMENT").and_return(nil)
        expect(method.call).to eql("#{expected_name}_development")
      
        # Development environment
        allow(ENV).to receive(:[]).with("TEST").and_return(nil)
        allow(ENV).to receive(:[]).with("TARGET_ENVIRONMENT").and_return('development')
        expect(method.call).to eql("#{expected_name}_development")
      
        # Staging environment
        allow(ENV).to receive(:[]).with("TEST").and_return(nil)
        allow(ENV).to receive(:[]).with("TARGET_ENVIRONMENT").and_return('staging')
        expect(method.call).to eql("#{expected_name}_staging")

        # Production environment
        allow(ENV).to receive(:[]).with("TEST").and_return(nil)
        allow(ENV).to receive(:[]).with("TARGET_ENVIRONMENT").and_return('production')
        expect(method.call).to eql("#{expected_name}_production")
      
      end

      it 'has employee' do
        test_table(Crudop::Dynamodb.method(:employee_table), 'employee')
      end
    end

  end



end




