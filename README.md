# Crudop

Crudop is a Ruby gem designed for streamlined interactions with AWS DynamoDB. It simplifies CRUD (Create, Read, Update, Delete) operations and provides utility methods for handling DynamoDB data types and queries.

## Installation

Add this line to your application's Gemfile:

``` ruby
gem 'crudop', git: 'https://github.com/tanzim10/CRUDOP_GEM.git'
```

Then execute it:

```bash
bundle install
```

## Usage

Crudop can be used in any Ruby on Rails model to perform standard CRUD operations for AWS DynamoDB.

First, you need to configure AWS by setting your region, access_key, and secret_access_key in your environment

```bash
AWS_REGION=your-region-here
AWS_ACCESS_KEY_ID=your-access-key-id
AWS_SECRET_ACCESS_KEY = your-secret-access-key
```

Then import the gem
``` ruby
require 'crudop'
```


### DynamoDB Client

```
Obtain an instance of Aws::DynamoDB::Client with proper configuration:
client = Crudop.dynamo_client
 => <Aws::DynamoDB::Client>
```

### Create
```
 Add a new item to a DynamoDB table:
 new_item = { 'id' => 1234567, 'first_name' => 'John', 'last_name' => 'Doe' }
```
 ```ruby
Crudop::Dynamo.dy_put_item('players_table_name', new_item)
 ```

### Read
Get Item by Key

```ruby
Crudop::Dynamo.dy_get_item(table_name, key, value) 
```

Retrieves an item based on the specified key and value, querying a specific table.
key = {"EMPNO": "1", "FirstName": "Tanzim"}

### Query 
```ruby
Crudop::Dynamo.dy_query_item(table_name, primary_key,primary_value)
```

Parse Response 

```ruby
Crudop::Dynamodb.parse_response(response_items)
```


### Update

The dy_update_item method in the Crudop DynamoDB Gem is used for updating specific attributes of an item in a DynamoDB table.

Update Item <br>
```ruby
Crudop::Dynamo.dy_update_item(table_name, key, attribute_updates)
```

```
Example
table_name = "Players"
key = { player_id: 'p123' } 
attribute_updates = { 'score' => 100, 'status' => 'active' }
```

### Delete

__Delete Item__ <br><br> 
```ruby
Crudop::Dynamo.dy_delete_item(table_name, key, value) 
```
Deletes an item based on the specified key.
key = {"EMPNO": "1", "FirstName": "Tanzim"} It needs a primary key or a composite key depending on the structure of the table.




## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/tanzim10/CRUDOP_GEM

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
