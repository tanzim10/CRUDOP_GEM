# Crudop

Welcome to Crudop! Crudop is a Ruby gem providing an easy-to-use interface for CRUD (Create, Read, Update, Delete) operations, making it simpler to manage these common database interactions in your Ruby on Rails applications.

## Installation

Add this line to your application's Gemfile:

gem 'crudop', git: 'https://github.com/tanzim10/CRUDOP_GEM.git'

Then execute it:

bundle install

## Usage

Crudop can be used in any Ruby on Rails model to perform standard CRUD operations

### Create
To create a new record:

attributes = { name: 'John Doe', email: 'john@example.com' } <br>

user = Crudop::Record.create(User, attributes)

### READ
To read a record by ID:

user = Crudop::Record.read(User, user_id)


## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/tanzim10/CRUDOP_GEM

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).
