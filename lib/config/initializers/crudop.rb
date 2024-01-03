Crudop.configure do |config|
    config.aws_access_key_id = ENV['AWS_ACCESS_KEY_ID']
    config.aws_secret_access_key = ENV['AWS_SECRET_ACCESS_KEY']
    config.aws_region = ENV['AWS_REGION']
  end

