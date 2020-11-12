# Kinesis::Aggregation

This gem knows how to read and write kinesis aggregated messages.  This is most useful when writing a ruby lambda that consumes kinesis aggregated messages.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'kinesis-aggregation'
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install kinesis-aggregation

## Usage

### Aggregation

```ruby
aggregator = Kinesis::Aggregation::Aggregator.new

# explicit_hash_key is optional
aggregator.add_user_record(partition_key: 'fc03dd88-3e79-448a-b01a-7cf1bd47b784',
                           explicit_hash_key: '38486495867508399078159723846051807020',
                           data: "RECORD 22 peeobhczbzdmskboupgyq\n")

aggregator.add_user_record(partition_key: 'cae41b1c-ea61-43f2-90be-b8755ebf88e2',
                           explicit_hash_key: '193787600037681706952143357071916352604',
                           data: "RECORD 23 uswkxftxroeusscxsjhno\n")

aggregator.add_user_record(partition_key: 'd490690c-e74d-4db2-a3c8-d8f2f184fd23',
                           explicit_hash_key: '266880436964932424265466916734068684439',
                           data: "RECORD 24 casehdgivfaxeustlyszy\n")

aggregator.add_user_record(partition_key: 'c924bc09-b85e-47f1-b32e-336522ee53c8',
                           explicit_hash_key: '339606600942967391854603552402021847292',
                           data: "RECORD 25 nvffvpmuogdopjhamevrk\n")
aggregated_record = aggregator.aggregate!
```

### Deaggregatoin

```ruby
deaggregated_records = Kinesis::Aggregation::Deaggregator.new(aggregated_record).deaggregate
```

### Use from within a lambda

`function.rb`:
```ruby
require 'kinesis/aggregation'

def handler(event:, context:)
  event['Records'].each do |aggregated_record|
    records = Kinesis::Aggregation::Deaggregator.new(aggregated_record).deaggregate

    # interesting code goes here
  end
end
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/hawknewton/ruby-kinesis-aggregation.

