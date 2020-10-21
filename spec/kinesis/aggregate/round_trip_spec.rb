require 'securerandom'
require 'pry'

RSpec.describe 'Round-tripping the aggregator and deaggregator' do
  subject(:round_trip) do
    aggregator = Kinesis::Aggregation::Aggregator.new
    user_records.each { |r| aggregator.add_user_record(r) }
    kinesis_record = {
      'kinesis' => { 'data' => aggregator.aggregate![:data] }
    }
    Kinesis::Aggregation::Deaggregator.new(kinesis_record).deaggregate
  end


  context 'Given values with explicit hash keys' do
    let(:user_records) do
      [
        {
          partition_key: SecureRandom.uuid,
          explicit_hash_key: SecureRandom.uuid,
          data: 'this is row 1'
        },
        {
          partition_key: SecureRandom.uuid,
          explicit_hash_key: SecureRandom.uuid,
          data: 'this is row 2'
        }
      ]
    end

    it 'matches the user record' do
      records = round_trip.map { |r| r[:kinesis] }
      expect(records[0][:partitionKey]).to eq user_records[0][:partition_key]
      expect(records[0][:explicitHashKey]).to eq user_records[0][:explicit_hash_key]
      expect(Base64.decode64(records[0][:data])).to eq user_records[0][:data]
    end
  end

  context 'Given values without explicit hash keys' do
    let(:user_records) do
      [
        {
          partition_key: SecureRandom.uuid,
          data: 'this is row 1'
        },
        {
          partition_key: SecureRandom.uuid,
          data: 'this is row 2'
        }
      ]
    end

    it 'matches the user record' do
      records = round_trip.map { |r| r[:kinesis] }
      expect(records[0][:partitionKey]).to eq user_records[0][:partition_key]
      expect(Base64.decode64(records[0][:data])).to eq user_records[0][:data]
    end
  end
end
