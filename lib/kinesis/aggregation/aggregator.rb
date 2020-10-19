module Kinesis
  module Aggregation
    class Aggregator
      MAGIC = "\xf3\x89\x9a\xc2".force_encoding('ASCII-8BIT').freeze

      def initialize
        @user_records = []
      end

      def add_user_record(user_record)
        @user_records << user_record
      end

      def aggregate!
        result = {
          partition_key: @user_records.first[:partition_key],
          explicit_hash_key: @user_records.first[:explicit_hash_key] || '',
          data: Base64.encode64(data)
        }
        @user_records = []
        result
      end

      def num_user_records
        @user_records.length
      end

      private

      def aggregated_record
        AggregatedRecord.new(
          partition_key_table: @user_records.map { |r| r[:partition_key] },
          explicit_hash_key_table: explicit_hash_key_table,
          records: records
        )
      end

      def data
        bytes = AggregatedRecord.encode(aggregated_record)
        MAGIC + bytes + Digest::MD5.digest(bytes)
      end

      def explicit_hash_key_table
        @user_records.map { |r| r[:explicit_hash_key] }.compact
      end

      def records
        @user_records.map.with_index do |user_record, index|
          record = Record.new(
            partition_key_index: index,
            data: user_record[:data]
          )
          record.explicit_hash_key_index = index if user_record[:explicit_hash_key]
          record
        end

      end
    end
  end
end
