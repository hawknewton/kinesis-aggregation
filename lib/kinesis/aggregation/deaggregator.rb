require 'base64'
require 'digest'
require 'active_support/core_ext/hash/indifferent_access'

module Kinesis
  module Aggregation
    class Deaggregator
      MAGIC = "\xf3\x89\x9a\xc2".force_encoding('ASCII-8BIT').freeze
      DIGEST_SIZE = 16

      def initialize(raw_record)
        @raw_record = raw_record.with_indifferent_access
      end

      def deaggregate
        return [kinesis_record] unless aggregated_record? && computed_md5 == kinesis_record_md5

        aggregated_record.records.map do |record|
          base_record.merge(
            kinesis: {
              kinesisSchemaVersion: kinesis_record[:kinesis][:kinesisSchemaVersion],
              sequenceNumber: kinesis_record[:kinesis][:sequenceNumber],
              approximateArrivalTimestamp: kinesis_record[:kinesis][:approximateArrivalTimestamp],
              explicitHashKey: explicit_hash_for_for(record),
              partitionKey: partition_key_for(record),
              data: record.data,
              recordId: kinesis_record[:kinesis][:recordId]
            }
          )
        end
      end

      private
      def aggregated_record?
        data[0..MAGIC.length - 1] == MAGIC
      end

      def aggregated_record
        @aggregated_record ||= AggregatedRecord.decode(kinesis_record_message_data)
      end

      def base_record
        kinesis_record.reject { |k, _v| k == :kinesis }
      end

      def data
        @data ||= Base64.decode64(kinesis_record[:kinesis][:data])
      end

      def explicit_hash_for_for(record)
        aggregated_record.explicit_hash_key_table[record.explicit_hash_key_index]
      end

      def kinesis_record
        @kinesis_record ||= begin
                            if @raw_record.has_key?(:kinesisStreamRecordMetadata)
                              KinesisAnalyticsConverter.new(@raw_record).convert
                            else
                              @raw_record
                            end
                          end
      end

      def partition_key_for(record)
        aggregated_record.partition_key_table[record.partition_key_index]
      end

      def kinesis_record_md5
        data[data.length - 16..-1]
      end

      def kinesis_record_message_data
        data[MAGIC.length..-17]
      end

      def computed_md5
        Digest::MD5.digest(kinesis_record_message_data)
      end
    end
  end
end
