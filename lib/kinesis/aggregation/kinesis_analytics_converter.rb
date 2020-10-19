class KinesisAnalyticsConverter
  def initialize(kinesis_analytics_record)
    @kinesis_analytics_record = kinesis_analytics_record
  end

  def convert
    {
      kinesis: {
        kinesisSchemaVersion: '1.0',
        sequenceNumber: @kinesis_analytics_record[:kinesisStreamRecordMetadata][:sequenceNumber],
        partitionKey: @kinesis_analytics_record[:kinesisStreamRecordMetadata][:partitionKey],
        approximateArrivalTimestamp: @kinesis_analytics_record[:kinesisStreamRecordMetadata][:approximateArrivalTimestamp],
        shardId: @kinesis_analytics_record[:kinesisStreamRecordMetadata][:shardId],
        data: @kinesis_analytics_record[:data],
        recordId: @kinesis_analytics_record[:recordId]
      }
    }
  end
end
