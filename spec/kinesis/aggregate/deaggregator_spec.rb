RSpec.describe Kinesis::Aggregation::Deaggregator do
  subject(:records) { described_class.new(raw_record).deaggregate }
  context 'Given an aggregated record with EHK values' do
    let(:raw_record) do
      {
        kinesis: {
          kinesisSchemaVersion: '1.0',
          partitionKey: 'a',
          sequenceNumber: '49581544954143914480256700934210366840104395343224897586',
          approximateArrivalTimestamp: 1518133507.063,
          data: <<~DATA
          84mawgokZmMwM2RkODgtM2U3OS00NDhhLWIwMWEtN2NmMWJkNDdiNzg0CiRjYWU0MWIxYy1lYTYxLTQzZjItOTBiZS1iODc1NWViZj
          g4ZTIKJGQ0OTA2OTBjLWU3NGQtNGRiMi1hM2M4LWQ4ZjJmMTg0ZmQyMwokYzkyNGJjMDktYjg1ZS00N2YxLWIzMmUtMzM2NTIyZWU1
          M2M4EiYzODQ4NjQ5NTg2NzUwODM5OTA3ODE1OTcyMzg0NjA1MTgwNzAyMBInMTkzNzg3NjAwMDM3NjgxNzA2OTUyMTQzMzU3MDcxOT
          E2MzUyNjA0EicyNjY4ODA0MzY5NjQ5MzI0MjQyNjU0NjY5MTY3MzQwNjg2ODQ0MzkSJzMzOTYwNjYwMDk0Mjk2NzM5MTg1NDYwMzU1
          MjQwMjAyMTg0NzI5MhomCAAQABogUkVDT1JEIDIyIHBlZW9iaGN6YnpkbXNrYm91cGd5cQoaJggBEAEaIFJFQ09SRCAyMyB1c3dreG
          Z0eHJvZXVzc2N4c2pobm8KGiYIAhACGiBSRUNPUkQgMjQgY2FzZWhkZ2l2ZmF4ZXVzdGx5c3p5ChomCAMQAxogUkVDT1JEIDI1IG52
          ZmZ2cG11b2dkb3BqaGFtZXZyawpRwVPQ3go0yp4Y6kvM0q3V
          DATA
        },
        eventSource: 'aws:kinesis',
        eventVersion: '1.0',
        eventID: 'shardId-000000000003:49581544954143914480256700934210366840104395343224897586',
        eventName: 'aws:kinesis:record',
        invokeIdentityArn: 'arn:aws:iam::857565855790:role/service-role/Python27KinesisDeaggregatorRole',
        awsRegion: 'us-east-1',
        eventSourceARN: 'arn:aws:kinesis:us-east-1:857565855790:stream/AggRecordStream'
      }
    end

    let(:actual_user_records) do
      [
        {
          partitionKey: 'fc03dd88-3e79-448a-b01a-7cf1bd47b784',
          explicitHashKey: '38486495867508399078159723846051807020',
          data: "RECORD 22 peeobhczbzdmskboupgyq\n"
        },
        {
          partitionKey: 'cae41b1c-ea61-43f2-90be-b8755ebf88e2',
          explicitHashKey: '193787600037681706952143357071916352604',
          data: "RECORD 23 uswkxftxroeusscxsjhno\n"
        },
        {
          partitionKey: 'd490690c-e74d-4db2-a3c8-d8f2f184fd23',
          explicitHashKey: '266880436964932424265466916734068684439',
          data: "RECORD 24 casehdgivfaxeustlyszy\n"
        },
        {
          partitionKey: 'c924bc09-b85e-47f1-b32e-336522ee53c8',
          explicitHashKey: '339606600942967391854603552402021847292',
          data: "RECORD 25 nvffvpmuogdopjhamevrk\n"
        }
      ]
    end

    it 'copies static attributes' do
      expect(records).to all match a_hash_including(eventSource: 'aws:kinesis')
      expect(records).to all match a_hash_including(eventVersion: '1.0')
      expect(records).to all match a_hash_including(eventID: 'shardId-000000000003:49581544954143914480256700934210366840104395343224897586')
      expect(records).to all match a_hash_including(eventName: 'aws:kinesis:record')
      expect(records).to all match a_hash_including(invokeIdentityArn: 'arn:aws:iam::857565855790:role/service-role/Python27KinesisDeaggregatorRole')
      expect(records).to all match a_hash_including(awsRegion: 'us-east-1')
      expect(records).to all match a_hash_including(eventSourceARN: 'arn:aws:kinesis:us-east-1:857565855790:stream/AggRecordStream')
    end

    it 'has the correct kinesisSchemaVersion' do
      expect(records).to all match a_hash_including(kinesis: a_hash_including(kinesisSchemaVersion: raw_record[:kinesis][:kinesisSchemaVersion]))
    end

    it 'has the correct sequenceNumber' do
      expect(records).to all match a_hash_including(kinesis: a_hash_including(sequenceNumber: raw_record[:kinesis][:sequenceNumber]))
    end

    it 'has the correct approximateArrivalTimestamp' do
      expect(records).to all match a_hash_including(kinesis: a_hash_including(approximateArrivalTimestamp: raw_record[:kinesis][:approximateArrivalTimestamp]))
    end

    it 'has the correct number of records' do
      expect(records.length).to eq 4
    end

    it 'has the correct explicitHashKey' do
      expect(records[0]).to match a_hash_including(kinesis: a_hash_including(explicitHashKey: actual_user_records[0][:explicitHashKey]))
      expect(records[1]).to match a_hash_including(kinesis: a_hash_including(explicitHashKey: actual_user_records[1][:explicitHashKey]))
      expect(records[2]).to match a_hash_including(kinesis: a_hash_including(explicitHashKey: actual_user_records[2][:explicitHashKey]))
      expect(records[3]).to match a_hash_including(kinesis: a_hash_including(explicitHashKey: actual_user_records[3][:explicitHashKey]))
    end

    it 'has the correct partitionKey' do
      expect(records[0]).to match a_hash_including(kinesis: a_hash_including(partitionKey: actual_user_records[0][:partitionKey]))
      expect(records[1]).to match a_hash_including(kinesis: a_hash_including(partitionKey: actual_user_records[1][:partitionKey]))
      expect(records[2]).to match a_hash_including(kinesis: a_hash_including(partitionKey: actual_user_records[2][:partitionKey]))
      expect(records[3]).to match a_hash_including(kinesis: a_hash_including(partitionKey: actual_user_records[3][:partitionKey]))
    end

    it 'has the correct data' do
      expect(records[0]).to match a_hash_including(kinesis: a_hash_including(data: actual_user_records[0][:data]))
      expect(records[1]).to match a_hash_including(kinesis: a_hash_including(data: actual_user_records[1][:data]))
      expect(records[2]).to match a_hash_including(kinesis: a_hash_including(data: actual_user_records[2][:data]))
      expect(records[3]).to match a_hash_including(kinesis: a_hash_including(data: actual_user_records[3][:data]))
    end
  end

  context 'Given an aggregated record with no EHK values' do
    let(:raw_record) do
      {
        kinesis: {
          eventVersion: '1.0',
          eventID: 'shardId-000000000003:49581544954143914480259384464784359468274789083712061490',
          approximateArrivalTimestamp: 1520372221.837,
          partitionKey: 'a',
          kinesisSchemaVersion: '1.0',
          sequenceNumber: '49581544954143914480259384464784359468274789083712061490',
          data: <<~DATA
            84mawgokNzhmZWIxMmEtZGJhMC00NWNhLWE5MWUtYmIxZjJmMTgxOWI0CiRjMmE3NTc4Ny00NjliLTQzMTAtODQwZC1kNDg2ZGNhN2
            ViNWUKJDY1MGU5MzYyLTU3MmItNDQyNy1iM2ZjLTEzNTQ5ZDdlNWFlNQokMWQ4ZDk2MDAtMDBiNy00NmYzLWE5ODMtZGU4MzQ3NzU0
            MGMwCiRmMmU3MThhMC0zODliLTQ5NGEtYjc5Ni0zMzU1YjA3NTY5Y2MKJDk2NWI4ZGE0LWE2YmQtNDc1NS04MWM5LWU3MTgxYWI3ZG
            M5YQokZDY5M2M1ZjAtNTc3Mi00NmM5LThkODYtMDhjNzA1NWJkYjc1CiQ4ZDkyNDc2Yi1lYjg5LTRlODEtOTlmYi1jMmJhNjNhZDZk
            OTAaJAgAGiBSRUNPUkQgMTggaWtscnBzdnloeG5lcm9kZmVoZXJ1ChokCAEaIFJFQ09SRCAxOSBsbm1udWR5aHZwdmNncm9rZm16ZG
            gKGiQIAhogUkVDT1JEIDIwIHBqcXJjZmJ4YWZ6ZG5kem1iZ2FuZQoaJAgDGiBSRUNPUkQgMjEgYWFmcmV5cm50d3V3eGxpeWdrYmhr
            ChokCAQaIFJFQ09SRCAyMiBxc2FhZGhic2dtdnFnZGlmZ3V5b3UKGiQIBRogUkVDT1JEIDIzIHdqb2tvaXd2enpobGZ0bHpocWZqaw
            oaJAgGGiBSRUNPUkQgMjQgem9wY2FzY3J0aGJicG5qaXd1aGpiChokCAcaIFJFQ09SRCAyNSB0eGJidm5venJ3c2JveHVvbXFib3EK
            vV1rrLdU+Sy2v8xkgZ5YaA==
          DATA
        },
        invokeIdentityArn: 'arn:aws:iam::857565855790:role/service-role/Python27KinesisDeaggregatorRole',
        eventName: 'aws:kinesis:record',
        eventSourceARN: 'arn:aws:kinesis:us-east-1:857565855790:stream/AggRecordStream',
        eventSource: 'aws:kinesis',
        awsRegion: 'us-east-1'
      }
    end

    let(:actual_user_records) do
      [
        {
          partitionKey: '78feb12a-dba0-45ca-a91e-bb1f2f1819b4',
          data: "RECORD 18 iklrpsvyhxnerodfeheru\n"
        },
        {
          partitionKey: 'c2a75787-469b-4310-840d-d486dca7eb5e',
          data: "RECORD 19 lnmnudyhvpvcgrokfmzdh\n"
        },
        {
          partitionKey: '650e9362-572b-4427-b3fc-13549d7e5ae5',
          data: "RECORD 20 pjqrcfbxafzdndzmbgane\n"
        },
        {
          partitionKey: '1d8d9600-00b7-46f3-a983-de83477540c0',
          data: "RECORD 21 aafreyrntwuwxliygkbhk\n"
        },
        {
          partitionKey: 'f2e718a0-389b-494a-b796-3355b07569cc',
          data: "RECORD 22 qsaadhbsgmvqgdifguyou\n"
        },
        {
          partitionKey: '965b8da4-a6bd-4755-81c9-e7181ab7dc9a',
          data: "RECORD 23 wjokoiwvzzhlftlzhqfjk\n"
        },
        {
          partitionKey: 'd693c5f0-5772-46c9-8d86-08c7055bdb75',
          data: "RECORD 24 zopcascrthbbpnjiwuhjb\n"
        },
        {
          partitionKey: '8d92476b-eb89-4e81-99fb-c2ba63ad6d90',
          data: "RECORD 25 txbbvnozrwsboxuomqboq\n"
        }
      ]
    end

    it 'has no explicitHashKey' do
      expect(records).to all match a_hash_including(kinesis: a_hash_including(explicitHashKey: nil))
    end

    it 'has the correct partitionKey'  do
      expect(records[0]).to match a_hash_including(kinesis: a_hash_including(partitionKey: actual_user_records[0][:partitionKey]))
      expect(records[1]).to match a_hash_including(kinesis: a_hash_including(partitionKey: actual_user_records[1][:partitionKey]))
      expect(records[2]).to match a_hash_including(kinesis: a_hash_including(partitionKey: actual_user_records[2][:partitionKey]))
      expect(records[3]).to match a_hash_including(kinesis: a_hash_including(partitionKey: actual_user_records[3][:partitionKey]))
      expect(records[4]).to match a_hash_including(kinesis: a_hash_including(partitionKey: actual_user_records[4][:partitionKey]))
      expect(records[5]).to match a_hash_including(kinesis: a_hash_including(partitionKey: actual_user_records[5][:partitionKey]))
      expect(records[6]).to match a_hash_including(kinesis: a_hash_including(partitionKey: actual_user_records[6][:partitionKey]))
      expect(records[7]).to match a_hash_including(kinesis: a_hash_including(partitionKey: actual_user_records[7][:partitionKey]))
    end

    it 'has the correct data'  do
      expect(records[0]).to match a_hash_including(kinesis: a_hash_including(data: actual_user_records[0][:data]))
      expect(records[1]).to match a_hash_including(kinesis: a_hash_including(data: actual_user_records[1][:data]))
      expect(records[2]).to match a_hash_including(kinesis: a_hash_including(data: actual_user_records[2][:data]))
      expect(records[3]).to match a_hash_including(kinesis: a_hash_including(data: actual_user_records[3][:data]))
      expect(records[4]).to match a_hash_including(kinesis: a_hash_including(data: actual_user_records[4][:data]))
      expect(records[5]).to match a_hash_including(kinesis: a_hash_including(data: actual_user_records[5][:data]))
      expect(records[6]).to match a_hash_including(kinesis: a_hash_including(data: actual_user_records[6][:data]))
      expect(records[7]).to match a_hash_including(kinesis: a_hash_including(data: actual_user_records[7][:data]))
    end
  end

  context 'Given a record aggregated as seen by the Kinesis Analytics preprocessor lambda function' do
    let(:raw_record) do
      {
        recordId: '49597411459012285111935017621194032819223185658889633794',
        kinesisStreamRecordMetadata: {
          approximateArrivalTimestamp: 1562602080951,
          sequenceNumber: '49597411459012285111935017621194032819223185658889633794',
          partitionKey: 'a',
          shardId: 'shardId-000000000000'
        },
        data: <<~DATA
          84mawgoNMTU2MjYwMjA3NDg5NhInMzM5NjA2NjAwOTQyOTY3MzkxODU0NjAzNTUyNDAyMDIxODQ3MjkyGicIABAAGiFSRUNPUkQgMjAwNSB
          wanFyY2ZieGFmemRuZHptYmdhbmUaJwgAEAAaIVJFQ09SRCAyMDA2IHBqcXJjZmJ4YWZ6ZG5kem1iZ2FuZRonCAAQABohUkVDT1JEIDIwMD
          cgcGpxcmNmYnhhZnpkbmR6bWJnYW5lGicIABAAGiFSRUNPUkQgMjAwOCBwanFyY2ZieGFmemRuZHptYmdhbmUHrLqXAgpJOT8v1lp81gM/
        DATA
      }
    end

    let(:actual_user_records) do
      [
        {
          explicitHashKey: '339606600942967391854603552402021847292',
          partitionKey: '1562602074896',
          data: 'RECORD 2005 pjqrcfbxafzdndzmbgane',
          recordId: '49597411459012285111935017621194032819223185658889633794'
        },
        {
          explicitHashKey: '339606600942967391854603552402021847292',
          partitionKey: '1562602074896',
          data: 'RECORD 2006 pjqrcfbxafzdndzmbgane',
          recordId: '49597411459012285111935017621194032819223185658889633794'
        },
        {
          explicitHashKey: '339606600942967391854603552402021847292',
          partitionKey: '1562602074896',
          data: 'RECORD 2007 pjqrcfbxafzdndzmbgane',
          recordId: '49597411459012285111935017621194032819223185658889633794'
        },
        {
          explicitHashKey: '339606600942967391854603552402021847292',
          partitionKey: '1562602074896',
          data: 'RECORD 2008 pjqrcfbxafzdndzmbgane',
          recordId: '49597411459012285111935017621194032819223185658889633794'
        }
      ]
    end

    it 'has the correct kinesisSchemaVersion' do
      expect(records).to all match a_hash_including(kinesis: a_hash_including(kinesisSchemaVersion: '1.0'))
    end

    it 'has the correct sequenceNumber' do
      expect(records).to all match a_hash_including(kinesis: a_hash_including(sequenceNumber: raw_record[:kinesisStreamRecordMetadata][:sequenceNumber]))
    end

    it 'has the correct approximateArrivalTimestamp' do
      expect(records).to all match a_hash_including(kinesis: a_hash_including(approximateArrivalTimestamp: raw_record[:kinesisStreamRecordMetadata][:approximateArrivalTimestamp]))
    end

    it 'has the correct partitonKey' do
      expect(records[0]).to match a_hash_including(kinesis: a_hash_including(partitionKey: actual_user_records[0][:partitionKey]))
      expect(records[1]).to match a_hash_including(kinesis: a_hash_including(partitionKey: actual_user_records[1][:partitionKey]))
      expect(records[2]).to match a_hash_including(kinesis: a_hash_including(partitionKey: actual_user_records[2][:partitionKey]))
      expect(records[3]).to match a_hash_including(kinesis: a_hash_including(partitionKey: actual_user_records[3][:partitionKey]))
    end

    it 'has the correct explicitHashKey' do
      expect(records[0]).to match a_hash_including(kinesis: a_hash_including(explicitHashKey: actual_user_records[0][:explicitHashKey]))
      expect(records[1]).to match a_hash_including(kinesis: a_hash_including(explicitHashKey: actual_user_records[1][:explicitHashKey]))
      expect(records[2]).to match a_hash_including(kinesis: a_hash_including(explicitHashKey: actual_user_records[2][:explicitHashKey]))
      expect(records[3]).to match a_hash_including(kinesis: a_hash_including(explicitHashKey: actual_user_records[3][:explicitHashKey]))
    end

    it 'has the correct data' do
      expect(records[0]).to match a_hash_including(kinesis: a_hash_including(data: actual_user_records[0][:data]))
      expect(records[1]).to match a_hash_including(kinesis: a_hash_including(data: actual_user_records[1][:data]))
      expect(records[2]).to match a_hash_including(kinesis: a_hash_including(data: actual_user_records[2][:data]))
      expect(records[3]).to match a_hash_including(kinesis: a_hash_including(data: actual_user_records[3][:data]))
    end

    it 'has the correct recordId' do
      expect(records[0]).to match a_hash_including(kinesis: a_hash_including(recordId: actual_user_records[0][:recordId]))
      expect(records[1]).to match a_hash_including(kinesis: a_hash_including(recordId: actual_user_records[1][:recordId]))
      expect(records[2]).to match a_hash_including(kinesis: a_hash_including(recordId: actual_user_records[2][:recordId]))
      expect(records[3]).to match a_hash_including(kinesis: a_hash_including(recordId: actual_user_records[3][:recordId]))
    end
  end

  context 'Given a non-aggregated record as seen by the Kinesis Analytics preprocessor lambda function' do
    let(:raw_record) do
      {
        recordId: '49597411459012285111935017620416693517210978893395132418',
        kinesisStreamRecordMetadata: {
          approximateArrivalTimestamp: 1562602078827,
          sequenceNumber: '49597411459012285111935017620416693517210978893395132418',
          partitionKey: '1562602074896',
          shardId: 'shardId-000000000000'
        },
        data: 'UkVDT1JEIDc0OSBwanFyY2ZieGFmemRuZHptYmdhbmU='
      }
    end

    it 'returns the record' do
      expect(records.length).to eq 1
      expect(records[0]).to eq(
        kinesis: {
          kinesisSchemaVersion: '1.0',
          sequenceNumber: raw_record[:kinesisStreamRecordMetadata][:sequenceNumber],
          partitionKey: raw_record[:kinesisStreamRecordMetadata][:partitionKey],
          approximateArrivalTimestamp: raw_record[:kinesisStreamRecordMetadata][:approximateArrivalTimestamp],
          shardId: raw_record[:kinesisStreamRecordMetadata][:shardId],
          data: raw_record[:data],
          recordId: raw_record[:recordId]
        }
      )
    end
  end
end
