RSpec.describe Kinesis::Aggregation::Aggregator do

  it 'can aggregate user records with EHK' do
    expected_data = <<~DATA
      84mawgokZmMwM2RkODgtM2U3OS00NDhhLWIwMWEtN2NmMWJkNDdiNzg0CiRjYWU0MWIxYy1lYTYxLTQz
      ZjItOTBiZS1iODc1NWViZjg4ZTIKJGQ0OTA2OTBjLWU3NGQtNGRiMi1hM2M4LWQ4ZjJmMTg0ZmQyMwokYzkyNGJjMDktYjg1ZS00N2
      YxLWIzMmUtMzM2NTIyZWU1M2M4EiYzODQ4NjQ5NTg2NzUwODM5OTA3ODE1OTcyMzg0NjA1MTgwNzAyMBInMTkzNzg3NjAwMDM3Njgx
      NzA2OTUyMTQzMzU3MDcxOTE2MzUyNjA0EicyNjY4ODA0MzY5NjQ5MzI0MjQyNjU0NjY5MTY3MzQwNjg2ODQ0MzkSJzMzOTYwNjYwMD
      k0Mjk2NzM5MTg1NDYwMzU1MjQwMjAyMTg0NzI5MhomCAAQABogUkVDT1JEIDIyIHBlZW9iaGN6YnpkbXNrYm91cGd5cQoaJggBEAEa
      IFJFQ09SRCAyMyB1c3dreGZ0eHJvZXVzc2N4c2pobm8KGiYIAhACGiBSRUNPUkQgMjQgY2FzZWhkZ2l2ZmF4ZXVzdGx5c3p5ChomCA
      MQAxogUkVDT1JEIDI1IG52ZmZ2cG11b2dkb3BqaGFtZXZyawpRwVPQ3go0yp4Y6kvM0q3V
    DATA
    aggregator = described_class.new

    aggregator.add_user_record(partition_key: 'fc03dd88-3e79-448a-b01a-7cf1bd47b784',
                               explicit_hash_key: '38486495867508399078159723846051807020',
                               data: "RECORD 22 peeobhczbzdmskboupgyq\n")
    expect(aggregator.num_user_records).to eq 1

    aggregator.add_user_record(partition_key: 'cae41b1c-ea61-43f2-90be-b8755ebf88e2',
                               explicit_hash_key: '193787600037681706952143357071916352604',
                               data: "RECORD 23 uswkxftxroeusscxsjhno\n")
    expect(aggregator.num_user_records).to eq 2

    aggregator.add_user_record(partition_key: 'd490690c-e74d-4db2-a3c8-d8f2f184fd23',
                               explicit_hash_key: '266880436964932424265466916734068684439',
                               data: "RECORD 24 casehdgivfaxeustlyszy\n")
    expect(aggregator.num_user_records).to eq 3

    aggregator.add_user_record(partition_key: 'c924bc09-b85e-47f1-b32e-336522ee53c8',
                               explicit_hash_key: '339606600942967391854603552402021847292',
                               data: "RECORD 25 nvffvpmuogdopjhamevrk\n")
    expect(aggregator.num_user_records).to eq 4

    aggregated_record = aggregator.aggregate!
    expect(aggregated_record).to match a_hash_including(partition_key: 'fc03dd88-3e79-448a-b01a-7cf1bd47b784')
    expect(aggregated_record).to match a_hash_including(explicit_hash_key: '38486495867508399078159723846051807020')
    expect(Base64.decode64(aggregated_record[:data])).to eq Base64.decode64(expected_data)
  end

  it 'can aggrete user records without EHK' do
    expected_data = <<~DATA
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

    aggregator = described_class.new

    aggregator.add_user_record(partition_key: '78feb12a-dba0-45ca-a91e-bb1f2f1819b4', data: "RECORD 18 iklrpsvyhxnerodfeheru\n")
    expect(aggregator.num_user_records).to eq 1

    aggregator.add_user_record(partition_key: 'c2a75787-469b-4310-840d-d486dca7eb5e', data: "RECORD 19 lnmnudyhvpvcgrokfmzdh\n")
    expect(aggregator.num_user_records).to eq 2

    aggregator.add_user_record(partition_key: '650e9362-572b-4427-b3fc-13549d7e5ae5', data: "RECORD 20 pjqrcfbxafzdndzmbgane\n")
    expect(aggregator.num_user_records).to eq 3

    aggregator.add_user_record(partition_key: '1d8d9600-00b7-46f3-a983-de83477540c0', data: "RECORD 21 aafreyrntwuwxliygkbhk\n")
    expect(aggregator.num_user_records).to eq 4

    aggregator.add_user_record(partition_key: 'f2e718a0-389b-494a-b796-3355b07569cc', data: "RECORD 22 qsaadhbsgmvqgdifguyou\n")
    expect(aggregator.num_user_records).to eq 5

    aggregator.add_user_record(partition_key: '965b8da4-a6bd-4755-81c9-e7181ab7dc9a', data: "RECORD 23 wjokoiwvzzhlftlzhqfjk\n")
    expect(aggregator.num_user_records).to eq 6

    aggregator.add_user_record(partition_key: 'd693c5f0-5772-46c9-8d86-08c7055bdb75', data: "RECORD 24 zopcascrthbbpnjiwuhjb\n")
    expect(aggregator.num_user_records).to eq 7

    aggregator.add_user_record(partition_key: '8d92476b-eb89-4e81-99fb-c2ba63ad6d90', data: "RECORD 25 txbbvnozrwsboxuomqboq\n")
    expect(aggregator.num_user_records).to eq 8

    aggregated_record = aggregator.aggregate!
    expect(aggregated_record).to match a_hash_including(partition_key: '78feb12a-dba0-45ca-a91e-bb1f2f1819b4')
    expect(aggregated_record[:explicit_hash_key]).to eq ''

    expect(Base64.decode64(aggregated_record[:data])).to eq Base64.decode64(expected_data)
  end
end
