IMPORT Parquet;

write_int() := EMBED(parquet: option('write'), destination('/home/hpccuser/dev/test_data/numbers.parquet'))
    {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
ENDEMBED;

INTEGER read_int() := EMBED(parquet: option('read'), location('/home/hpccuser/dev/test_data/numbers.parquet'), destination('/home/hpccuser/dev/test_data/output/numbers.csv'));
ENDEMBED;

SEQUENTIAL(
    write_int(),
    OUTPUT(read_int(), NAMED('SIMPLE_PARQUET_IO'))
);
