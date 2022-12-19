IMPORT Parquet;

simpleRec := RECORD
    INTEGER num;
    STRING name;
    STRING place;
    REAL balance;
END;

simpleDataset := DATASET([{1, 'Jack', 'US', 3.14}, {2, 'Alex', 'BR', 6.92}, {3, 'Lilly', 'US', 6.81}, {4, 'Kirsten', 'EU', 9.15}, {5, 'Glenn', 'EU', 9.15}],
                    simpleRec);

write_int(dataset(simpleRec) sd) := EMBED(parquet: option('write'), destination('/home/hpccuser/dev/test_data/numbers.parquet'))
ENDEMBED;

INTEGER read_int() := EMBED(parquet: option('read'), location('/home/hpccuser/dev/test_data/numbers.parquet'), destination('/home/hpccuser/dev/test_data/output/numbers.csv'))
ENDEMBED;

SEQUENTIAL(
    write_int(simpleDataset),
    OUTPUT(read_int(), NAMED('SIMPLE_PARQUET_IO'))
);
