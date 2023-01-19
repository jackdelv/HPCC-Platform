IMPORT Parquet;

locationRec := {STRING place, REAL balance};
simpleRec := RECORD
    INTEGER num;
    STRING name;
END; 
simpleDataset := DATASET([{1, 'Jack'}, {2, 'Alex'}, {3, 'Lilly'}, {4, 'Kirsten'}, {5, 'Glenn'}], simpleRec);
// simpleDataset := DATASET([{1, 'Jack', {'US', 3.14}}, {2, 'Alex', {'BR', 6.92}}, {3, 'Lilly', {'US', 6.81}}, {4, 'Kirsten', {'EU', 9.15}}, {5, 'Glenn', {'EU', 9.15}}], simpleRec);

write_rec(dataset(simpleRec) sd) := EMBED(parquet: option('write'), destination('/home/hpccuser/dev/test_data/simple.parquet'))
ENDEMBED;

DATASET(simpleRec) read_rec() := EMBED(parquet: option('read'), location('/home/hpccuser/dev/test_data/simple.parquet'))
ENDEMBED;

SEQUENTIAL(
    write_rec(simpleDataset),
    OUTPUT(read_rec(), NAMED('SIMPLE_PARQUET_IO'))
);
