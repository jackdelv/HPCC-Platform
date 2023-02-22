IMPORT Parquet;

simpleRec := RECORD
    INTEGER num;
		REAL balance;
		UTF8_de lastname;
    STRING name;
END; 
simpleDataset := DATASET([{1, 2.4356, U'de\3531', 'Jack'}, {1, 2.4356, U'de\3531', 'Jack'}, {2, 4.8937, U'as\352df', 'John'}, {3, 1.8573, 'nj\351vf', 'Jane'}, {4, 9.1235, U'ds\354fg', 'Jill'}, {5, 6.3297, U'po\355gm', 'Jim'}], simpleRec);

write_rec(dataset(simpleRec) sd) := EMBED(parquet: option('write'), MaxRowSize(2), destination('/home/hpccuser/dev/test_data/simple.parquet'))
ENDEMBED;

DATASET(simpleRec) read_rec() := EMBED(parquet: option('read'), location('/home/hpccuser/dev/test_data/simple.parquet'))
ENDEMBED;

SEQUENTIAL(
    write_rec(simpleDataset),
    OUTPUT(read_rec(), NAMED('SIMPLE_PARQUET_IO'))
);