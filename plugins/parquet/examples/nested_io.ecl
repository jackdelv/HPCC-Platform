IMPORT Parquet;

childRec := RECORD
     INTEGER age;
     REAL height;
     REAL weight;
END;

parentRec := RECORD
    UTF8_de firstname;
	UTF8_de lastname;
    childRec details;
END; 
nested_dataset := DATASET([{'Jack', 'Jackson', {22, 5.9, 600}}, {'John', 'Johnson', {17, 6.3, 18}}, 
                                {'Amy', 'Amyson', {59, 5.9, 59}}, {'Grace', 'Graceson', {11, 7.9, 100}}], parentRec);

write_rec(dataset(parentRec) sd) := EMBED(parquet: option('write'), MaxRowSize(2), destination('/home/hpccuser/dev/test_data/nested.parquet'))
ENDEMBED;

DATASET(parentRec) read_rec() := EMBED(parquet: option('read'), location('/home/hpccuser/dev/test_data/nested.parquet'))
ENDEMBED;

SEQUENTIAL(
    write_rec(nested_dataset),
    OUTPUT(read_rec(), NAMED('NESTED_PARQUET_IO'))
);