IMPORT Parquet;
IMPORT MONGODB;



airbnbRec := RECORD
    STRING _id;
    UTF8 name;
    INTEGER beds;
END;

dataset(airbnbRec) getAll() := EMBED(mongodb : user('hpccuser'), password('hpcc1'), server('cluster0.qbvdaak.mongodb.net'), database('sample_airbnb'),  collection('listingsAndReviews'))
    find({});
ENDEMBED; 

simpleDataset := getAll();
locationRec := {STRING place, REAL balance};
simpleRec := RECORD
    INTEGER num;
    STRING name;
    locationRec location;
END; 
// simpleDataset := DATASET([{1, 'Jack'}, {2, 'Alex'}, {3, 'Lilly'}, {4, 'Kirsten'}, {5, 'Glenn'}], simpleRec);
// simpleDataset := DATASET([{1, 'Jack', {'US', 3.14}}, {2, 'Alex', {'BR', 6.92}}, {3, 'Lilly', {'US', 6.81}}, {4, 'Kirsten', {'EU', 9.15}}, {5, 'Glenn', {'EU', 9.15}}], simpleRec);

write_rec(dataset(airbnbRec) sd) := EMBED(parquet: option('write'), destination('/home/hpccuser/dev/test_data/simple.parquet'))
ENDEMBED;

DATASET(airbnbRec) read_rec() := EMBED(parquet: option('read'), location('/home/hpccuser/dev/test_data/simple.parquet'))
ENDEMBED;

SEQUENTIAL(
    write_rec(simpleDataset),
    OUTPUT(read_rec(), NAMED('SIMPLE_PARQUET_IO'))
);
