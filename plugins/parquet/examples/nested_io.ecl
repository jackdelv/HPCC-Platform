IMPORT Parquet;

friendsRec :=RECORD
    UNSIGNED4 age;
    INTEGER2 friends;
    SET OF STRING friendsList;
END;

childRec := RECORD
     friendsRec friends;
     REAL height;
     REAL weight;
END;

parentRec := RECORD
    UTF8_de firstname;
	UTF8_de lastname;
    childRec details;
END; 
nested_dataset := DATASET([{'Jack', 'Jackson', { {22, 2, ['James', 'Jonathon']}, 5.9, 600}}, {'John', 'Johnson', { {17, 0, []}, 6.3, 18}}, 
                                {'Amy', 'Amyson', { {59, 1, ['Andy']}, 3.9, 59}}, {'Grace', 'Graceson', { {11, 3, ['Grayson', 'Gina', 'George']}, 7.9, 100}}], parentRec);

#IF(0)
Write(nested_dataset, '/datadrive/dev/test_data/nested.parquet');
#END

#IF(1)
read_in := Read(parentRec, '/datadrive/dev/test_data/nested.parquet');
OUTPUT(read_in, NAMED('NESTED_PARQUET_IO'));
#END