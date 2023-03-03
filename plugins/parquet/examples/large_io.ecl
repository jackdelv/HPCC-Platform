IMPORT STD;
IMPORT Parquet;

// #OPTION('outputLimit', 20000);

layout := RECORD
    STRING actor_login;
    INTEGER actor_id;
    INTEGER comment_id;
    STRING comment;
    STRING repo;
    STRING language;
    STRING author_login;
    INTEGER author_id;
    INTEGER pr_id;
    INTEGER c_id;
    INTEGER commit_date;
END;

#IF(0)
write_rec(streamed DATASET(layout) sd) := EMBED(parquet: activity, option('write'), destination('/datadrive/dev/test_data/ghtorrent-2019-01-07.parquet'))
ENDEMBED;

csv_data := DATASET('~parquet::large::ghtorrent-2019-02-04.csv', layout, CSV(HEADING(1)));
write_rec(csv_data);
#END

#IF(1)
DATASET(layout) read_rec() := EMBED(parquet: option('read'), location('/datadrive/dev/test_data/ghtorrent-2019-01-07.parquet'))
ENDEMBED;

parquet_data := read_rec();
OUTPUT(COUNT(parquet_data), NAMED('ghtorrent_2019_01_07'));
#END