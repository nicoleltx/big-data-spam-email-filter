lines = LOAD '/home/training/Desktop/data/*' AS (line:chararray);
clean1 = FOREACH lines GENERATE REPLACE(line,'[^a-zA-Z\\s]+','') AS (line:chararray);
clean2 = FOREACH clean1 GENERATE REPLACE(line,'\\s[a-zA-Z]\\s','') AS (line:chararray);
words = FOREACH clean2 GENERATE FLATTEN(TOKENIZE(line)) AS word;
grouped = GROUP words BY word;
wordcount = FOREACH grouped GENERATE group, COUNT(words) AS (count:long);
ordered_wordcount = ORDER wordcount BY count DESC;
STORE ordered_wordcount INTO 'total_out';

lines = LOAD '/home/training/Desktop/data/normal/*' AS (line:chararray);
clean1 = FOREACH lines GENERATE REPLACE(line,'[^a-zA-Z\\s]+','') AS (line:chararray);
clean2 = FOREACH clean1 GENERATE REPLACE(line,'\\s[a-zA-Z]\\s','') AS (line:chararray);
words = FOREACH clean2 GENERATE FLATTEN(TOKENIZE(line)) AS word;
grouped = GROUP words BY word;
wordcount = FOREACH grouped GENERATE group, COUNT(words) AS (count:long);
ordered_wordcount = ORDER wordcount BY count DESC;
STORE ordered_wordcount INTO 'normal_out';

lines = LOAD '/home/training/Desktop/data/spam/*' AS (line:chararray);
clean1 = FOREACH lines GENERATE REPLACE(line,'[^a-zA-Z\\s]+','') AS (line:chararray);
clean2 = FOREACH clean1 GENERATE REPLACE(line,'\\s[a-zA-Z]\\s','') AS (line:chararray);
words = FOREACH clean2 GENERATE FLATTEN(TOKENIZE(line)) AS word;
grouped = GROUP words BY word;
wordcount = FOREACH grouped GENERATE group, COUNT(words) AS (count:long);
ordered_wordcount = ORDER wordcount BY count DESC;
STORE ordered_wordcount INTO 'spam_out';
