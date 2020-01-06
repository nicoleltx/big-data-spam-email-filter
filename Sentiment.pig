
Register 'json-simple-1.1.1.jar'
Register 'elephant-bird-pig-4.1.jar'
Register 'elephant-bird-hadoop-compat-4.1.jar'

spamA = LOAD '/spamJson/' USING com.twitter.elephantbird.pig.load.JsonLoader('-nestedLoad') AS myMap;


extract_details = FOREACH spamA GENERATE myMap#'name' as name,myMap#'text' as text;




tokens = foreach extract_details generate name,text, FLATTEN(TOKENIZE(text)) As word;

dictionary = load '/AFINN-en-165.txt' using PigStorage('\t') AS(word:chararray,rating:int);


word_rating = join tokens by word left outer, dictionary by word using 'replicated';

rating = foreach word_rating generate tokens::name as name,tokens::text as text, dictionary::rating as rate;


word_group = group rating by (name,text);

avg_rate = foreach word_group generate group.name, AVG(rating.rate) as spam_rating;


STORE avg_rate INTO '/result/spamSentiment' ;












