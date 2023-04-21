E = LOAD '$G' USING PigStorage(',') AS (userID:double, rating:double);
D = FILTER E BY userID is not null and rating is not null;
F = GROUP D BY userID;
G = foreach F generate group,FLOOR(SUM(D.rating)/COUNT(D) *10);
H = GROUP G BY $1;
I = foreach H generate ROUND_TO(group/10,1), COUNT($1);
STORE I INTO '$O' USING PigStorage (' ');
