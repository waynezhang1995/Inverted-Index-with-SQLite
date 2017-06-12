CREATE TABLE postings (
   word VARCHAR2(100) PRIMARY KEY,
   df INT, 
   docidfreqbytestream BLOB
);

CREATE TABLE docmag (
   docid INT PRIMARY KEY,
   vectormagnitude FLOAT,
   maxf INT
);

