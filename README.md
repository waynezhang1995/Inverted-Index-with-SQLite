# Inverted-Index-With-SQLite

## Deprecated

## About
Simple search engine emulator using the TF-IDF indexing scheme. This project about implementing an inverted index using a relational database (e.g. SQLite) and Java. The idea is to use the B-Tree data structure offered by a relational database instead of building it from the scratch.

Implementation based on “Using a Relational Database for an Inverted Text Index” by Steve Putz

## Usage (Build Steps):

Clone this repo
```
git clone https://github.com/waynezhang1995/Inverted-Index-with-SQLite.git
```

cd into the project directory, and then create posting table
```
sqlite3 reuters.db < table_creation.sql
```

Compile Java source file
```
javac -cp "lib/*" -d bin src/*.java
```



