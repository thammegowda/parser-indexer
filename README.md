Nutch Tika Solr
================

This project contains course work of 'Information Retrieval and web Search Engines' (CSCI 572) 
course of University of Southern California.
The main theme of this project is building inverted index using `Apache Lucene/Solr`. The data is crawled from web
using `Apache Nutch` and it is read from segments using `Apache Hadoop-HDFS` API.
Additional enrichment to documents is made by parsing documents with `Apache Tika`.

NOTE : Visit [Step By Step Guide](./step-by-step.txt) for knowing how to make use of this code.


# Requirements 
+ JDK 1.8  (we faced issues with Open JDK 8, so please use Sun JDK 1.8)
+ Newer version of Maven (used 3.3)
+ Internet connection to download maven dependencies

# Additional Setup 
During the course of this project, we enhanced `Apache Tika` by adding a `NamedEntityParser` and supplied an
implementation of Named Entity Recogniser based on _StanfordCoreNLP_.  Edit: Its now part of Tika 1.12, pull the latest version of tika (>= 1.12)

+ Build Tika CoreNlp addon NLP
  + `git clone git@github.com:thammegowda/tika-ner-corenlp.git`
  + `mvn install`

# How to build.

After completing the _Additional Setup_ process, the build is as simple as running the following command:
 `mvn clean package`

 This command should produce a jar at `target/nutch-tika-solr*.jar`. Use this to run in the next step

# How to run

Run `java -jar target/nutch-tika-solr-1.0-SNAPSHOT.jar` command. It will show sub commands

This project offers sub commands.

  ```
  Usage : Main <CMD>
  The following command(CMD)s are available
         index :  Index nutch segments to solr
         graph :  Builds a graph of documents, and writes the edges set to file
      pagerank :  Computes page rank for nodes in graph
      phase2parse :  Pharses the text content for NER and updates index
      updaterank :  Updates Page rank
  ```

  + **index** Command

    This command loads nutch segment content to solr, parses metadata using tika.

    Usage :
    ```
    java -jar target/nutch-tika-solr-1.0-SNAPSHOT.jar index
     -batch (--batch-size) N  : Number of documents to buffer and post to solr
                                (default: 1000)
     -segs (--seg-paths) FILE : Path to a text file containing segment paths. One
                                path per line
     -url (--solr-url) URL    : Solr url
    ```

    Example :
    ```
    java -jar target/nutch-tika-solr-1.0-SNAPSHOT.jar index -batch 300 \
      -segs data/paths-all.txt \
      -url http://localhost:8983/solr
    ```

  + **graph** Command

    This command creates graph of documents in solr index. The edges will be written to text file on disk

    Usage :
    ```
      java -jar target/nutch-tika-solr-1.0-SNAPSHOT.jar graph
       -edge [locations | persons | dates |   : Edge type. This should be a field in
        organizations]                           solr docs.
       -out FILE                              : Output File for writing the edges of
                                                 graph.
       -solr URL                              : Solr URL to query docs
    ```

    Example :
    ```
      java -jar target/nutch-tika-solr-1.0-SNAPSHOT.jar graph \
        -solr http://localhost:8983/solr/  \
        -out locations.txt -edge locations
    ```
  + **pagerank** Command

      This command takes graph configuration in the form of edges, computes pagerank and outputs the ranks
       to a file

    Usage :
    ```
      java -jar target/nutch-tika-solr-1.0-SNAPSHOT.jar pagerank
       -edges FILE : Path to file having graph's edges
        -n N        : Number of Iteration
        -out FILE   : Path to Output file for storing page ranks
        -t N        : Number of Threads (default: 2)
    ```

    Example :
    ```
      java -jar target/nutch-tika-solr-1.0-SNAPSHOT.jar pagerank \
        -edges locations.txt -n 5 -out pr-loc.txt
    ```

  + **phase2parse** command

    This is a sub command for running phase 2 parser. In this phase docs from a
    solr core (indexed from 'nutch index' command) are imported, Named entity parser is run to extract
    names of people, locations, organizations, and also dates, weapon names, weapon types. The result is
    updated to another solr core.

    Usage :
    ```
    $ java -jar target/nutch-tika-solr-1.0-SNAPSHOT.jar phase2parse \
     -batch (--batch-size) N : Number of documents to buffer and post to solr
                               (default: 1000)
     -dest (--dest-solr) URL : Destination Solr url
     -q (--query) VAL        : Import Query (default: *:*)
     -src (--src-solr) URL   : Source Solr url
     -start (--start) N      : Import start (default: 0)
    ```

    Example :
    ```
      java -jar target/nutch-tika-solr-1.0-SNAPSHOT.jar phase2parse \
        -src http://localhost:8983/solr/weapons1 \
        -dest http://localhost:8983/solr/weapons3 \
        -batch 100 -q '*:*' -start 0
    ```
  + **updaterank** command
    This command takes pageranks file from the output of 'pageranks' command and posts it to solr.

    Usage :
      ```
      $ java -jar target/nutch-tika-solr-1.0-SNAPSHOT.jar updateranks
        -batch (--batch-size) N    : Batch or buffer size (default: 1000)
        -field (--rank-field) VAL  : Solr schema field for storing the page rank
        -ranks (--ranks-file) FILE : File containing Page ranks. Each line should have
                                     'URL	(double)SCORE'
        -solr (--solr-url) URL     : Solr server URL
      ```
    Example :
    ```
    $ java -jar target/nutch-tika-solr-1.0-SNAPSHOT.jar updaterank \
      -field location_pr -ranks pageranks-locations.txt \
      -solr http://localhost:8983/solr/collection2
    ```


# Developers / Team
+ Thamme Gowda N.
+ Rakshith
+ Rahul
+ Nii Mante
