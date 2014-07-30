CouchbaseLoad-From-CSV
======================

Load doc data to couch base using NodeJs.  

Data is streamed so presumably copes with large files. Tested against a few hundred k lines.

Unlike cbdocloader, this does not need to (write & then) read from json files. 


CSVs are parsed using Lazy.js and inserted directly into couchbase. 

There's a simple way to append data before the insert. 

Also easy to parse/modify each row item on the fly.

It does not seem to work on more recent versions of node 0.11.x because of some sort of "binding problem" with couchnode

Node 0.10.28 works fine.



Hope this is of use to others .... !  Improvements are welcomed of course.  Many thanks to all of the module contributors.

BTW ... I found lazy.js   very useful(!).
