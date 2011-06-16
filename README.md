As400 to CouchDB
================

Tiny Java utility for reading data from AS/400 database, convert it to JSON, and import it to CouchDB database.

It is NOT a generic tool. You will need to edit the code to use it.

I am sharing it because it was very hard for me to find working code and good documentation for this task.

With time I will improve it and add documentation for others to use.

Compiling
--------------
It is an [Eclipse](http://www.eclipse.org/) Java project, so the easiest way is to import it in Eclipse. Since it is also a [Maven](http://maven.apache.org/) project, I also recommend installing [m2e](http://www.eclipse.org/m2e/) plugin.

You can build an executable jar package with Maven:

    mvn package

But first you should manually install [jt400](http://jt400.sourceforge.net/) with something like:

    mvn install:install-file \
         -Dfile=lib/jt400.jar \
         -DgroupId=it.nuccioservizi.jars \
         -DartifactId=jt400 \
         -Dversion=1 -Dpackaging=jar

I put a script in `bin/install-jt400_jar' for convenience. This is because public maven packages are quite old.

Running
-------
You must create a `local.properties` file with your local settings (usernames, passwords, ...). Or pass them via command line.

Run it as a console Java application.

History
----------
It all started by the need to build a [CouchApp](http://couchapp.org/) backed by some data available on a legacy as400 system.

Java was chosen for driver support. The open-source jt400 drivers are the best I have found to query an as400 DB. They deal perfectly with character encodings (as400 does not use UTF-8) and they work seamlessly on any platform.

Documentation was the real problem (how to make it work through an SSH tunnel?). So I have decided to share this code hoping it will be useful to others.
