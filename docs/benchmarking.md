Benchmarking Tools
==================
This project includes utilities to help benchmark and test. It's packaged in the form
of commands that are built into the Socialite service. The following commands are supported

Bulk Loading Data
------------------
the "load" command can be used to bulk load users, followers, and messages into the system.

    $ java -jar ./target/socialite-0.0.1-SNAPSHOT.jar load
    usage: java -jar socialite-0.0.1-SNAPSHOT.jar
           load [-h] --users USERS --maxfollows MAXFOLLOWERS --messages MESSAGES --threads THREADS [file]

The options are:

    --users (integer)
specifies the number of users to create. they'll be created with names as stringified integers in the range "[0..USERS]"

    --maxfollows (integer)
specifies the maximum number of users each user can follow. the tool picks a zipfian distributed random number
between [0..maxfollows] of users to follow. it will then choose a zipfian distributed random user to follow in the
range [0..current graph size].

    --messages (integer)
specifies how many messages should be sent from each user. after the users have been created,
and the followers added, the command will send this many messages from each user in the system with random content.

    --threads (integer) 
how many threads to use. the loader creates a FIFO queue of tasks and a threadpool of this size will be used to actually 
load the data in.

    [file] optional path to configuration file
The load command uses the same configuration file as the socialite service itself. this allows you to configure
which implementation of the graph, user, and content service to use for the test. if you don't specify a file,
the default options will be used.

a sample invocation of the command

    $ java -jar ./target/socialite-0.0.1-SNAPSHOT.jar load --users 1000 --maxfollows 100 --messages 100

This will create 1000 users. Each user will have around 100 followers. Each user will have sent 100 messages.

Benchmark Runner
----------------
Please see docs [here](workload-generator.md).

Benchmark Timeline Read Latency
-------------------------------
The "bench-timeline" command can be used to measure the latency of timeline read operations. The tool assumes that
you used the "Bulk Load" command to load up the database. When it starts up, it will read the max_user_id from the
user service and use that as the upper bound of the user ID's that it randomly selects. It will select user ID's in
the range 0..MAX;

    $ java -jar ./target/socialite-0.0.1-SNAPSHOT.jar bench-timeline
    usage: java -jar socialite-0.0.1-SNAPSHOT.jar bench-timeline [-h] --iterations ITERATIONS [--csv CSV] [file]

it supports the following parameters

    --iterations (integer)
This is how many timeline reads will be performed over the run of the test. The tool will pick random user_id's and
read their timeline until it's reached this limit.

    --csv (directory-name) (optional)
If you omit this value, the tool will periodically print performance stats to STDOUT while it's running. Each outline
 will look like this:

    10/17/13 1:35:26 PM ============================================================

    -- Timers ----------------------------------------------------------------------
    timeline-latencies
                 count = 8885
             mean rate = 145.65 calls/second
         1-minute rate = 141.94 calls/second
         5-minute rate = 137.08 calls/second
        15-minute rate = 135.86 calls/second
                   min = 1.43 milliseconds
                   max = 36.10 milliseconds
                  mean = 6.62 milliseconds
                stddev = 4.89 milliseconds
                median = 5.12 milliseconds
                  75% <= 8.84 milliseconds
                  95% <= 16.02 milliseconds
                  98% <= 18.98 milliseconds
                  99% <= 22.50 milliseconds
            99.9% <= 36.09 milliseconds

If a value of csv is specified, the tool will append lines to a CSV file in that directory. The directory must exist
before the tool starts.

