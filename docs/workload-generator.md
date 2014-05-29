Overview
========

Socialite includes a workload generator that can simulate user traffic over your socialite configuration. This
workload generator will help you size deployments by allowing you to simulate different load levels and measure the
observed performance of your configuration.

Traffic Model
=============

The workload generator provides a traffic model that you can configure based on your expected load. Here we describe
the model and its tunable attributes.

Total user count
----------------

    --total_users INTEGER

The total number of users in the user population. The workload generator assumes that you have initialized the system
 using the load command with associated graph configuration. Since the load command generates user id's in the range
 [0..USER_COUNT], the workload generator can easily and predictably pick random user's from the population. This
 attribute is a sanity check to tell the generator which range of user id's to choose from.

Concurrent active users
-----------------------

    --active_users INTEGER

The traffic generator simulates user sessions which perform a number of operations before completing. Concurrent
active users controls how many different user ID's will be issuing commands the system over any short period of time.
 Since much of the data in socialite is stored in caches, the set of concurrent active users is going to dictate the
 required cache size to maximize cache efficiency. If you more users than you can store in cache,
 then your system is going to do a lot of disk IO. If you have far fewer users than room in the cache,
 then you are probably wasting money on expensive server hardware.

Connection concurrency
-----------------------

    --concurrency INTEGER

This is the number of simultaneous queries that can be active at a time. Note that this is typically less than
concurrent active users. Fore example, you may have 1M total users, 100k of which are active at a time,
and only 1000 of which may be concurrently processing a transaction.

Target transaction rate
-----------------------

    --target_rate INTEGER

The traffic generator allows you to control the total rate at which requests are sent to the system. This is
inclusive of all types of transactions (follow, unfollow, read own timeline, read timeline,
scroll timeline, send content). Omitting this value, or setting it to 0 will cause the driver to run the load as
quickly as it can.

Transaction mix
---------------

    --follow_pct [0..1]
    --unfollow_pct [0..1]
    --read_own_timeline_pct [0..1]
    --read_timeline_pct [0..1]
    --scroll_timeline_pct [0..1]
    --send_content_pct [0..1]

You can control the proportion of operations that are sent to the system. When the simulator is executing a user
session, it performs a sequence of transactions on behalf of that user. For each transaction,
it chooses which operation to perform with some probability. By controlling the probability values,
you control how frequently each type of request occurs.

You can specify values for zero or more transaction types.

If a subset of percentages are specified, then the specified transactions will run at the specified probability,
and the remaining set of transactions will be evenly distributed amongst the remaining transactions. Leaving all
attributes empty will result in each operation being chosen with equal probability.

Session length
--------------

    --session_length INTEGER

Session length is expressed in terms of the average number of transactions that are completed by a session before
ending. The workload generator will re-use each user-id for a random number of transactions whose average value
equals the session duration attribute.


