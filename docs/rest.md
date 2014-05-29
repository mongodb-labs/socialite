## REST API

    GET     /users/{user_id}                     Get a User by their ID
    DELETE  /users/{user_id}                     Remove a user by their ID
    POST    /users/{user_id}/posts               Send a message from this user
    GET     /users/{user_id}/followers           Get a list of followers of a user
    GET     /users/{user_id}/followers_count     Get the number of followers of a user
    GET     /users/{user_id}/following           Get the list of users this user is following
    GET     /users/{user_id}/following_count     Get the number of users this user follows
    GET     /users/{user_id}/posts               Get the messages sent by a user
    GET     /users/{user_id}/timeline            Get the timeline for this user
    PUT     /users/{user_id}                     Create a new user
    PUT     /users/{user_id}/following/{target}  Follow a user
    DELETE  /users/{user_id}/following/{target}  Unfollow a user

## Using the API

### Create some users

    $ curl -X PUT localhost:8080/users/jsr
    {"_id":"jsr"}

    $ curl -X PUT localhost:8080/users/darren
    {"_id":"darren"}

    $ curl -X PUT localhost:8080/users/ian
    {"_id":"ian"}

### Add some following relationships

    Ian and Darren follow jared
    $ curl -X PUT localhost:8080/users/ian/following/jsr
    $ curl -X PUT localhost:8080/users/darren/following/jsr

    Get jsr's followers
    $ curl localhost:8080/users/jsr/followers
    [{"_id":"darren"},{"_id":"ian"}]

    Find who darren is following
    $ curl localhost:8080/users/darren/following
    [{"_id":"jsr"}]

### Send a message

    Jared sends a message
    $ curl -X POST localhost:8080/users/jsr/posts?message=hello
    {"author":"jsr","_id":"525e2b01a0eeecc56300019d"}

### Get a user's timeline

    Get darren's timeline
    $ curl localhost:8080/users/darren/timeline
    [{"message":"still here","date":1381903352000,"author":"jsr","_id":"525e2bf8a0eeecc56300019f"},
     {"message":"hello again","date":1381903320000,"author":"jsr","_id":"525e2bd8a0eeecc56300019e"},
     {"message":"hello","date":1381903105000,"author":"jsr","_id":"525e2b01a0eeecc56300019d"}]

    Get jared's posts
    $ curl localhost:8080/users/jsr/posts
    [{"message":"still here","date":1381903352000,"author":"jsr","_id":"525e2bf8a0eeecc56300019f"},
     {"message":"hello again","date":1381903320000,"author":"jsr","_id":"525e2bd8a0eeecc56300019e"},
     {"message":"hello","date":1381903105000,"author":"jsr","_id":"525e2b01a0eeecc56300019d"}]


