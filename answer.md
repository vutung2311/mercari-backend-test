## Task 1

> Does the library fulfill the requirements described in the background section?
* Library doesn't fulfill the requirement.
> Is the library easy to use?
* The library is easy to use because it implements all methods of sql.DB.
> Is the code quality assured?
* Code quality is not assured because there was no test.
> Is the code readable?
* The code is not really readable in the original `readReplicaRoundRobin` function.
> Is the library thread-safe?
* Library is not thread-safe.

## Task 3
> Does the library fulfill the requirements described in the background section?
* I'm adding a read timeout and use this to mark a read replica as offline. For acknowledging that a read replica is back I use several go routines that `Ping` replicas periodically. I didn't use context.WithDeadline because a underline driver might not respect the deadline of a context.
> Is the library easy to use?
* I kept the same signature as sql.DB to make it easy to use.
> Is the code quality assured?
* I added tests to improve code quality.
> Is the code readable?
* I split the logic into smaller unit and give function name to each unit to improve readability.
> Is the library thread-safe?
* To make the library thread-safe I used function `atomic` package.