# Background

You are part of a team developing a web service that uses a managed MySQL DB from a public cloud provider with 1 master DB and multiple read replicas to spread the load.

The team is working on a library that can be used in the same way as the `DB` type from the `database/sql` package.

The purpose of this library is to automatically route read-only queries to read replicas, and all other queries to the master DB (see diagram), without the user having to be aware of it.

![image.png](./image.png)

The public cloud provider will perform automatic maintenance on the DBs with the following conditions: 

* During maintenance, the affected DB will not be available and will get restarted
* For the master DB, the team can set a maintenance window during which the public cloud provider will perform maintenance
* For the read replicas, maintenance can happen at any time (no maintenance window setting possible) 
* Maintenance of read replicas is always performed on one read replica at a time without overlap

Proper handling of read replicas under maintenance is required so that the service is available at all times. 

The maintenance of the master DB can be scheduled during service maintenance so additional handling is not required.

It has come to light that the package, `mydb.go`, contains some issues which need urgent attention.

# Your Tasks

## Task 1

Consider the following points for the current state of the library.

* Does the library fulfill the requirements described in the background section?
* Is the library easy to use?
* Is the code quality assured?
* Is the code readable?
* Is the library thread-safe?

Write down any issues you see.

## Task 2

Resolve the issues discovered in the previous task by fixing the code.

You may, within reason:

* Use external libraries
* Add files or directories
* Rename functions, change parameters, and make other destructive changes

Ensure that it works on Go 1.12 and above

## Task 3

Explain what you did in the previous task and your reasoning behind it.

## When Submitting Your Answers

Please commit your code on the `master` branch in the specified GitHub repository.

Additionally, the following two files must be included:

* `answer.md`: your answers for Tasks 1 and 3
* `packages.txt`: the import paths of any packages you created, separated by newlines
  * This file will be processed by a machine
  * Example `packages.txt`:

    ```
    github.com/xxxxx/mydb
    github.com/xxxxx/mylib
    ```

Please note that responses to GitHub issues, Pull Requests, comments etc., as well as branches other than `master`, will not be taken into consideration.
