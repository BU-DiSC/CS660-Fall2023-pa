# Introduction

## Database Architecture and Implementation Guide

Our database consists of:

- Classes that represent fields, tuples, and tuple schemas;

- Classes that apply predicates and conditions to tuples;

- One or more access methods (e.g., heap files) that store relations on disk and provide a way to iterate through tuples
  of those relations;

- A collection of operator classes (e.g., select, join, insert, delete, etc.) that process tuples;

- A buffer pool that caches active tuples and pages in memory and handles concurrency control and transactions (neither
  of which you need to worry about for now); and,

- A catalog that stores information about available tables and their schemas.

Our database does not include many things that you may think of as being a part of a "database". In particular, our
database does not have:

- a SQL front end or parser that allows you to type queries directly. Instead, queries are built up by chaining a set of
  operators together into a hand-built query plan. We will provide a simple parser for use in later labs.

- Views.

- Data types except integers and fixed-length strings.

- Query optimizer.

- Indices.

## Logistics

You must submit your code (see below) as well as a short (1-2 pages) writeup describing your approach. This writeup
should:

Describe any design decisions you made. These may be minimal for pa1.

Discuss and justify any changes you made to the API.

Describe any missing or incomplete elements of your code.

Describe how long you spent on the assignment, and whether there was anything you found particularly difficult or
confusing.

If you collaborate with someone else, you have to describe how you split the workload.

### Collaboration

This assignment should be manageable for a single person, but if you prefer to work with a partner, this is also OK
(which is the default setup in our class). Larger groups than 2 are not allowed. Please indicate clearly who you worked
with, if anyone, on your individual writeup.

### Submitting your assignment

Submit only the files that are provided with this assignment (only the [db](../db) and [include](../include)
directories).

### Submitting a bug

Please submit bug reports to the TF.

### Grading

85% of your grade will be based on whether your code passes the tests when we run it. These tests will be a superset
of the tests we have provided. Before handing in your code, you should make sure your code produces no errors.

For testing, we will use the `CMakeLists.txt` and the entire contents of the test directory with our version of these
files. This means you cannot change the format of .dat files. You should also not change our API. You should test that
your code compiles the unmodified tests. Try to add code only where we have indicated.

An additional 15% of your grade will be based on the quality of your writeup and our subjective evaluation of your code.

We hope you enjoy hacking on this assignment!
