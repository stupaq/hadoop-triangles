Triangle finding algorithms in Hadoop
=====================================

Graph format
------------
Graph is stored in flat, textual form as a list of pairs of vertices separated with comma.
Graph is assumed to be symmetric, the first vertex in a pair must be identified by a number lesser than the second
vertex.

Building and testing
--------------------
Build using `mvn package` (which will also run integration tests build on top
of JUnit) run `mvn package -DskipTests` if you wish to skip unit tests.

Scripts under `bin/` might be useful for running on a real cluster.

Each subdirectory of `inputs/` contains example inputs and expected outputs
together with nullary script `test.sh` which runs tests on default Hadoop
installation available to the user.

Tested on Arch Linux installation with AUR package `hadoop`.

Usage and side notes
--------------------
Scripts under `bin/` are meant to serve as documentation.
This is the right place to look for arguments explanation and other instructions.

Copyright (c) 2014 Mateusz Machalica
