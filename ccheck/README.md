# Consistency Checker

This folder implements a consistency checker as described in the [redis cluster tutorial documentation][tut] and implemented in the [reference redis cluster client ruby repository][rubycluster].

The `redis-trib.rb` and `create-cluster` scripts are from the [redis repository][redis] and copied here only for convenience, with the redis copyright added to the top of the files. The `create-cluster` script has been adjusted to start the $PATH-installed redis-server and redis-cli binaries, and the `redis-trib.rb` script in the current directory.

You must have `ruby` and `gem install redis` for it to work.

All scripts should be executed in this directory.

To run the cluster:

1. create-cluster start
2. create-cluster create (type yes when prompted)
3. (optional) execute the ccheck go client program (`go run ccheck.go --addrs localhost:30001`)
4. (optional) interactively play with the cluster, trigger failovers, etc. using either `redis-cli` or to interact via `redisc`, `go run redisc_cli.go --help`
5. create-cluster stop
6. create-cluster clean

[tut]: http://redis.io/topics/cluster-tutorial
[rubycluster]: https://github.com/antirez/redis-rb-cluster
[redis]: https://github.com/antirez/redis


