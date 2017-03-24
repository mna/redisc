# redisc [![GoDoc](https://godoc.org/github.com/mna/redisc?status.png)][godoc] [![Build Status](https://semaphoreci.com/api/v1/mna/redisc/branches/master/badge.svg)](https://semaphoreci.com/mna/redisc)

Package redisc implements a redis cluster client built on top of the [redigo package][redigo]. See the [godoc][] for details.

## Installation

    $ go get [-u] [-t] github.com/mna/redisc

## Documentation

The [godoc][] is the canonical source for documentation.

The design goal of redisc is to be as compatible as possible with the [redigo][] package. As such, the `Cluster` type can be used as a drop-in replacement to a `redis.Pool`, and the connections returned by the cluster implement the `redis.Conn` interface. The package offers additional features specific to dealing with a cluster that may be needed for more advanced scenarios.

The main features are:

* Drop-in replacement for `redis.Pool` (the `Cluster` type implements the same `Get` and `Close` method signatures).
* Connections are `redis.Conn` interfaces and use the `redigo` package to execute commands, `redisc` only handles the cluster part.
* Support for all cluster-supported commands including scripting, transactions and pub-sub.
* Support for READONLY/READWRITE commands to allow reading data from replicas.
* Client-side smart routing, automatically keeps track of which node holds which key slots.
* Automatic retry of MOVED, ASK and TRYAGAIN errors when desired, via `RetryConn`.
* Manual handling of redirections and retries when desired, via `IsTryAgain` and `ParseRedir`.
* Automatic detection of the node to call based on the command's first parameter (assumed to be the key).
* Explicit selection of the node to call via `BindConn` when needed.
* Support for optimal batch calls via `SplitBySlot`.

## Alternatives

* [redis-go-cluster][rgc].
* [radix v1][radix1] provides a cluster package.
* [radix v2][radix2] provides a cluster package.

## License

The [BSD 3-Clause license][bsd].

[bsd]: http://opensource.org/licenses/BSD-3-Clause
[godoc]: http://godoc.org/github.com/mna/redisc
[redigo]: https://github.com/garyburd/redigo
[rgc]: https://github.com/chasex/redis-go-cluster
[radix1]: https://github.com/fzzy/radix
[radix2]: https://github.com/mediocregopher/radix.v2

