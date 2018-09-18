Log Cache
=========
[![GoDoc][go-doc-badge]][go-doc] [![travis][travis-badge]][travis] [![slack.cloudfoundry.org][slack-badge]][log-cache-slack]


Log Cache persists data in memory from the [Loggregator System][loggregator].

## Usage

This repository should be imported as:

`import logcache "code.cloudfoundry.org/log-cache"`

## Source IDs

Log Cache indexes everything by the `source_id` field on the [Loggregator Envelope][loggregator_v2].
The source ID should distinguish the cluster from other clusters. It should not distinguish a specific instance.

### Cloud Foundry

In Cloud Foundry terms, the source ID can either represent an application
guid (e.g. `cf app <app-name> --guid`), or a component name (e.g. `doppler`).

Each request must have the `Authorization` header set with a UAA provided token.
If the token contains the `doppler.firehose` scope, the request will be able
to read data from any source ID.
If the source ID is an app guid, the Cloud Controller is consulted to verify
if the provided token has the appropriate app access.

## Restful API via Gateway

Log Cache implements a restful interface for getting data.

### **GET** `/v1/read/<source-id>`

Get data from Log Cache for the given `source-id`.

##### Request

Query Parameters:

- **start_time** is a UNIX timestamp in nanoseconds. It defaults to the start of the
  cache (e.g. `date +%s`). Start time is inclusive. `[starttime..endtime)`
- **end_time** is a UNIX timestamp in nanoseconds. It defaults to current time of the
  cache (e.g. `date +%s`). End time is exclusive. `[starttime..endtime)`
- **envelope_types** is a filter for Envelope Type. The available filters are:
  `LOG`, `COUNTER`, `GAUGE`, `TIMER`, and `EVENT`. If set, then only those
  types of envelopes will be emitted. This parameter may be specified multiple times
  to include more types.
- **limit** is the maximum number of envelopes to request. The max limit size
  is 1000 and defaults to 100.

```shell
$ curl "http://<log-cache-addr>:8081/v1/read/<source-id>?start_time=<start-time>&end_time=<end-time>"
```

##### Response Body
```javascript
{
  "envelopes": {"batch": [...] }
}
```

### **GET** `/v1/meta`

Lists the available source IDs that Log Cache has persisted.

##### Response Body
```javascript
{
  "meta":{
    "source-id-0":{"count":"100000","expired":"129452","oldestTimestamp":"1524071322998223702","newestTimestamp":"1524081739994226961"},
    "source-id-1":{"count":"2114","oldestTimestamp":"1524057384976840476","newestTimestamp":"1524081729980342902"},
    ...
  }
}
```
##### Response fields
 - **count** contains the number of envelopes held in Log Cache
 - **expired**, if present, is a count of envelopes that have been pruned
 - **oldestTimestamp** and **newestTimestamp** are the oldest and newest
   entries for the source, in nanoseconds since the Unix epoch.


## Prometheus-Compatible API

### Notes on PromQL
The ultimate goal of these endpoints is to create a fully-compliant,
Prometheus-compatible interface. This should allow tools such as Grafana to
work directly with Log Cache without any additional translation.

_There are still a few metadata endpoints that are unsupported. These should
be coming to Log Cache in a future release._

A valid PromQL metric name consists of the character [a-Z][0-9] and underscore. Names can begin with [a-Z] or underscore. Names cannot begin with [0-9].
As a measure to work with existing metrics that do not comply with the above format a conversion process takes place when matching on metric names.
Any character that is not in the set of valid characters is converted to an underscore.
The metric is not changed in the cache.

e.g., to match on a metric name ``http.latency`` use the name ``http_latency`` as a search term.

### **GET** `/api/v1/query`

Issues a PromQL instant query against Log Cache data. You can read more
detail in the Prometheus documentation [here](https://prometheus.io/docs/prometheus/latest/querying/api/#instant-queries).

```shell
$ curl -G "http://<log-cache-addr>:8081/api/v1/query" --data-urlencode 'query=metrics{source_id="source-id-1"}'
```

##### Response Body
```javascript
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      { "metric": {...}, "value": [ <timestamp>, "<value>" ] },
      ...
    ]
  }
}
```

### **GET** `/api/v1/query_range`

Issues a PromQL range query against Log Cache data. You can read more detail
in the Prometheus documentation [here](https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries).

```shell
$ curl -G "http://<log-cache-addr>:8081/api/v1/query_range" \
    --data-urlencode 'query=metrics{source_id="source-id-1"}' \
    --data-urlencode 'start=1537290750' \
    --data-urlencode 'end=1537290760' \
    --data-urlencode 'step=1'
```

##### Response Body
```javascript
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {...},
        "values": [
          [ <timestamp>, "<value>" ],
          ...
        ]
      },
      ...
    ]
  }
}
```

## Cloud Foundry CLI Plugin

Log Cache provides a [plugin][log-cache-cli] for the Cloud Foundry command
line tool, which makes interacting with the API simpler.

[slack-badge]:              https://slack.cloudfoundry.org/badge.svg
[log-cache-slack]:          https://cloudfoundry.slack.com/archives/log-cache
[log-cache]:                https://code.cloudfoundry.org/log-cache
[go-doc-badge]:             https://godoc.org/code.cloudfoundry.org/log-cache?status.svg
[go-doc]:                   https://godoc.org/code.cloudfoundry.org/log-cache
[travis-badge]:             https://travis-ci.org/cloudfoundry/log-cache.svg?branch=master
[travis]:                   https://travis-ci.org/cloudfoundry/log-cache?branch=master
[loggregator]:              https://github.com/cloudfoundry/loggregator
[loggregator_v2]:           https://github.com/cloudfoundry/loggregator-api/blob/master/v2/envelope.proto
[log-cache-cli]:            https://code.cloudfoundry.org/log-cache-cli
