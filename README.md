Log Cache
=========
[![GoDoc][go-doc-badge]][go-doc] [![travis][travis-badge]][travis] [![slack.cloudfoundry.org][slack-badge]][loggregator-slack]


Log Cache persists data in memory from the [Loggregator System][loggregator].

## Usage

This repository should be imported as:

`import logcache "code.cloudfoundry.org/log-cache"`

## Source IDs

Log Cache indexes everything by the `source_id` field on the [Loggregator Envelope][loggregator_v2].
The source ID should distinguish the cluster from other clusters. It should not distinguish a specific instance.

### Cloud Foundry

In terms of Cloud Foundry, the source ID can either represent an application
guid (e.g. `cf app <app-name> --guid`), or a component name (e.g. `doppler`).

Each request must have the `Authorization` header set with a UAA provided token.
If the token contains the `doppler.firehose` scope, the request will be able
to read data from any source ID.
If the source ID is an app guid, the Cloud Controller is consulted to verify
if the provided token has the appropriate app access.

## Shard Groups

Shard groups can be used to split up consumption among multiple readers. With
a shard group and set of requester IDs, Log Cache will do its best to evenly
spread results among the requesters.

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

```
curl "http://<log-cache-addr>:8080/v1/read/<source-id>/?start_time=<start-time>&end_time=<end-time>"
```

##### Response Body
```
{
  "envelopes": {"batch": [...] }
}
```

### **GET** `/v1/meta`

Lists the available source IDs that Log Cache has persisted.

##### Response Body
```
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

### **GET** `/v1/shard_group/<group-name>`

Reads from the given shard-group. The shard-group's source-ids are merged and sorted.

Query Parameters:

- **requester_id** is a string used to shard data across multiple clients. This
  string should be unique for each client reading from the group. If multiple
  clients use the same requester ID, sharding may not be reliable.
- **start_time** is UNIX timestamp in nanoseconds. It defaults to the start of the
  cache (e.g. `date +%s`). Start time is inclusive. `[starttime..endtime)`
- **end_time** is UNIX timestamp in nanoseconds. It defaults to current time of the
  cache (e.g. `date +%s`). End time is exclusive. `[starttime..endtime)`
- **envelope_types** is a filter for Envelope Type. The available filters are:
  `LOG`, `COUNTER`, `GAUGE`, `TIMER`, and `EVENT`. If set, then only those
  types of envelopes will be emitted. This parameter may be specified multiple times
  to include more types.
- **limit** is the maximum number of envelopes to request. The max limit size
  is 1000 and defaults to 100.

```
curl "http://<log-cache-addr>:8080/v1/shard_group/<group-name>/?start_time=<start-time>&end_time=<end-time>&requester_id=<requester-id>"
```

##### Response Body
```
{
  "envelopes": {"batch": [...] }
}
```

### **PUT** `/v1/shard_group/<group-name>/`

Adds the given source ids to the given shard-group. If the shard-group does
not exist, then it gets created. Each shard-group may contain many sub-groups.
Each sub-group may contain many source-ids. Each requester (identified by a
`requester_id`) will receive an equal subset of the shard group. A sub-group
ensures that each requester receives envelopes for the given source-ids
grouped together (and not spread across other requesters).

##### Request Body

```
{
  "sourceIds": [
    "source-id-1",
    "source-id-2"
  ]
}
```

```
curl "http://<log-cache-addr>:8080/v1/shard_group/<group-name>" -XPUT -d'{"sourceIds":["source-id-1","source-id-2"]}'
```

##### Response Body
```
{}
```

### **GET** `/v1/shard_group/<group-name>/meta`

Gets meta information about the shard-group.

```
curl "http://<log-cache-addr>:8080/v1/shard_group/<group-name>/meta"
```

##### Response Body
```
{
  "source_ids": [...]
}
```

### **GET** `/v1/promql`

Issues a PromQL query against Log Cache data.

```
curl -G "http://<log-cache-addr>:8080/v1/promql" --data-urlencode 'query=metrics{source_id="source-id-1"}'
```

##### Response Body
```
{
  "vector": {
    "samples": [{ "metric": {...}, "point": {...} }]
  }
}
```

## Cloud Foundry CLI Plugin

Log Cache provides a [plugin][log-cache-cli] for the Cloud Foundry command
line tool, which makes interacting with the API simpler.

[slack-badge]:              https://slack.cloudfoundry.org/badge.svg
[loggregator-slack]:        https://cloudfoundry.slack.com/archives/loggregator
[log-cache]:                https://code.cloudfoundry.org/log-cache
[go-doc-badge]:             https://godoc.org/code.cloudfoundry.org/log-cache?status.svg
[go-doc]:                   https://godoc.org/code.cloudfoundry.org/log-cache
[travis-badge]:             https://travis-ci.org/cloudfoundry/log-cache.svg?branch=master
[travis]:                   https://travis-ci.org/cloudfoundry/log-cache?branch=master
[loggregator]:              https://github.com/cloudfoundry/loggregator
[loggregator_v2]:           https://github.com/cloudfoundry/loggregator-api/blob/master/v2/envelope.proto
[log-cache-cli]:            https://code.cloudfoundry.org/log-cache-cli
