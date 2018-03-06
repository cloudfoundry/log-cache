Log Cache
=========
[![GoDoc][go-doc-badge]][go-doc] [![travis][travis-badge]][travis] [![slack.cloudfoundry.org][slack-badge]][loggregator-slack]


Log Cache persists data in memory from the [Loggregator System][loggregator].

## Usage

This repository should be imported as:

`import logcache "code.cloudfoundry.org/log-cache"`

## Source IDs

Log Cache indexes everything by the `source_id` field on the [Loggregator Envelope][loggregator_v2]. The source ID should distinguish the cluster from other clusters. It should not distinguish a specific instance. 

### Cloud Foundry 

In terms of Cloud Foundry, the source ID can either represent an application guid (e.g. `cf app <app-name> --guid`), or a component name (e.g. `doppler`).

Source IDs are used to authorize any attempt to read from Log Cache. Each request must have the `Authorization` header set with a UAA provided token. If the given scope has `doppler.firehose`, then it may see any Source ID. Otherwise, the CloudController is consuled to see if the token can read logs for the given Source ID. In the latter case, the Source ID must be an app guid.

## Restful API via Gateway

Log Cache implements a restful interface for getting data.

### **GET** `/v1/read/<source-id>`

Get data from Log Cache for the given `source-id`.

##### Request

Query Parameters:

- **start_time** is UNIX timestamp in nanoseconds. It defaults to the start of the
  cache (e.g. `date +%s`). Start time is inclusive. `[starttime..endtime)`
- **end_time** is UNIX timestamp in nanoseconds. It defaults to current time of the
  cache (e.g. `date +%s`). End time is exclusive. `[starttime..endtime)`
- **envelope_type** is filter for Envelope Type. The available filters are:
  `LOG`, `COUNTER`, `GAUGE`, `TIMER`, and `EVENT`. If set, then only those
  types of envelopes will be emitted.
- **limit** is the maximum number of envelopes to request. The max limit size
  is 1000 and defaults to 100.

```
curl "http://<log-cache-addr>:8080/v1/read/<source-id>/?start_time=<start time>&end_time=<end time>"
```

##### Response Body
```
{
  "envelopes": {"batch": [...] }
}
```

### **GET** `/v1/meta`

Lists the available `SourceIDs` that Log Cache has persisted.

##### Response Body
```
{
  "meta":{
    "source-id-0":{},
    "source-id-1":{},
    ...
  }
}
```

### **GET** `/v1/group/<group-name>`

Reads from the given group. The group's source-ids are merged and sorted.

Query Parameters:

- **requester_id** is a string used to shard data across multiple clients. This
  string should be unique for each client reading from the group.
- **start_time** is UNIX timestamp in nanoseconds. It defaults to the start of the
  cache (e.g. `date +%s`). Start time is inclusive. `[starttime..endtime)`
- **end_time** is UNIX timestamp in nanoseconds. It defaults to current time of the
  cache (e.g. `date +%s`). End time is exclusive. `[starttime..endtime)`
- **envelope_type** is filter for Envelope Type. The available filters are:
  `LOG`, `COUNTER`, `GAUGE`, `TIMER`, and `EVENT`. If set, then only those
  types of envelopes will be emitted.
- **limit** is the maximum number of envelopes to request. The max limit size
  is 1000 and defaults to 100.

```
curl "http://<log-cache-addr>:8080/v1/group/<group-name>/?start_time=<start time>&end_time=<end time>&requester_id=<requester_id>"
```

##### Response Body
```
{
  "envelopes": {"batch": [...] }
}
```

### **PUT** `/v1/group/<group-name>/<source-id>`

Adds the given source-id to the given group. If the group does not exist, then it creates it.

```
curl "http://<log-cache-addr>:8080/v1/group/<group-name>/<source-id>" -XPUT
```

##### Response Body
```
{}
```

### **DELETE** `/v1/group/<group-name>/<source-id>`

Removes the given source-id from the given group.

```
curl "http://<log-cache-addr>:8080/v1/group/<group-name>/<source-id>" -XDELETE
```

##### Response Body
```
{}
```

### **GET** `/v1/group/<group-name>/meta`

Gets meta information about the group.

```
curl "http://<log-cache-addr>:8080/v1/group/<group-name>/meta"
```

##### Response Body
```
{
  "source_ids": [...]
}
```

##### Normal Mode

LogCache will be queried periodically and pass a batch of envelopes to the
template.

##### Follow Mode

LogCache will be queried rapidly and pass a single envelope to the
template.

[slack-badge]:              https://slack.cloudfoundry.org/badge.svg
[loggregator-slack]:        https://cloudfoundry.slack.com/archives/loggregator
[log-cache]:                https://code.cloudfoundry.org/log-cache
[go-doc-badge]:             https://godoc.org/code.cloudfoundry.org/log-cache?status.svg
[go-doc]:                   https://godoc.org/code.cloudfoundry.org/log-cache
[travis-badge]:             https://travis-ci.org/cloudfoundry-incubator/log-cache.svg?branch=master
[travis]:                   https://travis-ci.org/cloudfoundry-incubator/log-cache?branch=master
[loggregator]:              https://github.com/cloudfoundry/loggregator
[loggregator_v2]:           https://github.com/cloudfoundry/loggregator-api/blob/master/v2/envelope.proto
