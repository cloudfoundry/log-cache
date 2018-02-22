Log Cache 
=========
[![GoDoc][go-doc-badge]][go-doc] [![travis][travis-badge]][travis] [![slack.cloudfoundry.org][slack-badge]][loggregator-slack]


Log Cache persists data in memory from the [Loggregator
System](https://github.com/cloudfoundry/loggregator).

## Usage

This repository should be imported as:

`import logcache "code.cloudfoundry.org/log-cache"`

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
curl "http://<log-cache-addr>:8080/v1/group/<group-name>/?start_time=<start time>&end_time=<end time>" -XDELETE
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

## WebHook

WebHook reads from LogCache and uses user provided golang text templates to
post data to remote endpoints. It has two modes, normal and follow mode.

##### Normal Mode

LogCache will be queried periodically and pass a batch of envelopes to the
template.

##### Follow Mode

LogCache will be queried rapidly and pass a single envelope to the
template.

### Example Templates

##### DataDog

The following template reads Counter and Gauge values from LogCache and posts
it to DataDog.

```
{{ if .GetCounter }}
    {{$m:=mapInit "metric" (printf "log-cache.%s" .GetCounter.GetName)}}
    {{$m.Add "host" "some-domain.com"}}
    {{$m.Add "type" "gauge"}}
    {{$m.Add "points" (sliceInit (sliceInit (nsToTime .Timestamp).Unix .GetCounter.GetTotal)) }}
    {{$s:=mapInit "series" (sliceInit $m)}}
    {{post "https://app.datadoghq.com/api/v1/series?api_key=99999999999999999999999999999999" nil $s}}
{{ else if .GetGauge }}
    {{$timestamp:=.Timestamp}}
    {{ range $name, $value := .GetGauge.GetMetrics }}
        {{$m:=mapInit "metric" (printf "log-cache.%s" $name)}}
        {{$m.Add "host" "some-domain.com"}}
        {{$m.Add "type" "gauge"}}
        {{$m.Add "points" (sliceInit (sliceInit (nsToTime $timestamp).Unix $value.GetValue)) }}
        {{$s:=mapInit "series" (sliceInit $m)}}
        {{post "https://app.datadoghq.com/api/v1/series?api_key=99999999999999999999999999999999" nil $s}}
    {{end}}
{{end}}
```

##### Post Upon Condition

The following template reads from batches of envelopes and makes a decision
and posts based on if the count is 1 or if the average comes to 100.

```
{{ if (eq (countEnvelopes .) 1) }}
    {{post "http://some.url" nil "Page Me 1"}}
{{ else if eq (averageEnvelopes .) 100.0 }}
    {{post "http://some.url" nil "Page Me 2"}}
{{end}}
```

##### Available Template Functions

WebHooks provide a set of built-in functions that can be used in templates.
- `.GetCounter` - Gets the counter from an envelope
- `.GetCounter.GetName` - Gets the counter name from an envelope
- `.GetGauge` - Gets the gauge from an envelope
- `.GetGauge.GetMetrics` - Gets the gauge metrics from an envelope
- `sliceInit` - converts arguments to a slice
- `sliceAppend` - adds arguments to a slice
- `mapInit` - creates a map
- `mapAdd` - adds key and value to a map
- `countEnvelopes` - returns the length of the passed envelope slice
- `averageEnvelopes` - return the average of metric values in the passed envelopes
- `nsToTime` - converts nanoseconds to `time.Time`

[slack-badge]:              https://slack.cloudfoundry.org/badge.svg
[loggregator-slack]:        https://cloudfoundry.slack.com/archives/loggregator
[log-cache]:                https://code.cloudfoundry.org/log-cache
[go-doc-badge]:             https://godoc.org/code.cloudfoundry.org/log-cache?status.svg
[go-doc]:                   https://godoc.org/code.cloudfoundry.org/log-cache
[travis-badge]:             https://travis-ci.org/cloudfoundry-incubator/log-cache.svg?branch=master
[travis]:                   https://travis-ci.org/cloudfoundry-incubator/log-cache?branch=master
