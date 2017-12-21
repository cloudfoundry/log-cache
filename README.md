Log Cache
=========

Log Cache persists data in memory from the [Loggregator
System](https://github.com/cloudfoundry/loggregator).

## Restful API via Gateway

Log Cache implements a restful interface for getting data.

### **GET** `/v1/read/<source-id>`

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
curl http://<log-cache-addr>:8080/v1/read/<source-id>/?start_time=<start time>&end_time=<end time>
```

##### Response Body
```
{
  "envelopes": {"batch": [...] }
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
