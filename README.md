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
