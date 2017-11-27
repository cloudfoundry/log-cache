Log Cache
=========

Log Cache persists data in memory from the [Loggregator
System](https://github.com/cloudfoundry/loggregator).

## Restful API

Log Cache implements a restful interface for getting data.

### **GET** `/<source-id>`

##### Request

Query Parameters:

- **starttime** is UNIX timestamp in nanoseconds. It defaults to the start of the
  cache (e.g. `date +%s`). Start time is inclusive. `[starttime..endtime)`
- **endtime** is UNIX timestamp in nanoseconds. It defaults to current time of the
  cache (e.g. `date +%s`). End time is exclusive. `[starttime..endtime)`
- **envelopetype** is filter for Envelope Type. The available filters are:
  `log`, `counter`, `gauge`, `timer`, and `event`. If set, then only those
  types of envelopes will be emitted.
- **limit** is the maximum number of envelopes to request. The max limit size
  is 1000 and defaults to 100.

```
curl http://<log-cache-addr>:8080/<source-id>/?starttime=<start time>&endtime=<end time>
```

##### Response Body
```
{
  "envelopes": [...]
}
```
