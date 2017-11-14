Log Cache
=========

Log Cache persists data in memory from the [Loggregator
System](https://github.com/cloudfoundry/loggregator).

## Restful API

Log Cache implements a restful interface for getting data.

### **GET** `/<source-id>`

##### Request

Query Parameters:

- **starttime** is UNIX timestamp in seconds. It defaults to the start of the
  cache (e.g. `date +%s`). Start time is inclusive. `[starttime..endtime)`
- **endtime** is UNIX timestamp in seconds. It defaults to current time of the
  cache (e.g. `date +%s`). End time is exclusive. `[starttime..endtime)`

```
curl http://<log-cache-addr>:8080/<source-id>/?starttime=<start time>&endtime=<end time>
```

##### Response Body
```
{
  "envelopes": [...]
}
```
