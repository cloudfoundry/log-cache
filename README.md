Log Cache
=========

Log Cache persists data in memory from the [Loggregator
System](https://github.com/cloudfoundry/loggregator).

## Restful API

Log Cache implements a restful interface for getting data.

### **GET** /<source-id>

Request:
```
curl http://10.0.0.5:8080/5711bbdd-ef74-4626-ab17-ed29d72c3f7c
```

Response Body:
```
{
  "envelopes": [...]
}
```
