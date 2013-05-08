# cronstalk
## Highly available scheduled jobs for beanstalkd

# Warning: IN PROGRESS

## TODO
- Round robin option for beanstalkd servers

## Running
- Uses redis for scheduling and coordination
- Run miltuple cronstalk process, connected to multiple redis servers for high availability


### Schedule
Jobs are defined in redis as `key:job` with the keys in the `cronstalk:job:*`
namespace. This allows jobs to have a TTL in redis.

### Jobs
Jobs are JSON encoded, with the following required attributes

```python
  {
    "start_time": "2010-01-01T15:04:05Z",  # start time in RFC3339 format
    "interval": "10m",                     # interval between jobs in hours, minutes, and seconds
    "tube": "test",                        # beanstalkd tube name
    "ttr":  120,                           # beanstalkd ttr
    "priority": 12345,                     # beanstalkd priority
    "body": "some data here",              # Job body
  }

