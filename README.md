## cronstalk
Highly available scheduled jobs for beanstalkd

## Running
- Uses redis for scheduling and coordination
- Run miltuple cronstalk process, connected to multiple redis servers for high availability

### Jobs
Jobs are stored as a redis hash, with the following fields:

```
start_time: 2010-01-01T15:04:05Z  # start time in RFC3339 format
interval:   10m                   # interval between jobs in hours, minutes, and seconds
tube:       test                  # beanstalkd tube name
ttr:        120                   # beanstalkd ttr
priority:   1024                  # beanstalkd priority
body:       "some data here"      # Job body
last_submit: 2010-01-01T15:14:05Z # Last time the job was submitted
```

Cronstalk loads all jobs referenced in the redis set `cronstalk:jobs`. The
actual job key is not important, though it is used as the job Name internally
in cronstalk.

### TODO
- Round robin option for beanstalkd servers


