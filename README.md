# cronstalk
## Highly available scheduled jobs for beanstalkd

# Warning: IN PROGRESS

## TODO
- Stay connected to redis master when possible
- Allow multiple cronstalks to run, negotiating for a single master.
- Round robin option for beanstalkd servers

## Running
- Uses redis for scheduling and coordination
- Run miltuple cronstalk process, connected to multiple redis servers for high availability


### Schedule
Jobs are defined as `name:job` in the `cronstalk:schedule` redis hash. I may change this to use
the `cronstalk:schedule:*` keyspace, so that TTL can be applied to indiviual jobs.

### Jobs
Jobs are JSON encoded, with the following required attributes

```python
  {
    "StartTime": "2010-01-01T15:04:05Z",  # start time in RFC3339 format  
    "Interval": "10m",                    # interval between jobs in hours, minutes, and seconds
    "Tube": "test",                       # beanstalkd tube name
    "Ttr":  120,                          # beanstalkd ttr
    "Priority": 12345,                    # beanstalkd priority
    "Body": "some data here",             # Job body
  }

