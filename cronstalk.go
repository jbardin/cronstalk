package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/kr/beanstalk"
)

var (
	debug           = flag.Bool("debug", false, "debug logging")
	redisAddrs      = flag.String("redis", "127.0.0.1:6379", "redis server addresses")
	beanstalkdAddrs = flag.String("beanstalkd", "127.0.0.1:11300", "beanstalkd addresses")

	JobRegistry       = make(map[string]*CronJob)
	RedisServers      []string
	BeanstalkdServers []string
	RedisConn         redis.Conn
	BeanstalkdConn    *beanstalk.Conn
	MyId              string

	// this is just a flag for logging the transitions
	Master bool

	SubmitLock sync.Mutex
)

// Atomic setting of master, returning True if we got it
// Sets a 15sec TTL on the master key
var (
	atomicCheck = `
local master = redis.call("GET", "cronstalk:master")
if (not master) or (master == ARGV[1])
then
	redis.call("SET", "cronstalk:master", ARGV[1], "EX", 15)
	return 1
else
	return 0
end
`
	CheckMaster = redis.NewScript(0, atomicCheck)
)

// generate a random Id for tracking the master
func randId() string {
	max := big.NewInt(int64(1<<63 - 1))
	bigx, _ := rand.Int(rand.Reader, max)
	return fmt.Sprintf("%x", bigx.Int64())
}

// struct to define the json layout of a scheduled job
type JobSpec struct {
	// time to start the scheduled job, in RFC3339 format.
	StartTime string `json:"start_time"`

	// interval between jobs, in 00h00m00s
	Interval string `json:"interval"`

	// beanstalkd fields
	Tube     string `json:"tube"`
	Priority uint32 `json:"priority"`
	Ttr      int    `json:"ttr"`
	Body     string `json:"body"`
}

type CronJob struct {
	StartTime time.Time
	Interval  time.Duration
	Tube      string
	Priority  uint32
	Ttr       int
	Body      string
	Key       string
	stop      chan bool
}

func (j *CronJob) String() string {
	return j.Key
}

// Stop the shceduler for this job
func (j *CronJob) Stop() {
	close(j.stop)
}

// two jobs are equal if all fields match
func (j *CronJob) Equals(jobB *CronJob) bool {
	return ((j.StartTime == jobB.StartTime) &&
		(j.Interval == jobB.Interval) &&
		(j.Tube == jobB.Tube) &&
		(j.Priority == jobB.Priority) &&
		(j.Ttr == jobB.Ttr) &&
		(j.Body == jobB.Body))
}

func NewJob(key string, jobData []byte) (*CronJob, error) {
	jobSpec := new(JobSpec)
	if err := json.Unmarshal(jobData, jobSpec); err != nil {
		return nil, err
	}

	start, err := time.Parse(time.RFC3339, jobSpec.StartTime)
	if err != nil {
		return nil, err
	}

	interval, err := time.ParseDuration(jobSpec.Interval)
	if err != nil {
		return nil, err
	}

	job := &CronJob{
		StartTime: start,
		Interval:  interval,
		Tube:      jobSpec.Tube,
		Priority:  jobSpec.Priority,
		Ttr:       jobSpec.Ttr,
		Body:      jobSpec.Body,
		Key:       key,
		stop:      make(chan bool),
	}
	return job, nil
}

// get the next time this job should run, based on the original start time
// and the interval.
func (j *CronJob) NextStart() time.Time {
	now := time.Now()
	start := j.StartTime
	for start.Before(now) {
		start = start.Add(j.Interval)
	}
	return start
}

// Start the scheduler for a job
func Schedule(job *CronJob) {
	logDebug("scheduling job", job)
	go func() {
		start := job.NextStart()
		logDebug(job.Key, "sleeping until", start)
		time.Sleep(start.Sub(time.Now()))

		ticker := time.NewTicker(job.Interval)
		for {
			Submit(job)
			select {
			case <-ticker.C:
			case <-job.stop:
				logDebug("exiting scheduler for", job)
				return
			}
		}
	}()
}

// Send the job off to a beanstalkd
// handle reconnect if needed while we have the SubmitLock
func Submit(job *CronJob) {
	SubmitLock.Lock()
	defer SubmitLock.Unlock()

	// previous reconnect failed, so we're nil here
	if BeanstalkdConn == nil {
		log.Println("not connected to a beanstalkd server")
		if err := connectBeanstalkd(); err != nil {
			return
		}
	}

	// loop if we need to reconnect
	for i := 0; i < 2; i++ {
		tube := beanstalk.Tube{
			BeanstalkdConn,
			job.Tube,
		}

		_, err := tube.Put([]byte(job.Body), job.Priority, 0, time.Duration(job.Ttr)*time.Second)
		if _, ok := err.(beanstalk.ConnError); ok {
			log.Println(err)
			// attempt to reconnect on a Connection Error
			connectBeanstalkd()
			continue
		}

		if err != nil {
			// anything else is fatal
			log.Println("error submitting job", err)
			return
		}

		// we're OK now
		logDebug("SUBMITED:", fmt.Sprintf("%s(%s)", job.Key, string(job.Body)))
		return
	}
}

func getJobs() (jobs map[string]string, err error) {
	jobs = make(map[string]string)
	var keys []string
	var val string

	keys, err = redis.Strings(RedisConn.Do("KEYS", "cronstalk:job:*"))
	if err != nil {
		log.Println("error updating jobs from redis")
		return
	}

	for _, key := range keys {
		val, err = redis.String(RedisConn.Do("GET", key))
		if err != nil {
			log.Printf("error getting job \"%s\": %s\n", key, err)
			return
		}
		jobs[key] = val
	}
	return
}

// Retrieve all jobs from redis, and schedule them accordingly.
// Update any jobs that have changed, and remove jobs no longer in the database.
// Return error on any connection problems.
func Update() error {
	logDebug("Update")
	if RedisConn == nil {
		return errors.New("not connected to a redis server")
	}

	// Check if we're master
	if ok, err := redis.Bool(CheckMaster.Do(RedisConn, MyId)); err != nil {
		log.Println("error checking for master in redis")
		return err
	} else if !ok {
		if Master {
			Master = false
			log.Printf("%s no longer master\n", MyId)
			AllStop()
		}
		logDebug(MyId + " not master")
		return nil
	}

	if !Master {
		Master = true
		log.Printf("%s taking over as master\n", MyId)
	}

	jobs, err := getJobs()
	if err != nil {
		return err
	}

	for key, val := range jobs {
		job, err := NewJob(key, []byte(val))
		if err != nil {
			log.Printf("error creating job from %s(%s)\n", key, val)
			continue
		}

		oldJob, ok := JobRegistry[key]
		if ok {
			if oldJob.Equals(job) {
				logDebug("job already scheduled:", job)
				continue
			} else {
				// delete the job, we'll create a new one further down
				oldJob.Stop()
				delete(JobRegistry, key)
			}
		}

		// create the new job
		JobRegistry[key] = job
		Schedule(job)
	}

	// now cancel any old jobs we didn't find
	for key, job := range JobRegistry {
		if _, ok := jobs[key]; !ok {
			logDebug("removing job", key, "from schedule")
			job.Stop()
			delete(JobRegistry, key)
		}
	}

	return nil
}

// Stop all jobs, and remove them from the registry
func AllStop() {
	if len(JobRegistry) > 0 {
		log.Println("stopping all scheduled jobs")
	}
	for k, j := range JobRegistry {
		j.Stop()
		delete(JobRegistry, k)
	}
}

// verify is the the connected redis is a Master
func redisMaster(r redis.Conn) bool {
	info, err := redis.Bytes(r.Do("INFO"))
	if err != nil {
		return false
	}

	for _, field := range bytes.Fields(info) {
		kv := bytes.Split(field, []byte(":"))
		if len(kv) == 2 && bytes.Equal(kv[0], []byte("role")) && bytes.Equal(kv[1], []byte("master")) {
			return true
		}
	}
	return false

}

// Connect, or re-connect to a redis server
// Take the first server in our list that is a master
func connectRedis() (err error) {
	if RedisConn != nil {
		RedisConn.Close()
	}
	for _, addr := range RedisServers {
		RedisConn, err = redis.Dial("tcp", addr)
		if err == nil && redisMaster(RedisConn) {
			logDebug("connected to redis", addr)
			return
		} else if err == nil {
			log.Printf("redis server %s is not master\n", addr)
		} else {
			log.Println("cannot connect to a redis server")
		}
	}

	log.Println("error: no redis server available")
	RedisConn = nil
	return errors.New("no redis master")
}

// Connect to a beanstalkd server
// Take the first server in our list with which we can get a connection
func connectBeanstalkd() (err error) {
	if BeanstalkdConn != nil {
		BeanstalkdConn.Close()
	}
	for _, addr := range BeanstalkdServers {
		BeanstalkdConn, err = beanstalk.Dial("tcp", addr)
		if err == nil {
			logDebug("connected to beanstalkd", addr)
			return
		} else {
			log.Println("cannot connect to beanstalkd server", err)
		}
	}

	log.Println("error: no beanstalkd server available")
	return
}

func logDebug(args ...interface{}) {
	if !*debug {
		return
	}
	log.Println(args...)
}

func Run() {
	log.Println("cronstalk started")

	connectRedis()
	connectBeanstalkd()

	for {
		// Update, or reconnect and Update
		if err := Update(); err != nil {
			log.Println(err)
			if err = connectRedis(); err != nil {
				// stop all jobs on update error, and try again later
				AllStop()
			} else {
				if err := Update(); err != nil {
					log.Println(err)
				}

			}
		}

		time.Sleep(10 * time.Second)
	}
}

func main() {
	flag.Parse()
	MyId = randId()
	RedisServers = strings.Split(*redisAddrs, ",")
	BeanstalkdServers = strings.Split(*beanstalkdAddrs, ",")

	Run()
}
