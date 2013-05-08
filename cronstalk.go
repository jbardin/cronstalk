package main

import (
	"bytes"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/big"
	"strconv"
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

	JobError = errors.New("error reading")

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

// take the strings returned from HGETALL and return a CronJob
func NewJob(key string, jobData []string) (job *CronJob, err error) {
	job = new(CronJob)
	job.Key = key

	for i := 0; i < len(jobData)-1; i += 2{
		switch jobData[i] {
		case "start_time":
			job.StartTime, err = time.Parse(time.RFC3339, jobData[i+1])
			if err != nil {
				return nil, err
			}
		case "interval":
			job.Interval, err = time.ParseDuration(jobData[i+1])
			if err != nil {
				return nil, err
			}
		case "tube":
			job.Tube = jobData[i+1]
		case "ttr":
			job.Ttr, err = strconv.Atoi(jobData[i+1])
			if err != nil {
				return nil, err
			}
		case "priority":
			p, err := strconv.ParseUint(jobData[i+1], 10, 32)
			if err != nil {
				return nil, err
			}
			job.Priority = uint32(p)
		case "body":
			job.Body = jobData[i+1]
		}


	}

	job.stop = make(chan bool)
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
	log.Printf("SUBMIT: %+v\n", job)
	SubmitLock.Lock()
	log.Println("HOLDING LOCK!")
	defer func() {
		log.Println("RELEASED LOCK")
		SubmitLock.Unlock()
	}()

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
			log.Println("error submitting job:", err)
			return
		}

		// we're OK now
		logDebug("SUBMITED:", fmt.Sprintf("%s(%s)", job.Key, string(job.Body)))
		return
	}
}

func getJobs() (jobs map[string]*CronJob, err error) {
	jobs = make(map[string]*CronJob)
	var jobKeys []string
	var jobData []string

	jobKeys, err = redis.Strings(RedisConn.Do("SMEMBERS", "cronstalk:jobs"))
	if err != nil {
		log.Println("error updating jobs from redis")
		return
	}


	for _, key := range jobKeys {
		jobData, err = redis.Strings(RedisConn.Do("HGETALL", key))
		if err != nil {
			log.Printf("error getting job \"%s\": %s\n", key, err)
			continue
		}

		job, err := NewJob(key, jobData)
		if err != nil {
			log.Printf("error creating job %s\n", key)
			continue
		}

		jobs[key] = job
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

	for key, job := range jobs {
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
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	MyId = randId()
	RedisServers = strings.Split(*redisAddrs, ",")
	BeanstalkdServers = strings.Split(*beanstalkdAddrs, ",")

	Run()
}
