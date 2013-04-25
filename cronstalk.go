package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/big"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	// "github.com/kr/beanstalk"
)

var (
	// actuary *beanstalk.Conn
	debug      = flag.Bool("debug", false, "debug logging")
	redisAddrs = flag.String("redis", "127.0.0.1:6379", "redis server addresses")

	JobRegistry  = make(map[string]*CronJob)
	RedisServers []string
	RedisConn    redis.Conn
	MyId         string
)

func randId() string {
	max := big.NewInt(int64(1<<63 - 1))
	bigx, _ := rand.Int(rand.Reader, max)
	return fmt.Sprintf("%x", bigx.Int64())
}

// struct to define the json layout of a scheduled job
type JobSpec struct {
	// time to start the scheduled job, in RFC3339 format.
	StartTime string
	// interval between jobs, in 00h00m00s
	Interval string
	Tube     string
	Priority uint32
	Ttr      int
	Body     string
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
	return j.Key + "->" + string(j.Body)
}

func (j *CronJob) Stop() {
	close(j.stop)
}

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

	//find the next start time based on the
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

// get the next time this jobs should run, based on the original start time
// and the interval.
func (j *CronJob) NextStart() time.Time {
	now := time.Now()
	start := j.StartTime
	for start.Before(now) {
		start = start.Add(j.Interval)
	}
	return start
}

func Schedule(job *CronJob) {
	logDebug("scheduling job", job)
	go func() {
		start := job.NextStart()
		logDebug("sleeping until", start)
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

func Submit(job *CronJob) {
	log.Println("SUBMIT:", job)
}

// check if we're master.
// elect a new one if no master is found.
// TODO: multiple cronstalks not yet supported
func amMaster() (bool, error) {
	return true, nil
}

// Retrieve all jobs from redis, and schedule them accordingly.
// Update any jobs that have changed, and remove jobs no longer in the database.
// Return error on any connection problems.
func Update() error {
	logDebug("updating")
	if RedisConn == nil {
		return errors.New("not connected to a redis server")
	}

	if iAm, err := amMaster(); err != nil {
		log.Println("error checking for master in redis")
		return err
	} else if !iAm {
		logDebug("not master")
		AllStop()
		return nil
	}

	vals, err := redis.Strings(RedisConn.Do("HGETALL", "cronstalk:schedule"))
	if err != nil {
		log.Println("error updating jobs from redis")
		return err
	}

	// set of jobs current found in the DB
	current := make(map[string]bool)

	var key string
	for i, val := range vals {
		// redis hash results are a slice of [key, val, key, val]
		if i%2 == 0 {
			key = val
			current[key] = true
			continue
		}

		job, err := NewJob(key, []byte(val))
		if err != nil {
			log.Printf("error creating job from %s->%s\n", key, val)
			continue
		}

		oldJob, ok := JobRegistry[key]
		if ok {
			if oldJob.Equals(job) {
				logDebug("job already scheduled", job)
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
		if _, ok := current[key]; !ok {
			logDebug("removing job", key, "from schedule")
			job.Stop()
			delete(JobRegistry, key)
		}
	}

	return nil
}

func AllStop() {
	log.Println("stopping all scheduled jobs")
	for k, j := range JobRegistry {
		j.Stop()
		delete(JobRegistry, k)
	}
}

// Connect, or re-connect to a redis server
func connectRedis() (err error) {
	for _, addr := range RedisServers {
		RedisConn, err = redis.Dial("tcp", addr)
		if err == nil {
			// we're connected
			break
		} else {
			log.Println("Cannot connect to redis server", err)
		}
	}
	return err
}

func logDebug(args ...interface{}) {
	if !*debug {
		return
	}
	log.Println(args...)
}

func Run() {
	log.Println("cronstalk started")

	// Make sure we have a redis server to start
	if err := connectRedis(); err != nil {
		return
	}

	for {
		// Update, or reconnect and Update
		if err := Update(); err != nil {
			log.Println(err)
			if err = connectRedis(); err != nil {
				// stop all jobs on connection error, and try again later
				AllStop()
			} else {
				Update()
			}
		}

		time.Sleep(10 * time.Second)
	}

}

func main() {
	flag.Parse()
	MyId = randId()
	RedisServers = strings.Split(*redisAddrs, ",")

	Run()
}
