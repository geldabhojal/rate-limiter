package main

import (
	"errors"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/curator-go/curator"
	recipe "github.com/curator-go/curator/recipes"
	zk "github.com/samuel/go-zookeeper/zk"
)

const (
	prefix        = "/quota"
	refreshSuffix = "/refresh"
	baseSuffix    = "/base"
	totalSuffix   = "/total"
	usableSuffix  = "/usable"
	// concatenate username to make a lockPath
	lockPath = "/lock-"
)

var (
	QUOTA_EXHAUSTED = errors.New("quota exhausted for time period message, waiting to be refreshed")
)

type RateLimitService interface {
	VerifyQuota(int64, chan error)
}

// RateLimit rate limit a user
type RateLimit struct {
	username          string
	usableQuotaPath   string
	baseQuotaPath     string
	totalQuotaPath    string
	refreshQuotaPath  string
	quotaLock         sync.Mutex
	m                 sync.Mutex
	lockTimeout       time.Duration
	refreshWindow     time.Duration
	baseQuota         int64
	totalAllowedQuota int64
	usableQuotaLeft   int64
	mTime             int64
	quotaLeft         int64
	lock              *recipe.InterProcessMutex
	client            curator.CuratorFramework
	optimizing        bool
}

func NewRateLimit(client curator.CuratorFramework, username string,
	totalAllowedQuota, baseQuota int64, lockTimeout time.Duration, refreshWindow time.Duration) *RateLimit {
	var err error
	rl := &RateLimit{
		username:          username,
		totalAllowedQuota: totalAllowedQuota,
		usableQuotaLeft:   totalAllowedQuota,
		baseQuota:         baseQuota,
		lockTimeout:       lockTimeout,
		refreshWindow:     refreshWindow,
		client:            client,
		baseQuotaPath:     prefix + "/" + username + baseSuffix,
		usableQuotaPath:   prefix + "/" + username + usableSuffix,
		totalQuotaPath:    prefix + "/" + username + totalSuffix,
		refreshQuotaPath:  prefix + "/" + username + refreshSuffix,
	}

	// initialize the lock to be used and inject it whereever required.
	rl.lock, err = recipe.NewInterProcessMutex(rl.client, lockPath+username)
	if err != nil {
		log.Fatalf("distributed lock to zookeeper could not be initialized", err)
	}

	rl.create(prefix, []byte(""))
	rl.create(prefix+"/"+rl.username, []byte(""))

	rl.create(rl.baseQuotaPath, []byte(strconv.FormatInt(rl.baseQuota, 10)))

	rl.create(rl.totalQuotaPath, []byte(strconv.FormatInt(rl.totalAllowedQuota, 10)))

	rl.create(rl.usableQuotaPath, []byte(strconv.FormatInt(rl.usableQuotaLeft, 10)))

	rl.create(rl.refreshQuotaPath, []byte(""))

	rl.addWatch()

	// concurrently look to refresh quota
	go rl.refreshQuota()
	// mimic user requests being processed with random size
	go rl.startRequests()
	// just in case there is skewness observed through loadbalancer and
	// quota gets concentrated on a single rate limit node
	go rl.relinquish()

	return rl
}

func (rl *RateLimit) addWatch() {
	var err error
	rl.baseQuota, err = strconv.ParseInt(string(rl.watch(rl.baseQuotaPath, rl.baseQuotaEvent)), 10, 64)
	if err != nil {
		log.Println(err)
	}
	// get and watch the overall usable quota for the user that he is consuming from
	rl.usableQuotaLeft, err = strconv.ParseInt(string(rl.watch(rl.usableQuotaPath, rl.usableQuotaEvent)), 10, 64)
	if err != nil {
		log.Println(err)
	}
	// get and watch the configured totalAllowedQuota
	rl.totalAllowedQuota, err = strconv.ParseInt(string(rl.watch(rl.totalQuotaPath, rl.totalQuotaEvent)), 10, 64)
	if err != nil {
		log.Println(err)
	}
	rl.quotaLeft = int64(rl.baseQuota)
	// check for last mTime on refresh node to use for refresh time calculations
	rl.mTime = rl.watchRefreshNode(rl.refreshQuotaPath, rl.refreshQuotaEvent)
	log.Println("last refresh happened ", time.Now().Unix()-rl.mTime, "seconds ago")
}

func (rl *RateLimit) create(path string, data []byte) error {
	var err error
	stat, err := rl.client.CheckExists().ForPath(path)
	if stat == nil {
		_, err = rl.client.Create().ForPathWithData(path, data)
	}
	if err != nil {
		return err
	}
	return nil
}

func main() {
	retryPolicy := curator.NewExponentialBackoffRetry(time.Second, 3, 15*time.Second)
	client := curator.NewClient("localhost:2181", retryPolicy)
	client.Start()
	defer client.Close()
	_ = NewRateLimit(client, "sri", 100000, 1000, 5*time.Second, 60*time.Second)

	shutdownChannel := make(chan os.Signal, 2)
	signal.Notify(shutdownChannel, syscall.SIGINT, syscall.SIGTERM)
	log.Println(<-shutdownChannel)
}

// this function mimics generating requests
func (rl *RateLimit) startRequests() {
	for {
		// start a user request every 1 second
		go func() {
			size := rl.requestSize()
			err := make(chan error)
			go rl.VerifyQuota(size, err)
			if e := <-err; e != nil {
				// respond to the user about quota exhaustion
				log.Println(e)
			} else {
				log.Println("request OK to be sent downstream")
			}
			return
		}()
		time.Sleep(500 * time.Millisecond)
	}
}

func (rl *RateLimit) requestSize() int64 {
	var size int
	for {
		s1 := rand.NewSource(time.Now().UnixNano())
		r1 := rand.New(s1)
		size = r1.Intn(15)
		// avoiding a request of size 0 to be returned
		if size != 0 {
			break
		}
	}
	return int64(size)
}

func (rl *RateLimit) refreshQuota() {
	for {
		now := time.Now().Unix()
		// Note: Because we Aquire the lock to refresh quota, we should keep refreshWindow to a reasonable value and not like every few seconds
		if now-rl.mTime >= int64(rl.refreshWindow.Seconds()) {
			ok, err := rl.lock.Acquire()
			if err != nil {
				log.Println(err)
				return
			}
			defer rl.lock.Release()
			if ok {
				// refresh only if the usableQuotaLeft is <=total allowed quota
				// check for time.Now().Unix()-mTime again incase another node just refreshed
				if rl.usableQuotaLeft <= rl.totalAllowedQuota && time.Now().Unix()-rl.mTime >= int64(rl.refreshWindow.Seconds()) {
					// set anything on the refresh node to update it's mTime
					err := rl.set(rl.refreshQuotaPath, []byte(""))
					if err != nil {
						log.Println("resetting quota failed ", err)
						return
					}
					// refresh the quota with totalQuota for this new window.
					err = rl.set(rl.usableQuotaPath, []byte(strconv.FormatInt(rl.totalAllowedQuota, 10)))
					if err != nil {
						log.Println("refreshing quota failed ", err)
						return
					} else {
						rl.lock.Release()
						log.Println("QUOTA REFRESHED")
					}
				} else if rl.usableQuotaLeft > rl.totalAllowedQuota {
					rl.lock.Release()
					// we can decide to make the system self correct itself by setting /quotas/usable equals to /quotas/total
					log.Println("WARN: usable quota is more than total allowed , an impossible case unless somebody manually changed the 'quota/usable' in zookeeper")
				} else {
					// some other rate-limit node refreshed or this is the first time the application is starting up
					log.Println("the quota was recently refreshed, need not be refreshed")
					rl.lock.Release()
				}
			}
		}
		//For eg. if 100MB/30min is the limit, it makes sense check every 10 min for refreshment
		// 30min/60 *2 = 600sec = 10min
		time.Sleep(time.Duration(rl.refreshWindow.Seconds()/60) * 2 * time.Second)
	}
}

// VerifyQuota take the size of the request and calculates usable bandwidth
// we should be using a channel for the user request to wait on GetQuota's
// response which is if the user has exhausted it's quota or not
func (rl *RateLimit) VerifyQuota(reqSize int64, response chan error) {
	rl.m.Lock()
	defer rl.m.Unlock()
	log.Println("request size received", reqSize)

	// if no usable quota left for this window, this will not let any inconsistent response to the customer
	// because when any node marks the usable quota for that window 0 , all will see it.
	// this means to provide user with it's assigned quota, we must start with an extra base quota on start up
	if rl.usableQuotaLeft <= 0 {
		response <- errors.New("no usable quota for this refreshWindow, waiting for it to be refreshed")
		return
	}
	rl.quotaLock.Lock()
	rl.quotaLeft = rl.quotaLeft - int64(reqSize)
	rl.quotaLock.Unlock()

	// if local quota left is 20% of the base quota, request more parallely for optimization
	if rl.quotaLeft <= int64(rl.baseQuota*20/100) && !rl.optimizing {
		go rl.getQuota()
	}

	if rl.quotaLeft >= 0 {
		log.Println("user has bandwidth to pass through request with size  ", int64(reqSize), " local quota left ", rl.quotaLeft)
		response <- nil
		return
	} else {
		log.Println("base quota not enough for the request size, requesting more..")
		// request more local quota from zookeeper
		rl.requestQuota(reqSize, response)
	}
}

func (rl *RateLimit) requestQuota(reqSize int64, response chan error) {
	ok, err := rl.lock.Acquire()
	if err != nil {
		log.Println(err)
		return
	}
	defer rl.lock.Release()
	if ok {
		// this rate-limit node has used up all the usableQuotaLeft
		if reqSize > rl.usableQuotaLeft {
			response <- QUOTA_EXHAUSTED
		} else {
			// let the request go through since user has usable bandwidth for refreshWindow
			response <- nil
		}
		log.Println("overall usable quota known is", rl.usableQuotaLeft)
		// update usableQuota in store with the new reduced value
		if rl.usableQuotaLeft-rl.baseQuota > 0 && rl.usableQuotaLeft-reqSize >= rl.baseQuota {
			// take quota for itself
			rl.quotaLock.Lock()
			rl.quotaLeft = int64(rl.baseQuota)
			rl.quotaLock.Unlock()
			newUsableQuota := rl.usableQuotaLeft - reqSize - rl.quotaLeft
			err := rl.set(rl.usableQuotaPath, []byte(strconv.FormatInt(newUsableQuota, 10)))
			if err != nil {
				log.Println("updating usable quota failed ", err)
				return
			}
			log.Println("took ", rl.baseQuota, " from overall usable quota, updating usable quota to store:", newUsableQuota)
			return
		} else {
			// adding extra safety check here to avoid case where in the event from the 'just' happened refresh hasn't reached
			// by the time this condition aquires the lock and try's to process this else condition and set the usableQuota back to 0
			// HIGHLY unlikely and rare but you never know...
			if rl.usableQuotaLeft <= rl.baseQuota {
				// update usableQuota with 0 since user does not have usable quota and wait for the refreshWindow
				err := rl.set(rl.usableQuotaPath, []byte(strconv.FormatInt(0, 10)))
				if err != nil {
					log.Println("updating usable quota failed ", err)
					return
				}
				log.Println("last quota bucket did not have enought, waiting for refresh")
			}
		}
	}
}

// just in case there is skewness observed through loadbalancer and
// quota gets concentrated on a single rate limit node
func (rl *RateLimit) relinquish() {
	for {
		if rl.quotaLeft > 2*rl.baseQuota {
			ok, err := rl.lock.Acquire()
			if err != nil {
				log.Println(err)
				return
			}
			defer rl.lock.Release()
			if ok {
				rl.quotaLock.Lock()
				rl.usableQuotaLeft = rl.usableQuotaLeft + rl.quotaLeft - rl.baseQuota
				rl.quotaLeft = rl.baseQuota
				log.Println("had too much quota, relinquising ", rl.quotaLeft-rl.baseQuota)
				err := rl.set(rl.usableQuotaPath, []byte(strconv.FormatInt(rl.usableQuotaLeft, 10)))
				if err != nil {
					log.Println("updating usable quota failed ", err)
					return
				}
			}
		}
		time.Sleep(15 * time.Second)
	}
}

func (rl *RateLimit) getQuota() {
	rl.optimizing = true
	defer func() {
		rl.optimizing = false
	}()
	ok, err := rl.lock.Acquire()
	if err != nil {
		log.Println(err)
		return
	}
	defer rl.lock.Release()
	if ok {
		log.Println("optimizing by pulling 20% base quota")
		if rl.usableQuotaLeft >= rl.baseQuota {
			rl.quotaLock.Lock()
			rl.quotaLeft = int64(float64(rl.quotaLeft) + float64(0.4)*float64(rl.baseQuota))
			rl.quotaLock.Unlock()
			log.Println("new local quota ", rl.quotaLeft)
			newUsableQuota := int64(float64(rl.usableQuotaLeft) - float64(0.4)*float64(rl.baseQuota))
			err := rl.set(rl.usableQuotaPath, []byte(strconv.FormatInt(newUsableQuota, 10)))
			if err != nil {
				log.Println("updating usable quota failed ", err)
				return
			}
			log.Println("optimization:took ", int64(float64(0.4)*float64(rl.baseQuota)), " from overall usable quota, updating usable quota to store:", newUsableQuota)
		} else {
			log.Println("not enough usable quota left for optimization")
		}
	}
}

func (rl *RateLimit) set(path string, data []byte) error {
	_, err := rl.client.SetData().ForPathWithData(path, data)
	return err
}

func (rl *RateLimit) watch(path string, callback func(*zk.Event)) []byte {
	b, err := rl.client.GetData().UsingWatcher(curator.NewWatcher(callback)).ForPath(path)
	if err != nil {
		log.Fatal("watcher failed ", err)
	}
	q, err := strconv.ParseInt(string(b), 10, 64)
	log.Println(path, " quota for all goroutines was updated as ", q)
	return b
}

func (rl *RateLimit) watchRefreshNode(path string, callback func(*zk.Event)) int64 {
	stat, err := rl.client.CheckExists().UsingWatcher(curator.NewWatcher(callback)).ForPath(path)
	if err != nil {
		log.Fatalf("check exists failed", err)
	}
	if stat == nil {
		log.Fatal("Fatal: refresh node does not exist in zookeeper")
	}
	return stat.Mtime / 1000
}

func (rl *RateLimit) usableQuotaEvent(event *zk.Event) {
	rl.quotaLock.Lock()
	defer rl.quotaLock.Unlock()
	var err error
	rl.usableQuotaLeft, err = strconv.ParseInt(string(rl.watch(rl.usableQuotaPath, rl.usableQuotaEvent)), 10, 64)
	if err != nil {
		log.Println(err)
	}
}

func (rl *RateLimit) totalQuotaEvent(event *zk.Event) {
	rl.quotaLock.Lock()
	defer rl.quotaLock.Unlock()
	var err error
	rl.totalAllowedQuota, err = strconv.ParseInt(string(rl.watch(rl.totalQuotaPath, rl.totalQuotaEvent)), 10, 64)
	if err != nil {
		log.Println(err)
	}
}

func (rl *RateLimit) baseQuotaEvent(event *zk.Event) {
	rl.quotaLock.Lock()
	defer rl.quotaLock.Unlock()
	var err error
	rl.baseQuota, err = strconv.ParseInt(string(rl.watch(rl.baseQuotaPath, rl.baseQuotaEvent)), 10, 64)
	if err != nil {
		log.Println(err)
	}
}

func (rl *RateLimit) refreshQuotaEvent(event *zk.Event) {
	rl.mTime = rl.watchRefreshNode(rl.refreshQuotaPath, rl.refreshQuotaEvent)
}
