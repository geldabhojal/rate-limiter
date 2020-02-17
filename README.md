# Rate Limiter
Rate Limiting in Distributed Applications using Zookeeper

PLEASE NOTE: that this is a POC, but still can be used in a production environment with small refactoring and making hardcoded sleeps and percentages configs

In a distributed environment, the state needs to be mutated by different applications running in different processes and still be able to observe the same state across them. Zookeeper like distributed stores which provide with locking mechanism seems like a very good candidate for this. The way zookeeper provides locking is through a lock path, where every client trying to aquire lock comes and adds an EPHEMERAL node in that path. Zookeeper maintains a priority queue of the clients arriving in order and provides lock in that order.

In our use case, we want to rate limit our user traffic over a certain period of time. The allowed rate(bandwidth) of the user gets refresh after every configured period called the RefreshWindow. Each individual rate limit node is able to update/see the same bandwidth utilization across using locks.

Algorithm and Design Decisions
Input: RequestSize, Error Channel
Output: error if the user has exhausted it's bandwidth the time window else nil

#### Procedure

1) Create and set user specific quota paths in zookeeper.
```
/quota/test-user/total 100
/quota/test-user/base 10
/quota/test-user/usable 100
/quota/test-user/refresh
```

2) Watch the above paths
3) Store Mtime(modified time) of the /refresh node so that it can be checked when is the refresh required
4) Start a thread which compares current with the Mtime of the refresh node to decide if the refresh is required.
 If refreshWindow is hit
           - Aquire Zookeeper Lock
           - Set Mtime on the refresh zk node to notify other rate-limit applications that a refresh is in progress. That way they don't need to refresh in case they also hit the refresh window at the same time
           - Update the /usable with /total value to refresh the quota back to full
           - Check every 'x' amount of time if the refresh is required, where 'x' completely divides the refresh window

5) Start a thread to mimic(for POC) requests coming with randomized size and the VerifyQuota using the following business logic.
```
// if no usable quota left for this window, this will not let any inconsistent response to the customer
    // because when any node marks the usable quota for that window 0 , all will see it.
    // this means to provide user with it's assigned quota, we must start with an extra base quota on start up
    if rl.usableQuotaLeft <= 0 {
        response <- errors.New("no usable quota for this refreshWindow, waiting for it to be refreshed")
        return
    }
 
    rl.quotaLeft = rl.quotaLeft - int64(reqSize)
 
    // if local quota left is 20% of the base quota, request more parallely for optimization
    if rl.quotaLeft <= int64(rl.baseQuota*20/100) && !rl.optimizing {
        go rl.addBaseQuota()
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
```

6)  update new usable quota in zookeeper with the following
```
if zk has sufficient usable quota {
	// take quota for itself
	// eg. newUsableQuota := usableQuotaLeft(70) - reqSize(15) - baseQuota(10) = 45
	newUsableQuota := usableQuotaLeft - reqSize - baseQuota
	// update zookeeper with new quota
} else {
       update usable quota with 0
}
```

#### Optimizations

1) Because we would not want to add latency to the requests coming in, one thing that can be done is requests flowing through and quota being calculated parallely, that way even though some amount of data goes through, the next requests will fail with quota exhaustion. The response from the rate limit can be update in a HashMap which is {"test-user": 0}, where 0 meaning further requests cannot be sent in this time window. 
This is something that can be decided in upstream.

2) Because every time local base quota gets over for a rate limit node, it has to go to zookeeper to ask for more before it can let the request go further, I added logic to ask for more quota concurrently, without affecting the traffic, every time 80% of the local quota is consumed and only 20% is left, that way the local quota keeps getting refilled concurrently and the requests don't wait.

A possible situation in this could be that, the requests for a specific user is skewed to one rate-limit node and it asks for more quota but suddenly it gets skewed to the other node, a small amount of quota gets wasted at that point of time that the previous node asked for. But this should not be a major concern because 
    a) we always start with an extra overall total quota than promised to the user.
    b) Relinquish excess quota that the rate-limit node might have every 15seconds back to zookeeper. In other words, if somehow(highly unlikely in my algorithm) quota gets concentrated in one node such that it is more than twice the size of our base quota. Again, we started with number of app-nodes * baseQuota as extra quota in the beginning, so loosing 2*baseQuota is not a quota leak.

3) Our base quota should be small enough to avoid leaks threw skewness. Optimization 2 can also help in reducing the number of hits to zookeeper when small base quota because we ask for more quota parallely when the base quota is about to finish.


