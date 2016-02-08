# RedisJobStore

A [Quartz Scheduler](http://quartz-scheduler.org/) JobStore that uses [Redis](http://redis.io/) for persistent storage.

## Configuration

To get [Quartz](http://quartz-scheduler.org/) up and running quickly with `RedisJobStore`, use the following example to configure your `quartz.properties` file:

    # setting the scheduler's misfire threshold, in milliseconds
    org.quartz.jobStore.misfireThreshold: 60000

    # setting the scheduler's JobStore to RedisJobStore
    org.quartz.jobStore.class: com.redislabs.quartz.RedisJobStore

    # setting your redis host
    org.quartz.jobStore.host: <your_redis_host>

    # setting your redis port
    org.quartz.jobStore.port: <your_redis_port>

    # setting your redis password (optional)
    org.quartz.jobStore.password: <your_redis_password>

    # setting a 'releaseTriggersInterval' will trigger a mechanism for releasing triggers of non-alive schedulers in a given interval, in milliseconds
    org.quartz.jobStore.releaseTriggersInterval: 600000

    # setting a 'instanceIdFilePath' will release triggers of previous schedulers on startup
    org.quartz.jobStore.instanceIdFilePath: /etc/quartz


## External Libraries

`RedisJobStore` uses the [jedis](https://github.com/xetorthio/jedis), [gson](https://code.google.com/p/google-gson/) and [jedis-lock](https://github.com/abelaska/jedis-lock) libraries, so you'll have to download them and add them to your project's classpath or define the relevant Maven dependencies:

    <dependency>
		<groupId>redis.clients</groupId>
		<artifactId>jedis</artifactId>
		<version>2.0.0</version>
	</dependency>

    <dependency>
     	<groupId>com.google.code.gson</groupId>
     	<artifactId>gson</artifactId>
     	<version>2.2.4</version>
    </dependency>

    <dependency>
      <groupId>com.github.jedis-lock</groupId>
      <artifactId>jedis-lock</artifactId>
      <version>1.0.0</version>
    </dependency>

## Limitations

`RedisJobStore` attempts to be fully compliant with all of [Quartz](http://quartz-scheduler.org/)'s features, but currently has some limitations that you should be aware of:

* Only `SimpleTrigger` and `CronTrigger`are supported.
* For any `GroupMatcher`, only a `StringOperatorName.EQUALS` operator is supported. You should note that if your scheduler is designed to compare any group of jobs, triggers, etc. with a pattern-based matcher.
* `RedisJobStore` is designed to use multiple schedulers, but it is not making any use of the `org.quartz.scheduler.instanceName`. The only limitation here is that you should maintain the uniquness of your trigger_group_name:trigger_name, and your job_group_name:job_name and you'll be good to go with multiple schedulers.
* A `Scheduler` should be started once on a machine, also to ensure releasing locked triggers of previously crashed schedulers.
* Data atomicity- `RedisJobStore` is not using any transaction-like mechanism, but ensures synchronization with global lockings. As a result, if a connection issue occurs during an operation, it might be partially completed.
* `JobDataMap` values are stored and returned as Strings, so you should implement your jobs accordingly.
* `RedisJobStore` is firing triggers only by their fire time, without any cosideration to their priorities at all.

## Known Issues

1. Quartz's standard JobStores are sometimes considering triggers without a next fire time as tirggers in a WAITING state. As `RedisJobStore` is using redis [Sorted Sets](http://redis.io/topics/data-types#sorted-sets) to maintain triggers states, using their next fire time as the score, it will consider these triggers as stateless.

## Redis Schema

To better understand the workflow and the behavior of a [Quartz Scheduler](http://quartz-scheduler.org/) using a `RedisJobStore`, you may want to review the [redis](http://redis.io/) schema in which the `RedisJobStore` is making a use of at: [/schema/schema.txt](/schema/schema.txt)

## License

[The MIT License](http://opensource.org/licenses/MIT)

[![githalytics.com alpha](https://cruel-carlota.pagodabox.com/e08b202aefce41667b99d181284b1f6e "githalytics.com")](http://githalytics.com/RedisLabs/redis-quartz)