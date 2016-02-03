package com.redislabs.quartz;

import com.github.jedis.lock.JedisLock;
import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.quartz.Calendar;
import org.quartz.CronTrigger;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.JobKey;
import org.quartz.JobPersistenceException;
import org.quartz.ObjectAlreadyExistsException;
import org.quartz.PersistJobDataAfterExecution;
import org.quartz.SchedulerConfigException;
import org.quartz.SchedulerException;
import org.quartz.SimpleTrigger;
import org.quartz.Trigger;
import org.quartz.Trigger.CompletedExecutionInstruction;
import org.quartz.Trigger.TriggerState;
import org.quartz.TriggerKey;
import org.quartz.impl.matchers.GroupMatcher;
import org.quartz.impl.matchers.StringMatcher.StringOperatorName;
import org.quartz.impl.triggers.CronTriggerImpl;
import org.quartz.impl.triggers.SimpleTriggerImpl;
import org.quartz.spi.ClassLoadHelper;
import org.quartz.spi.JobStore;
import org.quartz.spi.OperableTrigger;
import org.quartz.spi.SchedulerSignaler;
import org.quartz.spi.TriggerFiredBundle;
import org.quartz.spi.TriggerFiredResult;
import org.quartz.utils.ClassUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Tuple;

/**
 * <p>
 * This class implements a <code>{@link org.quartz.spi.JobStore}</code> that
 * utilizes <a href="http://redis.io/">Redis</a> as a persistent storage.
 * </p>
 * 
 * @author Matan Kehat
 */
public class RedisJobStore implements JobStore {
	
	/*** Sets Keys ***/
	private static final String JOBS_SET = "jobs";
	private static final String JOB_GROUPS_SET = "job_groups";
	private static final String BLOCKED_JOBS_SET = "blocked_jobs";
	private static final String TRIGGERS_SET = "triggers";
	private static final String TRIGGER_GROUPS_SET = "trigger_groups";
	private static final String CALENDARS_SET = "calendars";
	private static final String PAUSED_TRIGGER_GROUPS_SET = "paused__trigger_groups";
	private static final String PAUSED_JOB_GROUPS_SET = "paused_job_groups";
		
	/*** Job Hash Fields ***/
	private static final String JOB_CLASS = "job_class_name";
	private static final String DESCRIPTION = "description";
	private static final String IS_DURABLE = "is_durable";
	private static final String BLOCKED_BY = "blocked_by";
	private static final String BLOCK_TIME = "block_time";
		
	/*** Trigger Hash Fields ***/
	private static final String JOB_HASH_KEY = "job_hash_key";
	private static final String NEXT_FIRE_TIME = "next_fire_time";
	private static final String PREV_FIRE_TIME = "prev_fire_time";
	private static final String PRIORITY = "priority";
	private static final String TRIGGER_TYPE = "trigger_type";
	private static final String CALENDAR_NAME = "calendar_name";
	private static final String START_TIME = "start_time";
	private static final String END_TIME = "end_time";
	private static final String FINAL_FIRE_TIME = "final_fire_time";
	private static final String FIRE_INSTANCE_ID = "fire_instance_id";
	private static final String MISFIRE_INSTRUCTION = "misfire_instruction";
	private static final String LOCKED_BY = "locked_by";
	private static final String LOCK_TIME = "lock_time";
		
	/*** Simple Trigger Fields ***/
	private static final String TRIGGER_TYPE_SIMPLE = "SIMPLE";
	private static final String REPEAT_COUNT = "repeat_count";
	private static final String REPEAT_INTERVAL = "repeat_interval";
	private static final String TIMES_TRIGGERED = "times_triggered";
	
	/*** Cron Trigger Fields ***/
	private static final String TRIGGER_TYPE_CRON = "CRON";
	private static final String CRON_EXPRESSION = "cron_expression";
	private static final String TIME_ZONE_ID = "time_zone_id";
	
	/*** Calendar Hash Fields ***/	
	private static final String CALENDAR_CLASS = "calendar_class";
	private static final String CALENDAR_SERIALIZED = "calendar_serialized";
	
	/*** Misc Keys ***/
	private static final String LAST_TRIGGERS_RELEASE_TIME = "last_triggers_release_time";
	private static final String LOCK = "lock";
	
	/*** Channels ***/
	private static final String UNLOCK_NOTIFICATIONS_CHANNEL = "unlock-notificactions";
	 
	/*** Class Members ***/
	private final Logger log = LoggerFactory.getLogger(getClass());
	private static JedisPool pool;	
   private static JedisLock lockPool;
	private ClassLoadHelper loadHelper;
	private SchedulerSignaler signaler;
	private String instanceId;
	private String host;
	private int port;
	private String password;
	private String instanceIdFilePath;
	private int releaseTriggersInterval; // In seconds 
	private int lockTimeout; 
	
	protected long misfireThreshold = 60000L;
		
	
	@Override
	public void initialize(ClassLoadHelper loadHelper,
			SchedulerSignaler signaler) throws SchedulerConfigException {
		
		this.loadHelper = loadHelper;
		this.signaler = signaler;		
	   
    	// initializing a connection pool
    	JedisPoolConfig config = new JedisPoolConfig();
    	if (password != null)
    		pool = new JedisPool(config, host, port, Protocol.DEFAULT_TIMEOUT, password);	    		
    	else
    		pool = new JedisPool(config, host, port, Protocol.DEFAULT_TIMEOUT);
    	
    	// initializing a locking connection pool with a longer timeout
    	if (lockTimeout == 0)
    		lockTimeout = 10 * 60 * 1000; // 10 Minutes locking timeout
    	
      lockPool = new JedisLock(pool.getResource(), "JobLock", lockTimeout);

	}	

	@Override
	public void schedulerStarted() throws SchedulerException {
		if (this.instanceIdFilePath != null) {
			String path = this.instanceIdFilePath + File.separator + "scheduler.id";
			File instanceIdFile = new File(path);
			
			// releasing triggers that a previous scheduler instance didn't release		
			String lockedByInstanceId = readInstanceId(instanceIdFile);
			if (lockedByInstanceId != null) {
				releaseLockedTriggers(lockedByInstanceId, RedisTriggerState.ACQUIRED, RedisTriggerState.WAITING);
				releaseLockedTriggers(lockedByInstanceId, RedisTriggerState.BLOCKED, RedisTriggerState.WAITING);
				releaseLockedTriggers(lockedByInstanceId, RedisTriggerState.PAUSED_BLOCKED, RedisTriggerState.PAUSED);
				
				releaseBlockedJobs(lockedByInstanceId);
			}
			
			// writing the current instance id to a file
			writeInstanceId(instanceIdFile);			
		}			
	}

	@Override
	public void schedulerPaused() {
		// No-op
	}

	@Override
	public void schedulerResumed() {
		// No-op
	}

	@Override
	public void shutdown() {
		if (pool != null)
			pool.destroy();
   }

	@Override
	public boolean supportsPersistence() {
		return true;
	}

	@Override
	public long getEstimatedTimeToReleaseAndAcquireTrigger() {
		// varied
		return 100;
	}

	@Override
	public boolean isClustered() {
		return true;
	}

	@Override
	public void storeJobAndTrigger(JobDetail newJob, OperableTrigger newTrigger)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		String jobHashKey = createJobHashKey(newJob.getKey().getGroup(), newJob.getKey().getName());
		String triggerHashKey = createTriggerHashKey(newTrigger.getKey().getGroup(), newTrigger.getKey().getName());
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			storeJob(newJob, false, jedis);
			storeTrigger(newTrigger, false, jedis);			
      } catch (ObjectAlreadyExistsException ex) {
         log.warn("could not store job: " + jobHashKey + " and trigger: " + triggerHashKey, ex);
      } catch (Exception ex) {
         log.error("could not store job: " + jobHashKey + " and trigger: " + triggerHashKey, ex);
         throw new JobPersistenceException(ex.getMessage(), ex.getCause());
      } finally {
         lockPool.release();
		}		
	}

	@Override
   public void storeJob(JobDetail newJob, boolean replaceExisting) throws JobPersistenceException {
		String jobHashKey = createJobHashKey(newJob.getKey().getGroup(), newJob.getKey().getName());
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			storeJob(newJob, replaceExisting, jedis);
		} catch (ObjectAlreadyExistsException ex) {
			log.warn(jobHashKey + " already exists", ex);
			throw ex;
		} catch (Exception ex) {
			log.error("could not store job: " + jobHashKey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}		
	}

	/**
	 * Stores job in redis.
	 *
	 * @param newJob the new job
	 * @param replaceExisting the replace existing
	 * @param jedis thread-safe redis connection
	 * @throws ObjectAlreadyExistsException
	 */
	private void storeJob(JobDetail newJob, boolean replaceExisting, Jedis jedis)
			throws ObjectAlreadyExistsException {
		String jobHashKey = createJobHashKey(newJob.getKey().getGroup(), newJob.getKey().getName());
		String jobDataMapHashKey = createJobDataMapHashKey(newJob.getKey().getGroup(), newJob.getKey().getName());
		String jobGroupSetKey = createJobGroupSetKey(newJob.getKey().getGroup());
		
		if (jedis.exists(jobHashKey) && !replaceExisting)
			throw new ObjectAlreadyExistsException(newJob);
					
      Map<String, String> jobDetails = new HashMap<>();
		jobDetails.put(DESCRIPTION, newJob.getDescription() != null ? newJob.getDescription() : "");
		jobDetails.put(JOB_CLASS, newJob.getJobClass().getName());
		jobDetails.put(IS_DURABLE, Boolean.toString(newJob.isDurable()));
		jobDetails.put(BLOCKED_BY, "");
		jobDetails.put(BLOCK_TIME, "");
		jedis.hmset(jobHashKey, jobDetails);
		
		if (newJob.getJobDataMap() != null && !newJob.getJobDataMap().isEmpty())
			jedis.hmset(jobDataMapHashKey, getStringDataMap(newJob.getJobDataMap()));			
		
		jedis.sadd(JOBS_SET, jobHashKey);
		jedis.sadd(JOB_GROUPS_SET, jobGroupSetKey);
		jedis.sadd(jobGroupSetKey, jobHashKey);
	}
	
	/**
	 * Creates a job data map key in the form of: 'job_data_map:&#60;job_group_name&#62;:&#60;job_name&#62;'
	 *
	 * @param groupName the job group name
	 * @param jobName the job name
	 * @return the job data map key
	 */
	private String createJobDataMapHashKey(String groupName, String jobName) {
		return "job_data_map:" + groupName + ":" + jobName;
	}

	/**
	 * Creates a job group set key in the form of: 'job_group:&#60;job_group_name&#62;'
	 *
	 * @param groupName the group name
	 * @return the job group set key
	 */
	private String createJobGroupSetKey(String groupName) {
		return "job_group:" + groupName;
	}

	/**
	 * Creates a job key in the form of: 'job:&#60;job_group_name&#62;:&#60;job_name&#62;'
	 *
	 * @param groupName the job group name
	 * @param jobName the job name
	 * @return the job key
	 */
	private String createJobHashKey(String groupName, String jobName) {
		return "job:" + groupName + ":" + jobName;
	}
	
	/**
	 * Creates a calendar hash key in the form of: 'calendar:&#60;calendar_name&#62;'
	 *
	 * @param calendarName the calendar name
	 * @return the calendar key
	 */
	private String createCalendarHashKey(String calendarName) {
		return "calendar:" + calendarName;
	}

	private Map<String, String> getStringDataMap(JobDataMap jobDataMap) {
      Map<String, String> stringDataMap = new HashMap<>();
		for (String key : jobDataMap.keySet())
			stringDataMap.put(key, jobDataMap.get(key).toString());
					
		return stringDataMap;
	}

	@Override
	public void storeJobsAndTriggers(
			Map<JobDetail, Set<? extends Trigger>> triggersAndJobs,
			boolean replace) throws ObjectAlreadyExistsException,
			JobPersistenceException {
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			
			// verifying jobs and triggers don't exist
			if (!replace) {
				for (Entry<JobDetail, Set<? extends Trigger>> entry : triggersAndJobs.entrySet()) {
					String jobHashKey = createJobHashKey(entry.getKey().getKey().getGroup(), entry.getKey().getKey().getName());
					if (jedis.exists(jobHashKey))
						throw new ObjectAlreadyExistsException(entry.getKey());
					
					for (Trigger trigger : entry.getValue()) {
						String triggerHashKey = createTriggerHashKey(trigger.getKey().getGroup(), trigger.getKey().getName());
						if (jedis.exists(triggerHashKey))
							throw new ObjectAlreadyExistsException(trigger);
					}
				}
			}
			
			// adding jobs and triggers
			for (Entry<JobDetail, Set<? extends Trigger>> entry : triggersAndJobs.entrySet()) {
				storeJob(entry.getKey(), true, jedis);
				for (Trigger trigger: entry.getValue())
					storeTrigger((OperableTrigger) trigger, true, jedis);
			}			
		} catch (Exception ex) {
			log.error("could not store jobs and triggers", ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
	}

	@Override
	public boolean removeJob(JobKey jobKey) throws JobPersistenceException {
		boolean removed = false;
		String jobHashKey = createJobHashKey(jobKey.getGroup(), jobKey.getName());
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			if (jedis.exists(jobHashKey)) {
				removeJob(jobKey, jedis);
				removed = true;
			}			
		} catch (Exception ex) {
			log.error("could not remove job: " + jobHashKey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
		return removed;
	}

	/**
	 * Removes the job from redis.
	 *
	 * @param jobKey the job key
	 * @param jedis thread-safe redis connection
	 */
	private void removeJob(JobKey jobKey, Jedis jedis) {
		String jobHashKey = createJobHashKey(jobKey.getGroup(), jobKey.getName());
		String jobDataMapHashKey = createJobDataMapHashKey(jobKey.getGroup(), jobKey.getName());
		String jobGroupSetKey = createJobGroupSetKey(jobKey.getGroup());
		
		jedis.del(jobHashKey);
		jedis.del(jobDataMapHashKey);
		jedis.srem(JOBS_SET, jobHashKey);
		jedis.srem(BLOCKED_JOBS_SET, jobHashKey);
		jedis.srem(jobGroupSetKey, jobHashKey);
		if (jedis.scard(jobGroupSetKey) == 0) {
			jedis.srem(JOB_GROUPS_SET, jobGroupSetKey);
		}
	}

	@Override
	public boolean removeJobs(List<JobKey> jobKeys)
			throws JobPersistenceException {
      boolean removed = jobKeys.size() > 0;
		for (JobKey jobKey : jobKeys)
			removed = removed && removeJob(jobKey);
		
		return removed;
	}
	
	@Override
	public JobDetail retrieveJob(JobKey jobKey) throws JobPersistenceException {
		JobDetail jobDetail = null;
		String jobHashkey = createJobHashKey(jobKey.getGroup(), jobKey.getName());
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			jobDetail = retrieveJob(jobKey, jedis);			
      } catch (JobPersistenceException | ClassNotFoundException | InterruptedException ex) {
			log.error("could not retrieve job: " + jobHashkey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
		return jobDetail;
	}

	/**
	 * Retrieves job from redis.
	 *
	 * @param jobKey the job key
	 * @param jedis thread-safe redis connection
	 * @return the job detail
	 * @throws JobPersistenceException
	 */
	@SuppressWarnings("unchecked")
	private JobDetail retrieveJob(JobKey jobKey, Jedis jedis) throws JobPersistenceException, ClassNotFoundException {
		String jobHashkey = createJobHashKey(jobKey.getGroup(), jobKey.getName());
		String jobDataMapHashKey = createJobDataMapHashKey(jobKey.getGroup(), jobKey.getName());
      if (!jedis.exists(jobHashkey)) {
         log.warn("job: " + jobHashkey + " does not exist");
         return null;
      }
		Class<Job> jobClass = (Class<Job>) loadHelper.getClassLoader().loadClass(jedis.hget(jobHashkey, JOB_CLASS));
		JobBuilder jobBuilder = JobBuilder.newJob(jobClass)
									.withIdentity(jobKey)
									.withDescription(jedis.hget(jobHashkey, DESCRIPTION))
									.storeDurably(Boolean.getBoolean(jedis.hget(jobHashkey, IS_DURABLE)));
								
		Set<String> jobDataMapFields = jedis.hkeys(jobDataMapHashKey);
		if (!jobDataMapFields.isEmpty()) {
			for (String jobDataMapField : jobDataMapFields)
				jobBuilder.usingJobData(jobDataMapField, jedis.hget(jobDataMapHashKey, jobDataMapField));							
		}
		
		return jobBuilder.build();		
	}

	@Override
	public void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		String triggerHashKey = createTriggerHashKey(newTrigger.getKey().getGroup(), newTrigger.getKey().getName());
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			storeTrigger(newTrigger, replaceExisting, jedis);
		} catch (ObjectAlreadyExistsException ex) {
			log.warn(triggerHashKey + " already exists", ex);
			throw ex;
		} catch (Exception ex) {
			log.error("could not store trigger: " + triggerHashKey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
	}

	/**
	 * Stores trigger in redis.
	 *
	 * @param newTrigger the new trigger
	 * @param replaceExisting replace existing
	 * @param jedis thread-safe redis connection
	 * @throws JobPersistenceException
	 * @throws ObjectAlreadyExistsException
	 */
	private void storeTrigger(OperableTrigger newTrigger, boolean replaceExisting, Jedis jedis)
           throws JobPersistenceException {
		String triggerHashKey = createTriggerHashKey(newTrigger.getKey().getGroup(), newTrigger.getKey().getName());
		String triggerGroupSetKey = createTriggerGroupSetKey(newTrigger.getKey().getGroup());
		String jobHashkey = createJobHashKey(newTrigger.getJobKey().getGroup(), newTrigger.getJobKey().getName());
		String jobTriggerSetkey = createJobTriggersSetKey(newTrigger.getJobKey().getGroup(), newTrigger.getJobKey().getName());
		
      if (jedis.exists(triggerHashKey) && !replaceExisting) {
         ObjectAlreadyExistsException ex = new ObjectAlreadyExistsException(newTrigger);
         log.warn(ex.toString());
      }
      Map<String, String> trigger = new HashMap<>();
		trigger.put(JOB_HASH_KEY, jobHashkey);
		trigger.put(DESCRIPTION, newTrigger.getDescription() != null ? newTrigger.getDescription() : "");
		trigger.put(NEXT_FIRE_TIME, newTrigger.getNextFireTime() != null ? Long.toString(newTrigger.getNextFireTime().getTime()) : "");
		trigger.put(PREV_FIRE_TIME, newTrigger.getPreviousFireTime() != null ? Long.toString(newTrigger.getPreviousFireTime().getTime()) : "");
		trigger.put(PRIORITY, Integer.toString(newTrigger.getPriority()));
		trigger.put(START_TIME, newTrigger.getStartTime() != null ? Long.toString(newTrigger.getStartTime().getTime()) : "");
		trigger.put(END_TIME, newTrigger.getEndTime() != null ? Long.toString(newTrigger.getEndTime().getTime()) : "");
		trigger.put(FINAL_FIRE_TIME, newTrigger.getFinalFireTime() != null ? Long.toString(newTrigger.getFinalFireTime().getTime()) : "");
		trigger.put(FIRE_INSTANCE_ID, newTrigger.getFireInstanceId() != null ?  newTrigger.getFireInstanceId() : "");
		trigger.put(MISFIRE_INSTRUCTION, Integer.toString(newTrigger.getMisfireInstruction()));
		trigger.put(CALENDAR_NAME, newTrigger.getCalendarName() != null ? newTrigger.getCalendarName() : "");
		if (newTrigger instanceof SimpleTrigger) {
			trigger.put(TRIGGER_TYPE, TRIGGER_TYPE_SIMPLE);
			trigger.put(REPEAT_COUNT, Integer.toString(((SimpleTrigger) newTrigger).getRepeatCount()));
			trigger.put(REPEAT_INTERVAL, Long.toString(((SimpleTrigger) newTrigger).getRepeatInterval()));
			trigger.put(TIMES_TRIGGERED, Integer.toString(((SimpleTrigger) newTrigger).getTimesTriggered()));
		} else if (newTrigger instanceof CronTrigger) {
			trigger.put(TRIGGER_TYPE, TRIGGER_TYPE_CRON);
			trigger.put(CRON_EXPRESSION, ((CronTrigger) newTrigger).getCronExpression() != null ? ((CronTrigger) newTrigger).getCronExpression() : "");
			trigger.put(TIME_ZONE_ID, ((CronTrigger) newTrigger).getTimeZone().getID() != null ? ((CronTrigger) newTrigger).getTimeZone().getID() : "");
		} else { // other trigger types are not supported
			 throw new UnsupportedOperationException();
		}		
		
		jedis.hmset(triggerHashKey, trigger);
		jedis.sadd(TRIGGERS_SET, triggerHashKey);
		jedis.sadd(TRIGGER_GROUPS_SET, triggerGroupSetKey);			
		jedis.sadd(triggerGroupSetKey, triggerHashKey);
		jedis.sadd(jobTriggerSetkey, triggerHashKey);
		if (newTrigger.getCalendarName() != null && !newTrigger.getCalendarName().isEmpty()) { // storing the trigger for calendar, if exists
			String calendarTriggersSetKey = createCalendarTriggersSetKey(newTrigger.getCalendarName());
			jedis.sadd(calendarTriggersSetKey, triggerHashKey);
		}
		
		if (jedis.sismember(PAUSED_TRIGGER_GROUPS_SET, triggerGroupSetKey) || jedis.sismember(PAUSED_JOB_GROUPS_SET, createJobGroupSetKey(newTrigger.getJobKey().getGroup()))) {	
			long nextFireTime = newTrigger.getNextFireTime() != null ? newTrigger.getNextFireTime().getTime() : -1;
			if (jedis.sismember(BLOCKED_JOBS_SET, jobHashkey))
				setTriggerState(RedisTriggerState.PAUSED_BLOCKED, (double)nextFireTime, triggerHashKey);
			else
				setTriggerState(RedisTriggerState.PAUSED, (double)nextFireTime, triggerHashKey);
		} else if (newTrigger.getNextFireTime() != null) {			
			setTriggerState(RedisTriggerState.WAITING, (double)newTrigger.getNextFireTime().getTime(), triggerHashKey);
		}
	}

	/**
	 * Creates a calendar_triggers hash key in the form of: <br>
	 * 'calendar_triggers:&#60;calendar_name&#62;'
	 *
	 * @param calendarName the calendar name
	 * @return the calendar_triggers set key
	 */
	private String createCalendarTriggersSetKey(String calendarName) {
		return "calendar_triggers:" + calendarName;
	}

	/**
	 * Creates a job_triggers set key in the form of: <br>
	 * 'job_triggers:&#60;job_group_name&#62;:&#60;job_name&#62;'
	 *
	 * @param jobGroupName the job group name
	 * @param jobName the job name
	 * @return the job_triggers set key
	 */
	private String createJobTriggersSetKey(String jobGroupName, String jobName) {
		return "job_triggers:" + jobGroupName + ":" + jobName;
	}

	/**
	 * Creates a trigger group hash key in the form of: <br>
	 * 'trigger_group:&#60;trigger_group_name&#62;'
	 *
	 * @param groupName the group name
	 * @return the trigger group set key
	 */
	private String createTriggerGroupSetKey(String groupName) {
		return "trigger_group:" + groupName;
	}

	/**
	 * Creates a trigger hash key in the form of: <br>
	 * 'trigger:&#60;trigger_group_name&#62;:&#60;trigger_name&#62;'
	 * 
	 * @param groupName the group name
	 * @param triggerName the trigger name
	 * @return the trigger hash key
	 */
	private String createTriggerHashKey(String groupName, String triggerName) {
		return "trigger:" + groupName + ":" + triggerName;
	}

	@Override
	public boolean removeTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		boolean removed = false;
		String triggerHashKey = createTriggerHashKey(triggerKey.getGroup(), triggerKey.getName());
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			if (jedis.exists(triggerHashKey)) {
				removeTrigger(triggerKey, jedis);
				removed = true;
			}			
		} catch (Exception ex) {
			log.error("could not remove trigger: " + triggerHashKey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
		return removed;		
	}

	/**
	 * Removes the trigger from redis.
	 *
	 * @param triggerKey the trigger key
	 * jedis thread-safe redis connection
	 * @throws JobPersistenceException
	 */
	private void removeTrigger(TriggerKey triggerKey, Jedis jedis) 
			throws JobPersistenceException {
		String triggerHashKey = createTriggerHashKey(triggerKey.getGroup(), triggerKey.getName());
		String triggerGroupSetKey = createTriggerGroupSetKey(triggerKey.getGroup());
		
		jedis.srem(TRIGGERS_SET, triggerHashKey);
		jedis.srem(triggerGroupSetKey, triggerHashKey);
		
		String jobHashkey = jedis.hget(triggerHashKey, JOB_HASH_KEY);
		String jobTriggerSetkey = createJobTriggersSetKey(jobHashkey.split(":")[1], jobHashkey.split(":")[2]); 
		jedis.srem(jobTriggerSetkey, triggerHashKey);
		
		if (jedis.scard(triggerGroupSetKey) == 0) {
			jedis.srem(TRIGGER_GROUPS_SET, triggerGroupSetKey);
		}
		
		// handling orphaned jobs
		if (jedis.scard(jobTriggerSetkey) == 0 && jedis.exists(jobHashkey)) {
			if (!Boolean.parseBoolean(jedis.hget(jobHashkey, IS_DURABLE))) {
				JobKey jobKey = new JobKey(jobHashkey.split(":")[2], jobHashkey.split(":")[1]);
				removeJob(jobKey, jedis);
				signaler.notifySchedulerListenersJobDeleted(jobKey);
			}				
		}
		
		String calendarName = jedis.hget(triggerHashKey, CALENDAR_NAME);
		if (!calendarName.isEmpty()) {
			String calendarTriggersSetKey = createCalendarTriggersSetKey(calendarName);
			jedis.srem(calendarTriggersSetKey, triggerHashKey);			
		}			
			
		// removing trigger state
		unsetTriggerState(triggerHashKey);		
		jedis.del(triggerHashKey);
	}

	/**
	 * Sets a trigger state by adding the trigger to a relevant sorted set, using it's next fire time as the score.<br>
	 * The caller should handle synchronization.
	 *
	 * @param state the new state to be set
	 * @param score the trigger's next fire time
	 * @param triggerHashKey the trigger hash key
	 * @return 1- if set, 0- if the trigger is already member of the state's sorted set and it's score was updated, -1- if setting state failed
	 * @throws JobPersistenceException
	 */
	private long setTriggerState(RedisTriggerState state, double score, String triggerHashKey) throws JobPersistenceException {
		long success = -1;
      try (Jedis jedis = pool.getResource()) {
			long removed = unsetTriggerState(triggerHashKey);			
			if (state != null && removed >= 0)
				success = jedis.zadd(state.getKey(), score, triggerHashKey);			
		} catch (Exception ex) {
         log.error("could not set state " + state + " for trigger: " + triggerHashKey);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
      }
		return success;		
	}
	
	/**
	 * Unsets a state of a given trigger key by removing the trigger from the trigger state zsets. <br>
	 * The caller should handle synchronization.
	 *
	 * @param triggerHashKey the trigger hash key
	 * @param jedis the redis client
	 * @return 1- if removed, 0- if the trigger was stateless, -1- if unset state failed  
	 * @throws JobPersistenceException 
	 */
	private long unsetTriggerState(String triggerHashKey) throws JobPersistenceException {
		Long removed = -1L;
      try (Jedis jedis = pool.getResource()) {
			Pipeline p = jedis.pipelined();
			p.zrem(RedisTriggerState.WAITING.getKey(), triggerHashKey);
			p.zrem(RedisTriggerState.PAUSED.getKey(), triggerHashKey);
			p.zrem(RedisTriggerState.BLOCKED.getKey(), triggerHashKey);
			p.zrem(RedisTriggerState.PAUSED_BLOCKED.getKey(), triggerHashKey);
			p.zrem(RedisTriggerState.ACQUIRED.getKey(), triggerHashKey);
			p.zrem(RedisTriggerState.COMPLETED.getKey(), triggerHashKey);
			p.zrem(RedisTriggerState.ERROR.getKey(), triggerHashKey);
			List<Object> results = p.syncAndReturnAll();
			
			for (Object result : results) {
				removed = (Long)result;
				if (removed == 1) {
					jedis.hset(triggerHashKey, LOCKED_BY, "");
					jedis.hset(triggerHashKey, LOCK_TIME, "");
					break;
				}
			}
		} catch (Exception ex) {
			removed = -1L;
			log.error("could not unset state for trigger: " + triggerHashKey);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
      }
		return removed;
	}

	@Override
	public boolean removeTriggers(List<TriggerKey> triggerKeys)
			throws JobPersistenceException {
      boolean removed = triggerKeys.size() > 0;
		for (TriggerKey triggerKey : triggerKeys)
			removed = removeTrigger(triggerKey) && removed;
				
		return removed;
	}

	@Override
	public boolean replaceTrigger(TriggerKey triggerKey,
			OperableTrigger newTrigger) throws JobPersistenceException {
		boolean replaced = false;
		String triggerHashKey = createTriggerHashKey(triggerKey.getGroup(), triggerKey.getName());
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			
			String jobHashKey =jedis.hget(triggerHashKey, JOB_HASH_KEY); 
			if (jobHashKey == null || jobHashKey.isEmpty())
				throw new JobPersistenceException("trigger does not exist or no job is associated with the trigger");			
			
			if (!jobHashKey.equals(createJobHashKey(newTrigger.getJobKey().getGroup(), newTrigger.getJobKey().getName())))
				throw new JobPersistenceException("the new trigger is associated with a diffrent job than the existing trigger");
			
			removeTrigger(triggerKey, jedis);
			storeTrigger(newTrigger, false, jedis);
			replaced = true;
		} catch (Exception ex) {
			log.error("could not replace trigger: " + triggerHashKey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
		return replaced;
	}

	@Override
	public OperableTrigger retrieveTrigger(TriggerKey triggerKey) throws JobPersistenceException {
		OperableTrigger trigger = null;
		String triggerHashKey = createTriggerHashKey(triggerKey.getGroup(), triggerKey.getName());
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			trigger = retrieveTrigger(triggerKey, jedis);			
		} catch (Exception ex) {
			log.error("could not retrieve trigger: " + triggerHashKey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
		return trigger;
	}

	/**
	 * Retrieves trigger from redis.
	 *
	 * @param triggerKey the trigger key
	 * @param jedis thread-safe redis connection
	 * @return the operable trigger
	 * @throws JobPersistenceException
	 */
	private OperableTrigger retrieveTrigger(TriggerKey triggerKey, Jedis jedis) throws JobPersistenceException {
		String triggerHashKey = createTriggerHashKey(triggerKey.getGroup(), triggerKey.getName());
		Map<String, String> trigger = jedis.hgetAll(triggerHashKey);
		if (!jedis.exists(triggerHashKey)) {
			log.debug("trigger does not exist for key: " + triggerHashKey);
			return null;
		}			
		
      if (!jedis.exists(trigger.get(JOB_HASH_KEY))) {
         log.warn("job: " + trigger.get(JOB_HASH_KEY) + " does not exist for trigger: " + triggerHashKey);
         return null;
      }
		return toOperableTrigger(triggerKey, trigger);
	}

	private OperableTrigger toOperableTrigger(TriggerKey triggerKey, Map<String, String> trigger) {
		if (TRIGGER_TYPE_SIMPLE.equals(trigger.get(TRIGGER_TYPE))) {
			SimpleTriggerImpl simpleTrigger = new SimpleTriggerImpl();
			setOperableTriggerFields(triggerKey, trigger, simpleTrigger);
			if (trigger.get(REPEAT_COUNT) != null && !trigger.get(REPEAT_COUNT).isEmpty())
				simpleTrigger.setRepeatCount(Integer.parseInt(trigger.get(REPEAT_COUNT)));
			if (trigger.get(REPEAT_INTERVAL) != null && !trigger.get(REPEAT_INTERVAL).isEmpty())
				simpleTrigger.setRepeatInterval(Long.parseLong(trigger.get(REPEAT_INTERVAL)));
			if (trigger.get(TIMES_TRIGGERED) != null && !trigger.get(TIMES_TRIGGERED).isEmpty())
				simpleTrigger.setTimesTriggered(Integer.parseInt(trigger.get(TIMES_TRIGGERED)));
			
			return simpleTrigger;
		} else if (TRIGGER_TYPE_CRON.equals(trigger.get(TRIGGER_TYPE))) {
			CronTriggerImpl cronTrigger = new CronTriggerImpl();
			setOperableTriggerFields(triggerKey, trigger, cronTrigger);
			if (trigger.get(TIME_ZONE_ID) != null && !trigger.get(TIME_ZONE_ID).isEmpty())
				cronTrigger.getTimeZone().setID(trigger.get(TIME_ZONE_ID).isEmpty() ? null : trigger.get(TIME_ZONE_ID));
			try {
				if (trigger.get(CRON_EXPRESSION) != null && !trigger.get(CRON_EXPRESSION).isEmpty())
					cronTrigger.setCronExpression(trigger.get(CRON_EXPRESSION).isEmpty() ? null : trigger.get(CRON_EXPRESSION));
			} catch (ParseException ex) {
				log.warn("could not parse cron_expression: " + trigger.get(CRON_EXPRESSION) + " for trigger: " + createTriggerHashKey(triggerKey.getGroup(), triggerKey.getName()));
			}
			
			return cronTrigger;					
		} else { // other trigger types are not supported
			 throw new UnsupportedOperationException();
		}
	}

	private void setOperableTriggerFields(TriggerKey triggerKey, Map<String, String> trigger, OperableTrigger operableTrigger) {
		operableTrigger.setKey(triggerKey);
		operableTrigger.setJobKey(new JobKey(trigger.get(JOB_HASH_KEY).split(":")[2], trigger.get(JOB_HASH_KEY).split(":")[1]));
		operableTrigger.setDescription(trigger.get(DESCRIPTION).isEmpty() ? null : trigger.get(DESCRIPTION));
		operableTrigger.setFireInstanceId(trigger.get(FIRE_INSTANCE_ID).isEmpty() ? null : trigger.get(FIRE_INSTANCE_ID));
		operableTrigger.setCalendarName(trigger.get(CALENDAR_NAME).isEmpty() ? null : trigger.get(CALENDAR_NAME));
		operableTrigger.setPriority(Integer.parseInt(trigger.get(PRIORITY)));
		operableTrigger.setMisfireInstruction(Integer.parseInt(trigger.get(MISFIRE_INSTRUCTION)));
		operableTrigger.setStartTime(trigger.get(START_TIME).isEmpty() ? null : new Date(Long.parseLong(trigger.get(START_TIME))));
		operableTrigger.setEndTime(trigger.get(END_TIME).isEmpty() ? null : new Date(Long.parseLong(trigger.get(END_TIME))));
		operableTrigger.setNextFireTime(trigger.get(NEXT_FIRE_TIME).isEmpty() ? null : new Date(Long.parseLong(trigger.get(NEXT_FIRE_TIME))));
		operableTrigger.setPreviousFireTime(trigger.get(PREV_FIRE_TIME).isEmpty() ? null : new Date(Long.parseLong(trigger.get(PREV_FIRE_TIME))));		
	}

	@Override
	public boolean checkExists(JobKey jobKey) throws JobPersistenceException {
		String jobHashKey = createJobHashKey(jobKey.getGroup(), jobKey.getName());
      try (Jedis jedis = pool.getResource()) {
			return jedis.exists(jobHashKey);
		} catch (Exception ex) {
			log.error("could not check if job: " + jobHashKey + " exists", ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
      }
	}

	@Override
	public boolean checkExists(TriggerKey triggerKey)
			throws JobPersistenceException {
		String triggerHashKey = createTriggerHashKey(triggerKey.getGroup(), triggerKey.getName());
      try (Jedis jedis = pool.getResource()) {
			return jedis.exists(triggerHashKey);
		} catch (Exception ex) {
			log.error("could not check if trigger: " + triggerHashKey + " exists", ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
      }
	}

	@Override
	public void clearAllSchedulingData() throws JobPersistenceException {
		
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			
			// removing all jobs
			Set<String> jobs = jedis.smembers(JOBS_SET);
			for (String job : jobs)
				removeJob(new JobKey(job.split(":")[2], job.split(":")[1]), jedis);
						
			// removing all triggers
			Set<String> triggers = jedis.smembers(TRIGGERS_SET);
			for (String trigger : triggers)
				removeTrigger(new TriggerKey(trigger.split(":")[2], trigger.split(":")[1]), jedis);
			
			// removing all calendars
			Set<String> calendars = jedis.smembers(CALENDARS_SET);
			for (String calendar : calendars)
				removeCalendar(calendar.split(":")[1], jedis);
			
			log.debug("all scheduling data cleared!");
		} catch (Exception ex) {
			log.error("could not remove scheduling data", ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
	}

	@Override
	public void storeCalendar(String name, Calendar calendar,
			boolean replaceExisting, boolean updateTriggers)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		
		String calendarHashKey = createCalendarHashKey(name);
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			storeCalendar(name, calendar, replaceExisting, updateTriggers, jedis);
		} catch (ObjectAlreadyExistsException ex) {
			log.warn(calendarHashKey + " already exists");
			throw ex;
		} catch (Exception ex) {
			log.error("could not store calendar: " + calendarHashKey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
	}

	/**
	 * Store calendar in redis.
	 *
	 * @param name the name
	 * @param calendar the calendar
	 * @param replaceExisting the replace existing
	 * @param updateTriggers the update triggers
	 * @param jedis thread-safe redis connection
	 * @throws ObjectAlreadyExistsException the object already exists exception
	 * @throws JobPersistenceException
	 */
	private void storeCalendar(String name, Calendar calendar,
			boolean replaceExisting, boolean updateTriggers, Jedis jedis)
			throws ObjectAlreadyExistsException, JobPersistenceException {
		
		String calendarHashKey = createCalendarHashKey(name);
		if (jedis.exists(calendarHashKey) && !replaceExisting)
			throw new ObjectAlreadyExistsException(calendarHashKey + " already exists");
		
		Gson gson = new Gson();			
      Map<String, String> calendarHash = new HashMap<>();
		calendarHash.put(CALENDAR_CLASS, calendar.getClass().getName());
		calendarHash.put(CALENDAR_SERIALIZED, gson.toJson(calendar));
		
		jedis.hmset(calendarHashKey, calendarHash);
		jedis.sadd(CALENDARS_SET, calendarHashKey);
		
		if (updateTriggers) {
			String calendarTriggersSetkey = createCalendarTriggersSetKey(name);
			Set<String> triggerHasjKeys = jedis.smembers(calendarTriggersSetkey);
			for (String triggerHashKey : triggerHasjKeys) {				
				OperableTrigger trigger = retrieveTrigger(new TriggerKey(triggerHashKey.split(":")[2], triggerHashKey.split(":")[1]), jedis);
				long removed = jedis.zrem(RedisTriggerState.WAITING.getKey(), triggerHashKey);
				trigger.updateWithNewCalendar(calendar, getMisfireThreshold());
				if (removed == 1)
					setTriggerState(RedisTriggerState.WAITING, (double)trigger.getNextFireTime().getTime(), triggerHashKey);				
			}
		}
	}

	@Override
	public boolean removeCalendar(String calName)
			throws JobPersistenceException {
		boolean removed = false;
		String calendarHashKey = createCalendarHashKey(calName);
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			if (jedis.exists(calendarHashKey)) {
				removeCalendar(calName, jedis);
				removed = true;
			}			
		} catch (Exception ex) {
			log.error("could not remove calendar: " + calendarHashKey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
		return removed;
	}

	/**
	 * Removes the calendar from redis.
	 *
	 * @param calName the calendar name
	 * @param jedis thread-safe redis connection
	 * @throws JobPersistenceException
	 */
	private void removeCalendar(String calName, Jedis jedis)
			throws JobPersistenceException {
		String calendarHashKey = createCalendarHashKey(calName);
		
		// checking if there are triggers pointing to this calendar
		String calendarTriggersSetkey = createCalendarTriggersSetKey(calName);
		if (jedis.scard(calendarTriggersSetkey) > 0)
			throw new JobPersistenceException("there are triggers pointing to: " + calendarHashKey + ", calendar can't be removed");			
				
		// removing the calendar
		jedis.del(calendarHashKey);
		jedis.srem(CALENDARS_SET, calendarHashKey);
	}
	
	@Override
	public Calendar retrieveCalendar(String calName)
			throws JobPersistenceException {
		Calendar calendar = null;
		String calendarHashKey = createCalendarHashKey(calName);
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			calendar = retrieveCalendar(calName, jedis);					
		} catch (Exception ex) {
			log.error("could not retrieve calendar: " + calendarHashKey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
		return calendar;
	}

	/**
	 * Retrieve calendar.
	 *
	 * @param calname the calendar name
	 * @param jedis thread-safe redis connection
	 * @return the calendar
	 * @throws JobPersistenceException 
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private Calendar retrieveCalendar(String calName, Jedis jedis)
			throws JobPersistenceException {
		Calendar calendar = null; 
		try {			
			String calendarHashKey = createCalendarHashKey(calName);
			if (!jedis.exists(calendarHashKey))
				throw new JobPersistenceException(calendarHashKey + " does not exist");
			
			Gson gson = new Gson();
			Class calClass = Class.forName(jedis.hget(calendarHashKey, CALENDAR_CLASS));
         calendar = (Calendar) gson.fromJson(jedis.hget(calendarHashKey, CALENDAR_SERIALIZED), calClass);
		} catch (ClassNotFoundException ex) {
			log.warn("class not found for calendar: " + calName, ex);
		}
		return calendar;
	}

	@Override
	public int getNumberOfJobs() throws JobPersistenceException {
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			return jedis.scard(JOBS_SET).intValue();
		} catch (Exception ex) {
			log.error("could not get number of jobs", ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
	}

	@Override
	public int getNumberOfTriggers() throws JobPersistenceException {
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			return jedis.scard(TRIGGERS_SET).intValue();
		} catch (Exception ex) {
			log.error("could not get number of triggers", ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
	}

	@Override
	public int getNumberOfCalendars() throws JobPersistenceException {
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			return jedis.scard(CALENDARS_SET).intValue();
		} catch (Exception ex) {
			log.error("could not get number of triggers", ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
	}

	@Override
	public Set<JobKey> getJobKeys(GroupMatcher<JobKey> matcher)
			throws JobPersistenceException {
		if (matcher.getCompareWithOperator() != StringOperatorName.EQUALS)
			throw new UnsupportedOperationException();
		
      Set<JobKey> jobKeys = new HashSet<>();
		String jobGroupSetKey = createJobGroupSetKey(matcher.getCompareToValue());
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			if(jedis.sismember(JOB_GROUPS_SET, jobGroupSetKey)) {
				Set<String> jobs = jedis.smembers(jobGroupSetKey);
				for(String job : jobs)
					jobKeys.add(new JobKey(job.split(":")[2], job.split(":")[1]));
			}							
		} catch (Exception ex) {
			log.error("could not get job keys for job group: " + jobGroupSetKey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}			
		
		return jobKeys;				
	}

	@Override
	public Set<TriggerKey> getTriggerKeys(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		if (matcher.getCompareWithOperator() != StringOperatorName.EQUALS)
			throw new UnsupportedOperationException();
		
      Set<TriggerKey> triggerKeys = new HashSet<>();
		String triggerGroupSetKey = createTriggerGroupSetKey(matcher.getCompareToValue());
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			if(jedis.sismember(TRIGGER_GROUPS_SET, triggerGroupSetKey)) {
				Set<String> triggers = jedis.smembers(triggerGroupSetKey);
				for(String trigger : triggers)
					triggerKeys.add(new TriggerKey(trigger.split(":")[2], trigger.split(":")[1]));
			}						
		} catch (Exception ex) {
			log.error("could not get trigger keys for trigger group: " + triggerGroupSetKey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
		
		return triggerKeys;
	}

	@Override
	public List<String> getJobGroupNames() throws JobPersistenceException {
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
         return new ArrayList<>(jedis.smembers(JOB_GROUPS_SET));
		} catch(Exception ex) {
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}		
	}

	@Override
	public List<String> getTriggerGroupNames() throws JobPersistenceException {
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
         return new ArrayList<>(jedis.smembers(TRIGGER_GROUPS_SET));
		} catch(Exception ex) {
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
	}

	@Override
	public List<String> getCalendarNames() throws JobPersistenceException {
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
         return new ArrayList<>(jedis.smembers(CALENDARS_SET));
		} catch(Exception ex) {
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
	}

	@Override
	public List<OperableTrigger> getTriggersForJob(JobKey jobKey)
			throws JobPersistenceException {
		String jobTriggerSetkey = createJobTriggersSetKey(jobKey.getGroup(), jobKey.getName());
      List<OperableTrigger> triggers = new ArrayList<>();
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			triggers = getTriggersForJob(jobTriggerSetkey, jedis);
		} catch (Exception ex) {
			log.error("could not get triggers for job_triggers: " + jobTriggerSetkey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
		return triggers;	
	}

	/**
	 * Gets triggers for a job.
	 *
	 * @param jobTriggerHashkey the job_trigger hash key
	 * @param jedis thread-safe redis connection
	 * @return the triggers for the job
	 * @throws JobPersistenceException
	 */
	private List<OperableTrigger> getTriggersForJob(String jobTriggerHashkey, Jedis jedis) throws JobPersistenceException {
      List<OperableTrigger> triggers = new ArrayList<>();
		Set<String> triggerHashkeys = jedis.smembers(jobTriggerHashkey);
		for (String triggerHashkey : triggerHashkeys)
			triggers.add(retrieveTrigger(new TriggerKey(triggerHashkey.split(":")[2], triggerHashkey.split(":")[1]), jedis));
		
		return triggers;
	}

	@Override
	public TriggerState getTriggerState(TriggerKey triggerKey)
			throws JobPersistenceException {
		String triggerHashKey = createTriggerHashKey(triggerKey.getGroup(), triggerKey.getName());
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			
			if (jedis.zscore(RedisTriggerState.PAUSED.getKey(), triggerHashKey) != null || jedis.zscore(RedisTriggerState.PAUSED_BLOCKED.getKey(), triggerHashKey)!= null)
				return TriggerState.PAUSED;
			else if (jedis.zscore(RedisTriggerState.BLOCKED.getKey(), triggerHashKey) != null)
				return TriggerState.BLOCKED;
			else if (jedis.zscore(RedisTriggerState.WAITING.getKey(), triggerHashKey) != null || jedis.zscore(RedisTriggerState.ACQUIRED.getKey(), triggerHashKey) != null)
				return TriggerState.NORMAL;
			else if (jedis.zscore(RedisTriggerState.COMPLETED.getKey(), triggerHashKey) != null)
				return TriggerState.COMPLETE;
			else if (jedis.zscore(RedisTriggerState.ERROR.getKey(), triggerHashKey) != null)
				return TriggerState.ERROR;
			else
				return TriggerState.NONE;
		} catch (Exception ex) {
			log.error("could not get trigger state: " + triggerHashKey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
	}	

	@Override
	public void pauseTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		String triggerHashKey = createTriggerHashKey(triggerKey.getGroup(), triggerKey.getName());
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			pauseTrigger(triggerKey, jedis);				
		} catch (Exception ex) {
			log.error("could not pause trigger: " + triggerHashKey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
	}

	/**
	 * Pause trigger in redis.
	 *
	 * @param triggerKey the trigger key
	 * @param jedis thread-safe redis connection
	 * @throws JobPersistenceException
	 */
	private void pauseTrigger(TriggerKey triggerKey, Jedis jedis) throws JobPersistenceException {
		String triggerHashKey = createTriggerHashKey(triggerKey.getGroup(), triggerKey.getName());
		if (!jedis.exists(triggerHashKey))
			throw new JobPersistenceException("trigger: " + triggerHashKey + " does not exist");
				
		if (jedis.zscore(RedisTriggerState.COMPLETED.getKey(), triggerHashKey) != null)
			return;
		
		Long nextFireTime = jedis.hget(triggerHashKey, NEXT_FIRE_TIME).isEmpty() ? -1 : Long.parseLong(jedis.hget(triggerHashKey, NEXT_FIRE_TIME));
		if (jedis.zscore(RedisTriggerState.BLOCKED.getKey(), triggerHashKey) != null)
			setTriggerState(RedisTriggerState.PAUSED_BLOCKED, (double)nextFireTime, triggerHashKey);
		else
			setTriggerState(RedisTriggerState.PAUSED, (double)nextFireTime, triggerHashKey);		
	}

	@Override
	public Collection<String> pauseTriggers(GroupMatcher<TriggerKey> matcher)
			throws JobPersistenceException {
		if (matcher.getCompareWithOperator() != StringOperatorName.EQUALS)
			throw new UnsupportedOperationException();
		
      Set<String> pausedTriggerdGroups = new HashSet<>();
		String triggerGroupSetKey = createTriggerGroupSetKey(matcher.getCompareToValue());
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			if (pauseTriggers(triggerGroupSetKey, jedis))				
				pausedTriggerdGroups.add(triggerGroupSetKey);	// as we currently support only EQUALS matcher's operator, the paused group set will consist of one paused group only.
		} catch (Exception ex) {
			log.error("could not pause triggers group: " + triggerGroupSetKey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
		return pausedTriggerdGroups;
	}

	/**
	 * Pause triggers.
	 *
	 * @param triggerGroupHashKey the trigger group hash key
	 * @param jedis thread-safe redis connection
	 * @throws JobPersistenceException
	 */
	private boolean pauseTriggers(String triggerGroupHashKey, Jedis jedis) throws JobPersistenceException {
		if (jedis.sadd(PAUSED_TRIGGER_GROUPS_SET, triggerGroupHashKey) > 0) { 
			Set<String> triggers = jedis.smembers(triggerGroupHashKey);
			for(String trigger : triggers)
				pauseTrigger(new TriggerKey(trigger.split(":")[2], trigger.split(":")[1]), jedis);
			return true;
		}
		return false;
	}

	@Override
	public void pauseJob(JobKey jobKey) throws JobPersistenceException {
		String jobHashKey = createJobHashKey(jobKey.getGroup(),jobKey.getName());
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			pauseJob(jobHashKey, jedis);	
		} catch (Exception ex) {
			log.error("could not pause job: " + jobHashKey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
	}

	/**
	 * Pause job.
	 *
	 * @param jobKey the job key
	 * @param jedis thread-safe redis connection
	 * @throws JobPersistenceException
	 */
	private void pauseJob(String jobHashKey, Jedis jedis) throws JobPersistenceException {
		if (!jedis.sismember(JOBS_SET, jobHashKey))
			throw new JobPersistenceException("job: " + jobHashKey + " des not exist");
		
		String jobTriggerSetkey = createJobTriggersSetKey(jobHashKey.split(":")[1], jobHashKey.split(":")[2]);
		List<OperableTrigger> triggers = getTriggersForJob(jobTriggerSetkey, jedis);
		for (OperableTrigger trigger : triggers)
			pauseTrigger(trigger.getKey(), jedis);
	}

	@Override
	public Collection<String> pauseJobs(GroupMatcher<JobKey> groupMatcher)
			throws JobPersistenceException {
		if (groupMatcher.getCompareWithOperator() != StringOperatorName.EQUALS)
			throw new UnsupportedOperationException();
		
      Set<String> pausedJobGroups = new HashSet<>();
		String jobGroupSetKey = createJobGroupSetKey(groupMatcher.getCompareToValue());
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
						
			if (jedis.sadd(PAUSED_JOB_GROUPS_SET, jobGroupSetKey) > 0) {
				pausedJobGroups.add(jobGroupSetKey);			
				Set<String> jobs = jedis.smembers(jobGroupSetKey);
				for (String job : jobs)
					pauseJob(job, jedis);
			}			
		} catch (Exception ex) {
			log.error("could not pause job group: " + jobGroupSetKey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}		
		return pausedJobGroups;
	}

	@Override
	public void resumeTrigger(TriggerKey triggerKey)
			throws JobPersistenceException {
		String triggerHashKey = createTriggerHashKey(triggerKey.getGroup(), triggerKey.getName());
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			
         OperableTrigger trigger = retrieveTrigger(new TriggerKey(triggerHashKey.split(":")[2], triggerHashKey.split(":")[1]), jedis);
			resumeTrigger(trigger, jedis);
			if (trigger != null)
				applyMisfire(trigger, jedis);
		} catch (Exception ex) {
			log.error("could not resume trigger: " + triggerHashKey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
	}

	/**
	 * Resume a trigger in redis.
	 *
	 * @param trigger the trigger
	 * @param jedis thread-safe redis connection
	 * @throws JobPersistenceException
	 */
	private void resumeTrigger(OperableTrigger trigger, Jedis jedis) throws JobPersistenceException {
		String triggerHashKey = createTriggerHashKey(trigger.getKey().getGroup(), trigger.getKey().getName());
		if (!jedis.sismember(TRIGGERS_SET, triggerHashKey))
			throw new JobPersistenceException("trigger: " + trigger + " does not exist");
		
		if (jedis.zscore(RedisTriggerState.PAUSED.getKey(), triggerHashKey) == null && jedis.zscore(RedisTriggerState.PAUSED_BLOCKED.getKey(), triggerHashKey) == null)
			throw new JobPersistenceException("trigger: " + trigger + " is not paused");
				
		String jobHashKey = createJobHashKey(trigger.getJobKey().getGroup(), trigger.getJobKey().getName());
		Date nextFireTime = trigger.getNextFireTime();
		if (nextFireTime != null) {
			if (jedis.sismember(BLOCKED_JOBS_SET, jobHashKey))
				setTriggerState(RedisTriggerState.BLOCKED, (double)nextFireTime.getTime(), triggerHashKey);
			else
				setTriggerState(RedisTriggerState.WAITING, (double)nextFireTime.getTime(), triggerHashKey);
		}
	}

	@Override
	public Collection<String> resumeTriggers(GroupMatcher<TriggerKey> matcher) 
			throws JobPersistenceException {
		Set<String> resumedTriggerdGroups;
		String triggerGroupSetKey = createTriggerGroupSetKey(matcher.getCompareToValue());
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			resumedTriggerdGroups = resumeTriggers(triggerGroupSetKey, jedis);				
		} catch (Exception ex) {
			log.error("could not pause triggers group: " + triggerGroupSetKey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
		return resumedTriggerdGroups;
	}

	/**
	 * Resume triggers in redis.
	 *
	 * @param triggerGroupHashKey the trigger group hash key
	 * @param jedis thread-safe redis connection
	 * @return resumed trigger groups set 
	 * @throws JobPersistenceException the job persistence exception
	 */
	private Set<String> resumeTriggers(String triggerGroupHashKey, Jedis jedis) throws JobPersistenceException {
      Set<String> resumedTriggerdGroups = new HashSet<>();
		jedis.srem(PAUSED_TRIGGER_GROUPS_SET, triggerGroupHashKey);
		Set<String> triggerHashKeys = jedis.smembers(triggerGroupHashKey);
		for(String triggerHashKey : triggerHashKeys) {
			OperableTrigger trigger = retrieveTrigger(new TriggerKey(triggerHashKey.split(":")[2], triggerHashKey.split(":")[1]), jedis);
			resumeTrigger(trigger, jedis);
			resumedTriggerdGroups.add(trigger.getKey().getGroup());	// as we currently support only EQUALS matcher's operator, the paused group set will consist of one paused group only.			
		}
		
		return resumedTriggerdGroups;
	}

	@Override
	public Set<String> getPausedTriggerGroups() throws JobPersistenceException {
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			return jedis.smembers(PAUSED_TRIGGER_GROUPS_SET);
		} catch(Exception ex) {
			log.error("could not get paused trigger groups", ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
	}

	@Override
	public void resumeJob(JobKey jobKey) throws JobPersistenceException {
		String jobHashKey = createJobHashKey(jobKey.getGroup(),jobKey.getName());
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			resumeJob(jobKey, jedis);	
		} catch (Exception ex) {
			log.error("could not resume job: " + jobHashKey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
	}

	/**
	 * Resume a job in redis.
	 *
	 * @param jobKey the job key
	 * @param jedis thread-safe redis connection
	 * @throws JobPersistenceException
	 */
	private void resumeJob(JobKey jobKey, Jedis jedis) throws JobPersistenceException {
		String jobHashKey = createJobHashKey(jobKey.getGroup(),jobKey.getName());
		if (!jedis.sismember(JOBS_SET, jobHashKey))
			throw new JobPersistenceException("job: " + jobHashKey + " des not exist");
			
		List<OperableTrigger> triggers = getTriggersForJob(jobKey);
		for (OperableTrigger trigger : triggers)
			resumeTrigger(trigger, jedis);
	}

	@Override
	public Collection<String> resumeJobs(GroupMatcher<JobKey> matcher)
			throws JobPersistenceException {
		if (matcher.getCompareWithOperator() != StringOperatorName.EQUALS)
			throw new UnsupportedOperationException();
		
      Set<String> resumedJobGroups = new HashSet<>();
		String jobGroupSetKey = createJobGroupSetKey(matcher.getCompareToValue());
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			
			if(!jedis.sismember(JOB_GROUPS_SET, jobGroupSetKey))
				throw new JobPersistenceException("job group: " + jobGroupSetKey + " does not exist");
			
			if (jedis.srem(PAUSED_JOB_GROUPS_SET, jobGroupSetKey) > 0)
				resumedJobGroups.add(jobGroupSetKey);
			
			Set<String> jobs = jedis.smembers(jobGroupSetKey);
			for (String job : jobs)
				resumeJob(new JobKey(job.split(":")[2], job.split(":")[1]), jedis);			
		} catch (Exception ex) {
			log.error("could not resume job group: " + jobGroupSetKey, ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}		
		return resumedJobGroups;
	}

	@Override
	public void pauseAll() throws JobPersistenceException {
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
         List<String> triggerGroups = new ArrayList<>(jedis.smembers(TRIGGER_GROUPS_SET));
			for (String triggerGroup : triggerGroups)
				pauseTriggers(triggerGroup, jedis);
		} catch(Exception ex) {
			log.error("could not pause all triggers", ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
	}

	@Override
	public void resumeAll() throws JobPersistenceException {
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
         List<String> triggerGroups = new ArrayList<>(jedis.smembers(TRIGGER_GROUPS_SET));
			for (String triggerGroup : triggerGroups)
				resumeTriggers(triggerGroup, jedis);
		} catch(Exception ex) {
			log.error("could not resume all triggers", ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
	}

	@Override
	public List<OperableTrigger> acquireNextTriggers(long noLaterThan,
			int maxCount, long timeWindow) throws JobPersistenceException {

		// Run job store background functionality periodically in acquireNextTriggers since we know this is called periodically by the scheduler  
		keepAlive();
		releaseTriggersCron();
		
      List<OperableTrigger> acquiredTriggers = new ArrayList<>();
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			boolean retry = true;
			while (retry) {
				retry = false;
            Set<String> acquiredJobHashKeysForNoConcurrentExec = new HashSet<>();
				Set<Tuple> triggerTuples = jedis.zrangeByScoreWithScores(RedisTriggerState.WAITING.getKey(), 0d, (double)(noLaterThan + timeWindow), 0, maxCount);
				for (Tuple triggerTuple : triggerTuples) {
					OperableTrigger trigger = retrieveTrigger(new TriggerKey(triggerTuple.getElement().split(":")[2], triggerTuple.getElement().split(":")[1]), jedis);
									
					// handling misfires
					if (applyMisfire(trigger, jedis)) {
						log.debug("misfired trigger: " + triggerTuple.getElement());
						retry = true;
						break;
					}				
						
					// if the trigger has no next fire time its WAITING state should be unset 
					if (trigger.getNextFireTime() == null) {
						unsetTriggerState(triggerTuple.getElement());
						continue;
					}
					
					// if the trigger's job is annotated as @DisallowConcurrentExecution, check if one of its triggers were already acquired
					String jobHashKey = createJobHashKey(trigger.getJobKey().getGroup(), trigger.getJobKey().getName());
					if (isJobConcurrentExectionDisallowed(jedis.hget(jobHashKey, JOB_CLASS))) {
						if (acquiredJobHashKeysForNoConcurrentExec.contains(jobHashKey)) // a trigger is already acquired for this job, continue to the next trigger
							continue;
						else
							acquiredJobHashKeysForNoConcurrentExec.add(jobHashKey);
					}
					
					// acquiring trigger
					jedis.hset(triggerTuple.getElement(), LOCKED_BY, instanceId);
					jedis.hset(triggerTuple.getElement(), LOCK_TIME, Long.toString(System.currentTimeMillis()));
					setTriggerState(RedisTriggerState.ACQUIRED, triggerTuple.getScore(), triggerTuple.getElement());
					acquiredTriggers.add(trigger);
					log.debug("trigger: " + triggerTuple.getElement() + " acquired");												
				}
			}
		} catch(Exception ex) {
			log.error("could not acquire next triggers", ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
		return acquiredTriggers;
	}	
	
	/**
	 * Releasing triggers of non-alive schedulers.
	 * A `releaseTriggersInterval` should be configured for this mechanism to run.
	 */
	private void releaseTriggersCron() {
		if (this.releaseTriggersInterval > 0) {
			long lastTriggersReleseTime = getLastTriggersReleaseTime();	
			if ((System.currentTimeMillis() - lastTriggersReleseTime) > (this.releaseTriggersInterval)) {
				releaseOrphanedTriggers(RedisTriggerState.ACQUIRED, RedisTriggerState.WAITING);
				releaseOrphanedTriggers(RedisTriggerState.BLOCKED, RedisTriggerState.WAITING);
				releaseOrphanedTriggers(RedisTriggerState.PAUSED_BLOCKED, RedisTriggerState.PAUSED);							
				
				setLastTriggersReleaseTime(Long.toString(System.currentTimeMillis()));
			}			
		}			
	}
		
	private long getLastTriggersReleaseTime() {
		long lastTriggersReleaseTime = 0;
      try (Jedis jedis = pool.getResource()) {
			if (jedis.exists(LAST_TRIGGERS_RELEASE_TIME))
				lastTriggersReleaseTime = Long.parseLong(jedis.get(LAST_TRIGGERS_RELEASE_TIME));
		} catch (Exception ex) {
			log.error("could not get last trigger release time");
      }
		return lastTriggersReleaseTime;
	}
	
	private void setLastTriggersReleaseTime(String time) {
      try (Jedis jedis = pool.getResource()) {
			jedis.set(LAST_TRIGGERS_RELEASE_TIME, time);
		} catch (Exception ex) {
			log.error("could not get last triggerrelease time");
      }
	}	
	

	/**
	 * Release triggers from a given current state to a given new state, 
	 * if its locking scheduler instance id is non-alive for over 10 minutes.
	 * 
	 *
	 * @param currentState the current orphaned trigger state
	 * @param newState the new state of the orphaned trigger
	 */
	private void releaseOrphanedTriggers(RedisTriggerState currentState, RedisTriggerState newState) {
		final int ALIVE_TIMEOUT = 10 * 60 * 1000; // 10 minutes non-alive scheduler timeout
		Map<String, Long> instanceIdLastAlive = new HashMap<>();
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			
			Set<Tuple> triggerTuples = jedis.zrangeWithScores(currentState.getKey(), 0, -1);
			for (Tuple triggerTuple : triggerTuples) {
				String lockedByInstanceId = jedis.hget(triggerTuple.getElement(), LOCKED_BY);
				if (lockedByInstanceId != null && !lockedByInstanceId.isEmpty()) {
					if (!instanceIdLastAlive.containsKey(lockedByInstanceId)) {
						long lastAliveTime = jedis.exists(lockedByInstanceId) ? Long.parseLong(jedis.get(lockedByInstanceId)) : 0;
						instanceIdLastAlive.put(lockedByInstanceId, lastAliveTime);
					}
					if ((System.currentTimeMillis() - instanceIdLastAlive.get(lockedByInstanceId)) > (ALIVE_TIMEOUT))
						setTriggerState(newState, triggerTuple.getScore(), triggerTuple.getElement());
				}					
			}						
      } catch (JobPersistenceException | NumberFormatException | InterruptedException ex) {
			log.error("could not release orphaned triggers from: " + currentState, ex);
		} finally {
         lockPool.release();
		}		
	}

	/**
	 * Release triggers locked by an instance id.
	 *
	 * @param lockedByInstanceId the locking instance id
	 * @param currentState the current locking state
	 * @param newState the new trigger's state
	 */
	private void releaseLockedTriggers(String lockedByInstanceId, RedisTriggerState currentState, RedisTriggerState newState) {
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
						
			Set<Tuple> triggerTuples = jedis.zrangeWithScores(currentState.getKey(), 0, -1);
			for (Tuple triggerTuple : triggerTuples) {
				if (lockedByInstanceId != null && lockedByInstanceId.equals(jedis.hget(triggerTuple.getElement(), LOCKED_BY))) {
					log.debug("releasing trigger: " + triggerTuple.getElement() + " from: " + currentState.getKey());
					setTriggerState(newState, triggerTuple.getScore(), triggerTuple.getElement());
				}					
			}			
		} catch (Exception ex) {
			log.error("could not release locked triggers in: " + currentState.getKey(), ex);
		} finally {
         lockPool.release();
		}						
	}

	private String readInstanceId(File instanceIdFile) {
		String previousInstanceId = null;
		InputStreamReader isr = null;
      StringBuilder buffer = new StringBuilder();
		try {			
			if (instanceIdFile.exists()) {
				FileInputStream fis = new FileInputStream(instanceIdFile);
				isr = new InputStreamReader(fis, "UTF-8");
				Reader in = new BufferedReader(isr);
		        int ch;
		        while ((ch = in.read()) > -1) {
		            buffer.append((char)ch);
		        }
		        
		        previousInstanceId = buffer.toString();
			}		
		} catch (IOException ex) {
			log.error("could not read previous scheduler instance id", ex);			
		} finally {
			try {
				if (isr != null)
					isr.close();				
			} catch (IOException ex) {
				log.warn("could not close file reader", ex);
			}			
		}
		return previousInstanceId;
	}
	
	private void writeInstanceId(File instanceIdFile) {
		Writer out = null;
		try {			
			FileOutputStream fos = new FileOutputStream(instanceIdFile, false);
			out = new OutputStreamWriter(fos, "UTF-8");
			out.write(this.instanceId);
		}  catch (IOException ex) {
			log.error("could not write new scheduler instance id", ex);			
		} finally {
			try {
				if (out != null)
					out.close();
			} catch (IOException ex) {
				log.warn("could not close file writer", ex);
			}						
		}
	}	
	
	/**
	 * Release blocked jobs, blocked by the given instance id.
	 *
	 * @param blockedByInstanceId the blocking instance id
	 */
	private void releaseBlockedJobs(String blockedByInstanceId) {
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			
			Set<String> blockedJobs = jedis.smembers(BLOCKED_JOBS_SET)	;
			for (String blockedJob : blockedJobs) {
				if (blockedByInstanceId != null && blockedByInstanceId.equals(jedis.hget(blockedJob, BLOCKED_BY))) {
					jedis.hset(blockedJob, BLOCKED_BY, "");
                	jedis.hset(blockedJob, BLOCK_TIME, "");
					jedis.srem(BLOCKED_JOBS_SET, blockedJob);
				}					
			}
		} catch (Exception ex) {
			log.error("could not release blocked jobs", ex);
		} finally {
         lockPool.release();
		}
	}

	@SuppressWarnings("unchecked")
	private boolean isJobConcurrentExectionDisallowed(String jobClassName) {
		boolean jobConcurrentExectionDisallowed = false;
		try {
			Class<Job> jobClass = (Class<Job>) loadHelper.getClassLoader().loadClass(jobClassName);
			jobConcurrentExectionDisallowed = ClassUtils.isAnnotationPresent(jobClass, DisallowConcurrentExecution.class);
		} catch (Exception ex) {
			log.error("could not determine whether class: " + jobClassName + " is JobConcurrentExectionDisallowed annotated");
		}
		return jobConcurrentExectionDisallowed;
	}
	
	@SuppressWarnings("unchecked")
	private boolean isPersistJobDataAfterExecution(String jobClassName) {
		boolean persistJobDataAfterExecution = false;
		try {
			Class<Job> jobClass = (Class<Job>) loadHelper.getClassLoader().loadClass(jobClassName);
			persistJobDataAfterExecution = ClassUtils.isAnnotationPresent(jobClass, PersistJobDataAfterExecution.class);
		} catch (Exception ex) {
			log.error("could not determine whether class: " + jobClassName + " is PersistJobDataAfterExecution annotated");
		}
		return persistJobDataAfterExecution;
	}

	@Override
   public void releaseAcquiredTrigger(OperableTrigger trigger) {
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			
			String triggerHashKey = createTriggerHashKey(trigger.getKey().getGroup(), trigger.getKey().getName());
			if (jedis.zscore(RedisTriggerState.ACQUIRED.getKey(), triggerHashKey) != null) {
				if (trigger.getNextFireTime() != null)
					setTriggerState(RedisTriggerState.WAITING, (double)trigger.getNextFireTime().getTime(), triggerHashKey);
				else
					unsetTriggerState(triggerHashKey);
			}						
		} catch(Exception ex) {
			log.error("could not release acquired triggers", ex);
         throw new RuntimeException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}		
	}

	@Override
	public List<TriggerFiredResult> triggersFired(List<OperableTrigger> triggers)
			throws JobPersistenceException {
      List<TriggerFiredResult> results = new ArrayList<>();
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			
			for (OperableTrigger trigger : triggers) {
				String triggerHashKey = createTriggerHashKey(trigger.getKey().getGroup(), trigger.getKey().getName());
				log.debug("trigger: " + triggerHashKey + " fired");
				
				if (!jedis.exists(triggerHashKey))
					continue; // the trigger does not exist
				
				if (jedis.zscore(RedisTriggerState.ACQUIRED.getKey(), triggerHashKey) == null)
					continue; // the trigger is not acquired

            Calendar cal = null;
				if (trigger.getCalendarName() != null) {
               String calendarName = trigger.getCalendarName();
					cal = retrieveCalendar(calendarName, jedis);
                    if(cal == null)
                        continue;
                }
                
                Date prevFireTime = trigger.getPreviousFireTime();
                trigger.triggered(cal);

                TriggerFiredBundle bundle = new TriggerFiredBundle(retrieveJob(trigger.getJobKey(), jedis), trigger, cal, false, new Date(), trigger.getPreviousFireTime(), prevFireTime, trigger.getNextFireTime());
                
                // handling job concurrent execution disallowed
                String jobHashKey = createJobHashKey(trigger.getJobKey().getGroup(), trigger.getJobKey().getName());
                if (isJobConcurrentExectionDisallowed(jedis.hget(jobHashKey, JOB_CLASS))) {
                	String jobTriggerSetKey = createJobTriggersSetKey(trigger.getJobKey().getGroup(), trigger.getJobKey().getName());
                	Set<String> nonConcurrentTriggerHashKeys = jedis.smembers(jobTriggerSetKey);
                	for (String nonConcurrentTriggerHashKey : nonConcurrentTriggerHashKeys) {
                		Double score = jedis.zscore(RedisTriggerState.WAITING.getKey(), nonConcurrentTriggerHashKey);
                		if (score != null) {
                			setTriggerState(RedisTriggerState.BLOCKED, score, nonConcurrentTriggerHashKey);
                		} else {
                			score = jedis.zscore(RedisTriggerState.PAUSED.getKey(), nonConcurrentTriggerHashKey);
                			if (score != null)
                				setTriggerState(RedisTriggerState.PAUSED_BLOCKED, score, nonConcurrentTriggerHashKey);
                		}                			
                	}
                	
                	jedis.hset(jobHashKey, BLOCKED_BY, instanceId);
                	jedis.hset(jobHashKey, BLOCK_TIME, Long.toString(System.currentTimeMillis()));
                	jedis.sadd(BLOCKED_JOBS_SET, jobHashKey);
                }
                
                // releasing the fired trigger
        		if (trigger.getNextFireTime() != null) {
        			jedis.hset(triggerHashKey, NEXT_FIRE_TIME, Long.toString(trigger.getNextFireTime().getTime()));
        			setTriggerState(RedisTriggerState.WAITING, (double)trigger.getNextFireTime().getTime(), triggerHashKey);
        		} else {
        			jedis.hset(triggerHashKey, NEXT_FIRE_TIME, "");
        			unsetTriggerState(triggerHashKey);
        		}
                
                results.add(new TriggerFiredResult(bundle));			
			}
      } catch (JobPersistenceException | ClassNotFoundException | InterruptedException ex) {
			log.error("could not acquire next triggers", ex);
			throw new JobPersistenceException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
		return results;		
	}

	@Override
	public void triggeredJobComplete(OperableTrigger trigger,
           JobDetail jobDetail, CompletedExecutionInstruction triggerInstCode) {
		String jobHashKey = createJobHashKey(jobDetail.getKey().getGroup(), jobDetail.getKey().getName());
		String jobDataMapHashKey = createJobDataMapHashKey(jobDetail.getKey().getGroup(), jobDetail.getKey().getName());
		String triggerHashKey = createTriggerHashKey(trigger.getKey().getGroup(), trigger.getKey().getName());
		log.debug("job: " + jobHashKey + " completed");
      try (Jedis jedis = pool.getResource()) {
         lockPool.acquire();
			
			if (jedis.exists(jobHashKey)) { // checking that the job wasn't deleted during the execution
				String jobClassName = jedis.hget(jobHashKey, JOB_CLASS);
				if (isPersistJobDataAfterExecution(jobClassName)) {
					// updating the job data map
					JobDataMap jobDataMap = jobDetail.getJobDataMap();
					
					jedis.del(jobDataMapHashKey);
					if (jobDataMap != null && !jobDataMap.isEmpty())
						jedis.hmset(jobDataMapHashKey, getStringDataMap(jobDataMap));
				}
				
				if (isJobConcurrentExectionDisallowed(jobClassName)) {
					jedis.hset(jobHashKey, BLOCKED_BY, "");
                	jedis.hset(jobHashKey, BLOCK_TIME, "");
					jedis.srem(BLOCKED_JOBS_SET, jobHashKey);
					
					String jobTriggersSetKey = createJobTriggersSetKey(trigger.getJobKey().getGroup(), trigger.getJobKey().getName());
                	Set<String> nonConcurrentTriggerHashKeys = jedis.smembers(jobTriggersSetKey);
                	for (String nonConcurrentTriggerHashkey : nonConcurrentTriggerHashKeys) {
                		Double score = jedis.zscore(RedisTriggerState.BLOCKED.getKey(), nonConcurrentTriggerHashkey);
                		if (score != null) {
                			setTriggerState(RedisTriggerState.WAITING, score, nonConcurrentTriggerHashkey);
                		} else {
                			score = jedis.zscore(RedisTriggerState.PAUSED_BLOCKED.getKey(), nonConcurrentTriggerHashkey);
                			if (score != null)
                				setTriggerState(RedisTriggerState.PAUSED, score, nonConcurrentTriggerHashkey);
                		}                			
                	}
                	
					signaler.signalSchedulingChange(0L);
				}				
			} else { // removing the job from blocked set anyway
				jedis.srem(BLOCKED_JOBS_SET, jobHashKey);
			}
			
			
			if (jedis.exists(triggerHashKey)) { // checking that the trigger wasn't deleted during the execution
				// applying the execution instruction for the completed job's trigger
				if (triggerInstCode == CompletedExecutionInstruction.DELETE_TRIGGER) {
					if (trigger.getNextFireTime() == null) {
						// double check for possible reschedule within job execution, which would cancel the need to delete...
						if (jedis.hget(triggerHashKey, NEXT_FIRE_TIME).isEmpty()) {
							removeTrigger(trigger.getKey(), jedis);
						}
					} else {
						removeTrigger(trigger.getKey(), jedis);
				        signaler.signalSchedulingChange(0L);
					}
				} else if (triggerInstCode == CompletedExecutionInstruction.SET_TRIGGER_COMPLETE) {
					setTriggerState(RedisTriggerState.COMPLETED, (double)System.currentTimeMillis(), triggerHashKey);
					signaler.signalSchedulingChange(0L);
				} else if(triggerInstCode == CompletedExecutionInstruction.SET_TRIGGER_ERROR) {
					log.debug("trigger: " + triggerHashKey + " ended with error");
					double score = trigger.getNextFireTime() != null ? (double)trigger.getNextFireTime().getTime() : 0D;
					setTriggerState(RedisTriggerState.ERROR, score, triggerHashKey);
					signaler.signalSchedulingChange(0L);
				} else if (triggerInstCode == CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_ERROR) {
					String jobTriggersSetKey = createJobTriggersSetKey(jobDetail.getKey().getGroup(), jobDetail.getKey().getName());
					Set<String> errorTriggerHashkeys = jedis.smembers(jobTriggersSetKey);
					for (String errorTriggerHashKey : errorTriggerHashkeys) {
						double score = jedis.hget(errorTriggerHashKey, NEXT_FIRE_TIME).isEmpty() ? 0D : Double.parseDouble(jedis.hget(errorTriggerHashKey, NEXT_FIRE_TIME));
						setTriggerState(RedisTriggerState.ERROR, score, errorTriggerHashKey);
					}					
					signaler.signalSchedulingChange(0L);
				} else if (triggerInstCode == CompletedExecutionInstruction.SET_ALL_JOB_TRIGGERS_COMPLETE) {
					String jobTriggerHashkey = createJobTriggersSetKey(jobDetail.getKey().getGroup(), jobDetail.getKey().getName());
					Set<String> comletedTriggerHashkeys = jedis.smembers(jobTriggerHashkey);
					for (String completedTriggerHashKey : comletedTriggerHashkeys) {
						setTriggerState(RedisTriggerState.COMPLETED, (double)System.currentTimeMillis(), completedTriggerHashKey);
					}					
					signaler.signalSchedulingChange(0L);
				}
			}			
      } catch (JobPersistenceException | NumberFormatException | InterruptedException ex) {
			log.error("could not handle triggegered job completion", ex);
         throw new RuntimeException(ex.getMessage(), ex.getCause());
		} finally {
         lockPool.release();
		}
	}

	@Override
	public void setInstanceId(String schedInstId) {
		this.instanceId = schedInstId;
	}

	@Override
	public void setInstanceName(String schedName) {
		// No-op
	}

	@Override
	public void setThreadPoolSize(int poolSize) {
		// No-op
	}
	
	protected boolean applyMisfire(OperableTrigger trigger, Jedis jedis) throws JobPersistenceException {
       long misfireTime = System.currentTimeMillis();
       if (getMisfireThreshold() > 0)
           misfireTime -= getMisfireThreshold();       

       Date triggerNextFireTime = trigger.getNextFireTime();
       if (triggerNextFireTime == null || triggerNextFireTime.getTime() > misfireTime 
               || trigger.getMisfireInstruction() == Trigger.MISFIRE_INSTRUCTION_IGNORE_MISFIRE_POLICY) { 
           return false; 
       }

       Calendar cal = null;
       if (trigger.getCalendarName() != null)
           cal = retrieveCalendar(trigger.getCalendarName(), jedis);       

       signaler.notifyTriggerListenersMisfired((OperableTrigger)trigger.clone());

       trigger.updateAfterMisfire(cal);
       
       if (triggerNextFireTime.equals(trigger.getNextFireTime()))
    	   return false;

       storeTrigger(trigger, true, jedis);
       if (trigger.getNextFireTime() == null) { // Trigger completed
    	   setTriggerState(RedisTriggerState.COMPLETED, (double)System.currentTimeMillis(), createTriggerHashKey(trigger.getKey().getGroup(), trigger.getKey().getName()));
    	   signaler.notifySchedulerListenersFinalized(trigger);
       }
       
       return true;
    }
	
	private void keepAlive() {
      try (Jedis jedis = pool.getResource()) {
			jedis.set(instanceId, Long.toString(System.currentTimeMillis()));			
		} catch(Exception ex) {
			log.error("could not keep alive");
      }
   }

	public long getMisfireThreshold() {
		 return misfireThreshold;
	 }
	 
	 public void setMisfireThreshold(long misfireThreshold) {
        if (misfireThreshold < 1) {
            throw new IllegalArgumentException("misfire threshold must be larger than 0");
        }
        
        this.misfireThreshold = misfireThreshold;
	 }
	 
	 public String getInstanceId() {
		 return this.instanceId;
	 }
	
	public void setHost(String host) {
		this.host = host;
	}
	
	public void setPort(int port) {
		this.port = port;
	}
	
	public void setPassword(String password) {
		this.password = password;
	}
	
	public void setInstanceIdFilePath(String instanceIdFilePath) {
		this.instanceIdFilePath = instanceIdFilePath;
	}
	
	public void setReleaseTriggersInterval(int releaseTriggersInterval) {
		this.releaseTriggersInterval = releaseTriggersInterval;
	}
	
	public void setLockTimeout(int lockTimeout) {
		this.lockTimeout = lockTimeout;
	}
	
	class UnlockListener extends JedisPubSub {

		@Override
		public void onMessage(String channel, String message) {
			log.debug("message recieved: " + message + " on channel: " + channel);
			if ("unlocked".equals(message) && UNLOCK_NOTIFICATIONS_CHANNEL.equals(channel))
				unsubscribe(channel);
		}

		@Override
		public void onPMessage(String pattern, String channel, String message) {
			log.debug("pmessage recieved: " + message + " on channel: " + channel + ", pattern: " + pattern);
		}

		@Override
		public void onSubscribe(String channel, int subscribedChannels) {
			log.debug("subscribing to channel: " + channel);			
		}

		@Override
		public void onUnsubscribe(String channel, int subscribedChannels) {
			log.debug("unsubscribing to channel: " + channel);			
		}

		@Override
		public void onPUnsubscribe(String pattern, int subscribedChannels) {
			log.debug("punsubscribing to channels pattern: " + pattern + ", subscribed channels: " + subscribedChannels);
		}

		@Override
		public void onPSubscribe(String pattern, int subscribedChannels) {
			log.debug("psubscribing to channels pattern: " + pattern + ", subscribed channels: " + subscribedChannels);		
		}		
	}
}