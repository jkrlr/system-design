/**
 * Represents the lifecycle of a task.
 *
 * Interview Note:
 * Keeping explicit task states allows us to support:
 * - Monitoring
 * - Cancellation
 * - Retry
 * - UI status
 */
public enum TaskStatus {
	CREATED,
	SCHEDULED,
	RUNNING,
	SUCCESS,
	FAILED,
	CANCELLED
}

/**
 * Type of scheduling.
 *
 * Extensible.
 *
 * Today:
 *  - ONE_TIME
 *
 * Tomorrow:
 *  - FIXED_DELAY
 *  - FIXED_RATE
 *  - CRON
 */
public enum ScheduleType {
	ONE_TIME,
	FIXED_DELAY,
	FIXED_RATE
}

/**
 * Optional.
 *
 * Can be used when two tasks
 * have same execution timestamp.
 */
public enum Priority {
	HIGH,
	MEDIUM,
	LOW
}

/**
 * Defines retry behavior.
 *
 * This class is intentionally
 * separated from Task because
 * retry logic may evolve independently.
 */
@Data
public class RetryPolicy {
	privte int maxRetries;
	private long retryDealyMillis;
}

/**
 * Strategy Pattern.
 *
 * Instead of using an enum with switch statements, we use Strategy Pattern.
 * Tomorrow we can plug in Cron scheduling without touching the scheduler.
 *
 * Every scheduling strategy knows
 * when the task should execute next.
 */
public interface Schedule {
	long nextExecutionTime();
}


/**
 * Executes task only once.
 */
 public class OneTimeSchedule implement Schedule {
 	private final long executeAt;

 	public OneTimeSchedule(long delayMillis) {
 		this.executeAt = System.currentTimeMillis() + delayMillis;
 	}

 	@Override
 	public long nextExecutionTime {
 		return executeAt;
 	}
 }

 /**
 * Executes task repeatedly.
 *
 * Next execution is calculated
 * after previous execution finishes.
 */
 public class FixedDelaySchedule implements Schedule {
 	private long nextExecutionTime;
 	private final long dealyMillis;

 	public FixedDelaySchedule(long delayMillis) {
 		this.delayMillis = delayMillis;
 		this.nextExecutionTime = System.currentTimeMillis() + delayMillis;
 	}

 	@Override
 	public long nextExecutionTime {
 		return nextExecutionTime;
 	}

 	/**
     * Called after successful execution.
     */
     public void scheduleNext() {
     	nextExecutionTime = System.currentTimeMillis() + delayMillis;
     }
 }

 /**
 * Represents a schedulable task.
 *
 * Design Decisions:
 *
 * 1. Task owns metadata.
 *
 * 2. Actual work is delegated
 *    to Runnable.
 *
 * 3. Scheduler manages lifecycle.
 */
 @Data
 public class Task {
 	private final String id;
 	private final Runnable runnable;
 	private final Schedule schedule;
 	private final RetryPolicy retryPolicy;
 	private Priority priority;
 	private TaskStatus status;
 	private int retryCount;

 	public Task(Runnable runnable, Schedule schedule, RetryPolicy retryPolicy, Priority priority) {
 		this.id = UUID.randomUUID().toString();
 		this.runnable = runnable;
 		this.schedule = schedule;
 		this.retryPolicy = retryPolicy;
 		this.priority = priority;
 		this.status = TaskStatus.CREATED;
 		this.retryCount = 0;
 	}

 	public void incrementRetry() {
 		++retryCount;
 	}
 }

 /**
 * Wrapper over Task.
 *
 * DelayQueue stores Delayed objects.
 *
 * This keeps scheduling concerns
 * outside Task.
 */
 @RequiredArgsConstructor
 public class DelayTask implements Delayed {
 	private final Task task;

 	public Task getTask() {
 		retrun task;
 	}

 	@Override
 	public long getDelay() {
 		long dealy = task.getSchedule().getNextExecutionTime() - System.currentTimeMillis();
 		return unit.convert(dealy, TimeUnit.MILLISECONDS);
 	}

 	@Override
 	public int compareTo(Delayed other) {
 		DelayedTask o = (DelayedTask) other;
 		long timeComparison = Long.compare(task.getSchedule().getNextExecutionTime() - o.getSchedule().getNextExecutionTime());

 		if (timeComparison != 0) {
 			return (int) timeComparison;
 		}

 		/*
         * Same execution time.
         *
         * HIGH priority executes first.
         */
         return task.getPriority().ordinal() - o.getPriority().ordinal();
 	}
 }

 /**
 * Repository Pattern.
 *
 * Responsible for storing
 * and retrieving task metadata.
 *
 * Current Implementation:
 *      In Memory
 *
 * Future:
 *      Database
 *      Redis
 */
 public interface TaskRepository {

    void save(Task task);

    Optional<Task> findById(String taskId);

    void delete(String taskId);

    List<Task> findAll();

    List<Task> findByStatus(TaskStatus status);
 }

 /**
 * Thread-safe in-memory implementation.
 *
 * Design Decision:
 *
 * ConcurrentHashMap is sufficient because:
 *
 * - Many readers
 * - Few writers
 * - O(1) lookup
 */
 public class InMemoryTaskRepository implements TaskRepository {
 	/**
     * Key:
     *      Task Id
     *
     * Value:
     *      Task
     */
    private final ConcurrentHashMap<String, Task> tasks = new ConcurrentHashMap<>();

    @Override
    public void save(Task task) {
    	tasks.put(task.getId(), task);
    }

    @Override
    public Optional<Task> findById(String taskId) {
    	Optional.ofNullable(tasks.get(taskId));
    }

    @Override
    public void delete(String taskId) {
    	tasks.remove(taskId);
    }

    @Override
    public List<Task> findAll() {
    	return new ArrayList<>(tasks.values());
    }

    /*
    * Show all running jobs
    */
    @Override
    public List<Task> findByStatus(TaskStatus status) {
    	List<Task> result = new ArrayList<>();

    	for(Task task : tasks.values()) {
    		if (task.getStatus() == status) {
    			result.add(task);
    		}
    	}

    	return result;
    }
 }


 /**
 * Responsible only for
 * executing business logic.
 *
 * Scheduler does not know
 * how task executes.
 */
 public interface TaskExecutor {
 	void execute(Task task);
 }

 /**
 * Default implementation.
 *
 * Delegates execution to
 * Runnable.
 *
 * Later we can add:
 *
 * Logging
 * Metrics
 * Timeout
 * Tracing
 */
 public class DefaultTaskExecutor implements TaskExecutor {
 	@Override
 	public void execute(Task task) {
 		task.getRunnable().run();
 	}
 }


 /**
 * Executes exactly one task.
 *
 * Design Decision:
 *
 * Worker knows execution.
 *
 * Worker does NOT know
 * scheduling.
 */
 @RequiredArgsConstructor
 public class Worker implements Runnable {
 	private final Task task;
 	private final TaskExecutor executor;
 	private final TaskRepository taskRepository;

 	@Override
 	public void run() {

 		task.setStartTime(System.currentTimeMillis());
 		task.setStatus(TaskStatus.RUNNING);
 		repository.save(task);

 		try {
 			executor.execute(task);
 			task.setStatus(TaskStatus.SUCCESS);
 		} catch (Exception ex) {
 			task.setStatus(TaskStatus.FAILED);
 			System.out.println("Task Failed : " + task.getId());
 		} finally {
 			task.setEndTime(System.currentTimeMillis());
 			repository.save(task);
 		}
 	}
 }


/**
 * Dispatcher continuously waits for
 * tasks whose delay has expired.
 *
 * Responsibilities:
 *
 * 1. Wait for next task.
 * 2. Skip cancelled tasks.
 * 3. Submit work to worker pool.
 *
 * Dispatcher does NOT execute tasks.
 */

import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;

@RequiredArgsConstructor
public class Dispatcher implements Runnable {
    private final DelayQueue<DelayTask> queue;
    private final ExecutorService executorService;
    private final TaskExecutor taskExecutor;
    private final TaskRepository taskRepository;

    private volatile boolean running = true;


    @Override
    public void run() {
        while (running) {
            try {
                /*
                 * Blocks until
                 * next task becomes ready.
                 *
                 * No busy waiting.
                 */
                 DelayedTask delayedTask = queue.take();
                 Task task = delayedTask.getTask();

                 /*
                 * Skip cancelled task.
                 */
                 if (task.getStatus == TaskStatus.CANCELLED) {
                    continue;
                 }

                 /*
                 * Submit to thread pool.
                 */
                 executorService.submit(new Worker(task, taskExecutor, repository));
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    /**
     * Gracefully stop dispatcher.
     */
     public void stop() {
        running = false;
     }
}

/**
 * Facade exposed to clients.
 *
 * Responsibilities:
 *
 * 1. Accept task
 * 2. Store metadata
 * 3. Push task into DelayQueue
 * 4. Lifecycle management
 *
 * Does NOT execute task.
 */ 
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TaskScheduler {
    /**
     * Thread-safe queue ordered by
     * execution timestamp.
     */
     private final DelayQueue<DelayTask> delayQueue = new DelayQueue<>();

     /**
     * Repository storing task metadata.
     */
     private final TaskRepository repository = new InMemoryTaskRepository();

     /**
     * Executes business logic.
     */
     private final TaskExecutor taskExecutor = new DefaultTaskExecutor();

     /**
     * Fixed worker pool.
     *
     * Threads are reused.
     */
     private final ExecutorService executorService = Executors.newFixedThreadPool(5);

     /**
     * Single dispatcher thread.
     */
     private final Dispatcher dispatcher;

     private final Thread dispatcherThread;


     public TaskScheduler() {
        dispatcher = new Dispatcher(delayQueue, executorService, taskExecutor, repository);
        
        dispatcherThread = new Thread(dispatcher, "Dispatcher");
        dispatcherThread.start();
     }

     /**
     * Schedule new task.
     */
     public String schedule(Task task) {
        task.setStatus(TaskStatus.SCHEDULED);
        repository.save(task);

        delayQueue.offer(new DelayTask(task));
        return task.getId();
     }

     /**
     * Cancel task.
     *
     * Current implementation:
     *
     * Mark CANCELLED.
     *
     * Dispatcher skips execution.
     */
     public void cancel(String taskId) {
        Optional<Task> taskOpt = repository.findById(taskId);
        if(taskOpt.isEmpty()) {
            return;
        }

        Task task = taskOpt.get();
        task.setStatus(TaskStatus.CANCELLED);
        repository.save(task);
     }

     /**
     * Returns latest task status.
     */
     public TaskStatus getStatus(String taskId) {
        Optional<Task> taskOpt = repository.findById(taskId);
        if(taskOpt.isEmpty()) {
            return null;
        }

        return taskOpt.get().getStatus();
     }

     /**
     * Gracefully stop scheduler.
     */
     public void shutdown() {
        dispatcher.stop();
        dispatcherThread.interrupt();
        executorService.shutdown();

        try {
            if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
     }
}

public class Main {
    public static void main(String[] args) throws Exception {
        TaskScheduler scheduler = new TaskScheduler();

        Task emailTask = new Task(() -> 
            {
                System.out.println(Thread.currentThread.getName() + " Sending Email");
            }, 
            new OneTimeSchedule(3000),
            new RetryPolicy(3, 2000)
            Priority.HIGH
        );

        Task reportTask = new Task(() -> 
            {
                System.out.println(Thread.currentThread.getName() + " Generating Report");
            }, 
            new OneTimeSchedule(1000),
            new RetryPolicy(3, 2000)
            Priority.MEDIUM
        );

        Task paymentTask = new Task(() -> 
            {
                System.out.println(Thread.currentThread.getName() + " Processing Payment");
            }, 
            new OneTimeSchedule(5000),
            new RetryPolicy(3, 2000)
            Priority.HIGH
        );

        scheduler.schedule(emailTask);
        scheduler.schedule(reportTask);
        scheduler.schedule(paymentTask);

        Thread.sleep(8000);
        scheduler.shutdown();
    }
}
