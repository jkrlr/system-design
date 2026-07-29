/* 
"Why this design?"
Task → Holds the business logic (Runnable) and metadata.
Schedule → Strategy Pattern. Tomorrow we can add FixedDelaySchedule, CronSchedule, etc., without changing TaskScheduler.
DelayedTask → DelayQueue only accepts objects implementing Delayed, so this wrapper keeps scheduling concerns outside Task (maintains SRP).
TaskStatus → Enables monitoring, cancellation, retries, and future APIs like getStatus()
*/

public interface Schedule {
    long nextExecutionTime();
}


public class OneTimeSchedule implements Schedule {
    private final long executionTime;

    public OneTimeSchedule(long delayMillis) {
        this.executionTime = System.currentTimeMillis() + delayMillis;
    }

    @Override
    public long nextExecutionTime() {
        return executionTime;
    }
}

public enum TaskStatus {
    CREATED,
    SCHEDULED,
    RUNNING,
    SUCCESS,
    FAILED,
    CANCELLED
}



@Getter
@Setter
public class Task {

    private final String id;
    private final Runnable runnable;
    private final Schedule schedule;
    private TaskStatus status;

    public Task(Runnable runnable, Schedule schedule) {
        this.id = UUID.randomUUID().toString();
        this.runnable = runnable;
        this.schedule = schedule;
        this.status = TaskStatus.CREATED;
    }
}

@Getter
@RequiredArgsConstructor
public class DelayedTask implements Delayed {

    private final Task task;

    @Override
    public long getDelay(TimeUnit unit) {
        long delay = task.getSchedule().nextExecutionTime() - System.currentTimeMillis();
        return unit.convert(delay, TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed other) {
        DelayedTask delayedTask = (DelayedTask) other;

        return Long.compare(task.getSchedule().nextExecutionTime(),
                delayedTask.getTask().getSchedule().nextExecutionTime()

        );
    }
}

public interface TaskExecutor {
    void execute(Task task);
}

public class DefaultTaskExecutor implements TaskExecutor {
    @Override
    public void execute(Task task) {
        task.getRunnable().run();
    }
}

@RequiredArgsConstructor
public class Dispatcher implements Runnable {

    private final DelayQueue<DelayedTask> delayQueue;
    private final ExecutorService executorService;
    private final TaskExecutor taskExecutor;
    private volatile boolean running = true;

    @Override
    public void run() {
        while (running) {
            try {
                /*
                 * Blocks until next task is ready.
                 */
                DelayedTask delayedTask = delayQueue.take();
                Task task = delayedTask.getTask();

                if (task.getStatus() == TaskStatus.CANCELLED) {
                    continue;
                }

                executorService.submit(() -> {
                    task.setStatus(TaskStatus.RUNNING);

                    try {
                        taskExecutor.execute(task);
                        task.setStatus(TaskStatus.SUCCESS);
                    } catch (Exception ex) {
                        task.setStatus(TaskStatus.FAILED);
                        System.out.println("Task failed : " + task.getId());
                    }
                });
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public void stop() {
        running = false;
    }
}

/* 
At this point, stop coding for a few seconds and say:
"So far I've separated scheduling from execution. Dispatcher is only responsible for waiting on the DelayQueue and handing work to the thread pool. 
The actual business logic is delegated to TaskExecutor, which keeps the design extensible and follows the Single Responsibility Principle."
*/


public class TaskScheduler {
    private final DelayQueue<DelayedTask> delayQueue = new DelayQueue<>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(5);
    private final TaskExecutor taskExecutor = new DefaultTaskExecutor();
    private final Dispatcher dispatcher;
    private final Thread dispatcherThread;

    public TaskScheduler() {
        dispatcher = new Dispatcher(delayQueue, executorService, taskExecutor);
        dispatcherThread = new Thread(dispatcher, "dispatcher-thread");
        dispatcherThread.start();
    }

    /**
     * Schedule a new task.
     */
    public String schedule(Task task) {
        task.setStatus(TaskStatus.SCHEDULED);
        delayQueue.offer(new DelayedTask(task));
        return task.getId();
    }

    /**
     * Simple cancellation.
     *
     * Dispatcher skips cancelled tasks.
     */
    public void cancel(Task task) {
        task.setStatus(TaskStatus.CANCELLED);
    }

    /**
     * Graceful shutdown.
     */
    public void shutdown() {
        dispatcher.stop();
        dispatcherThread.interrupt();
        executorService.shutdown();

        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
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

        Task reportTask = new Task(
                () -> System.out.println(Thread.currentThread().getName() + " -> Report Generated"),
                new OneTimeSchedule(2000)
        );

        Task emailTask = new Task(
                () -> System.out.println(Thread.currentThread().getName() + " -> Email Sent"),
                new OneTimeSchedule(4000)
        );

        Task paymentTask = new Task(
                () -> {
                    System.out.println(Thread.currentThread().getName() + " -> Payment Processed");
                },
                new OneTimeSchedule(1000)
        );

        scheduler.schedule(reportTask);
        scheduler.schedule(emailTask);
        scheduler.schedule(paymentTask);

        Thread.sleep(6000);
        scheduler.shutdown();
    }
}
