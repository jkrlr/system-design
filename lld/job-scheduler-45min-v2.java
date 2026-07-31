import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/*
 * Assumptions:
 * 1. Single JVM, in-memory scheduler.
 * 2. Supports one-time delayed tasks.
 * 3. Supports status tracking and cancellation.
 * 4. Persistence, retries, recurring schedules and distributed execution
 *    are future extensions.
 */
public class TaskSchedulerDemo {

    /*
     * Defines when a task should execute.
     *
     * This is a Strategy interface. We can later add:
     * - FixedDelaySchedule
     * - FixedRateSchedule
     * - CronSchedule
     */
    public interface Schedule {
        long executionTimeMillis();
    }

    public static class OneTimeSchedule implements Schedule {

        private final long executionTimeMillis;

        public OneTimeSchedule(long delay, TimeUnit unit) {
            if (delay < 0) {
                throw new IllegalArgumentException("Delay cannot be negative");
            }
            this.executionTimeMillis = System.currentTimeMillis() + unit.toMillis(delay);
        }

        @Override
        public long executionTimeMillis() {
            return executionTimeMillis;
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
    public static class Task {

        private final String id;
        private final Runnable runnable;
        private final Schedule schedule;

        /*
         * AtomicReference provides:
         * 1. Visibility across threads.
         * 2. Atomic lifecycle transitions using compareAndSet.
         */
        private final AtomicReference<TaskStatus> status = new AtomicReference<>(TaskStatus.CREATED);

        public Task(Runnable runnable, Schedule schedule) {
            this.id = UUID.randomUUID().toString();
            this.runnable = runnable;
            this.schedule = schedule;
        }

        public TaskStatus getStatus() {
            return status.get();
        }

        public boolean markScheduled() {
            return status.compareAndSet(TaskStatus.CREATED, TaskStatus.SCHEDULED);
        }
      
        public boolean markRunning() {
            return status.compareAndSet(TaskStatus.SCHEDULED, TaskStatus.RUNNING);
        }

        public boolean markSuccess() {
            return status.compareAndSet(TaskStatus.RUNNING, TaskStatus.SUCCESS);
        }

        public boolean markFailed() {
            return status.compareAndSet(TaskStatus.RUNNING, TaskStatus.FAILED);
        }

        public boolean cancel() {
            return status.compareAndSet(TaskStatus.SCHEDULED, TaskStatus.CANCELLED);
        }
    }

    /*
     * DelayQueue only accepts elements implementing Delayed.
     *
     * DelayedTask is an adapter between our business Task
     * and Java's DelayQueue.
     */
    @Getter
    @RequiredArgsConstructor
    public static class DelayedTask implements Delayed {

        private final Task task;
        private final long executionTimeMillis;

        public DelayedTask(Task task) {
            this(task, task.getSchedule().executionTimeMillis());
        }

        @Override
        public long getDelay(TimeUnit unit) {
            long remainingDelay = executionTimeMillis - System.currentTimeMillis();
            return unit.convert(remainingDelay, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            DelayedTask otherTask = (DelayedTask) other;
            return Long.compare(executionTimeMillis, otherTask.executionTimeMillis);
        }
    }

    /*
     * Separates task execution from scheduling.
     *
     * A future implementation may add:
     * - logging
     * - metrics
     * - retries
     * - tracing
     */
    public interface TaskExecutor {
        void execute(Task task);
    }

    public static class DefaultTaskExecutor implements TaskExecutor {

        @Override
        public void execute(Task task) {
            task.getRunnable().run();
        }
    }

    /*
     * Dispatcher waits for ready tasks and submits them
     * to the worker thread pool.
     */
    @RequiredArgsConstructor
    public static class Dispatcher implements Runnable {

        private final DelayQueue<DelayedTask> delayQueue;
        private final ExecutorService executorService;
        private final TaskExecutor taskExecutor;
        private final Map<String, Future<?>> runningTasks;

        private volatile boolean running = true;

        @Override
        public void run() {
            while (running) {
                try {
                    /*
                     * Blocks until the earliest task becomes ready.
                     * No active polling is required.
                     */
                    DelayedTask delayedTask = delayQueue.take();
                    Task task = delayedTask.getTask();

                    /*
                     * Atomic transition prevents a cancelled task
                     * from moving to RUNNING.
                     */
                    if (!task.markRunning()) {
                        continue;
                    }

                    Future<?> future = executorService.submit(() -> {
                        try {
                            taskExecutor.execute(task);
                            task.markSuccess();
                        } catch (Exception exception) {
                            task.markFailed();
                            System.err.println("Task failed: " + task.getId());
                        } finally {
                            runningTasks.remove(task.getId());
                        }
                    });

                    runningTasks.put(task.getId(), future);

                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    break;

                } catch (RejectedExecutionException exception) {
                    System.err.println("Task rejected because scheduler is shutting down");
                }
            }
        }

        public void stop() {
            running = false;
        }
    }

    public static class TaskScheduler {

        private final DelayQueue<DelayedTask> delayQueue = new DelayQueue<>();
        private final ExecutorService executorService = Executors.newFixedThreadPool(5);

        private final Map<String, Task> tasks = new ConcurrentHashMap<>();

        private final Map<String, DelayedTask> queuedTasks = new ConcurrentHashMap<>();
        private final Map<String, Future<?>> runningTasks = new ConcurrentHashMap<>();

        private final Dispatcher dispatcher;
        private final Thread dispatcherThread;

        private volatile boolean acceptingTasks = true;

        public TaskScheduler() {
            dispatcher = new Dispatcher(delayQueue, executorService, new DefaultTaskExecutor(), runningTasks);

            dispatcherThread = new Thread(dispatcher, "dispatcher-thread");
            dispatcherThread.start();
        }

        public String schedule( Runnable runnable, Schedule schedule) {
            if (!acceptingTasks) {
                throw new IllegalStateException("Scheduler has been shut down");
            }

            Task task = new Task(runnable, schedule);

            if (!task.markScheduled()) {
                throw new IllegalStateException("Task could not be scheduled");
            }

            DelayedTask delayedTask = new DelayedTask(task);

            tasks.put(task.getId(), task);
            queuedTasks.put(task.getId(), delayedTask);
            delayQueue.offer(delayedTask);

            return task.getId();
        }

        public Optional<TaskStatus> getStatus(String taskId) {
            Task task = tasks.get(taskId);
            return task == null ? Optional.empty() : Optional.of(task.getStatus());
        }

        public boolean cancel(String taskId) {
            Task task = tasks.get(taskId);

            if (task == null) {
                return false;
            }

            /*
             * Cancels only tasks that have not started.
             */
            if (task.cancel()) {
                DelayedTask delayedTask = queuedTasks.remove(taskId);

                if (delayedTask != null) {
                    delayQueue.remove(delayedTask);
                }

                return true;
            }

            /*
             * Optional support for interrupting an already submitted task.
             */
            Future<?> future = runningTasks.get(taskId);

            if (future != null) {
                return future.cancel(true);
            }

            return false;
        }

        public void shutdown() {
            acceptingTasks = false;

            dispatcher.stop();
            dispatcherThread.interrupt();

            executorService.shutdown();

            try {
                if (!executorService.awaitTermination(
                        5,
                        TimeUnit.SECONDS
                )) {
                    executorService.shutdownNow();
                }

            } catch (InterruptedException exception) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    public static void main(String[] args)
            throws InterruptedException {

        TaskScheduler scheduler = new TaskScheduler();

        String reportTaskId = scheduler.schedule(
                () -> System.out.println( Thread.currentThread().getName() + " -> Report generated"),
          d      new OneTimeSchedule(2, TimeUnit.SECONDS)
        );
        String emailTaskId = scheduler.schedule(
                () -> System.out.println(Thread.currentThread().getName() + " -> Email sent"),
                new OneTimeSchedule(4, TimeUnit.SECONDS)
        );
        String paymentTaskId = scheduler.schedule(
                () -> System.out.println(Thread.currentThread().getName() + " -> Payment processed"),
                new OneTimeSchedule(1, TimeUnit.SECONDS)
        );
        String cancelledTaskId = scheduler.schedule(
                () -> System.out.println("This should not execute"),
                new OneTimeSchedule(3, TimeUnit.SECONDS)
        );

        scheduler.cancel(cancelledTaskId);
        Thread.sleep(5_000);

        System.out.println("Report status: " + scheduler.getStatus(reportTaskId).orElse(null));
        System.out.println("Email status: " + scheduler.getStatus(emailTaskId).orElse(null));
        System.out.println("Payment status: " + scheduler.getStatus(paymentTaskId).orElse(null));
        System.out.println("Cancelled status: " + scheduler.getStatus(cancelledTaskId).orElse(null));

        scheduler.shutdown();
    }
}

/*
Likely interviewer questions

Why not use ScheduledExecutorService?
For simple delayed execution, I would prefer ScheduledExecutorService. 
I am building a custom scheduler here because the requirements include task IDs, explicit lifecycle states, monitoring, custom cancellation, retry policies, and potentially pluggable scheduling strategies. 
If those requirements are absent, the built-in scheduler is simpler and safer.

Why DelayQueue?
It maintains tasks according to their remaining delay and blocks the consumer until the earliest task becomes eligible. This avoids active polling.

Why separate dispatcher and worker pool?
The dispatcher should only wait for eligible tasks and hand them off. If it executes business logic itself, one long-running task blocks dispatching of other ready tasks.

Is volatile enough for status?
It ensures visibility but not atomic state transitions. Since cancellation and execution can race, I would use AtomicReference and compare-and-set.

How would this work across multiple instances?
The current implementation is single-process. 
For multiple instances, task metadata must be persisted and due tasks must be claimed using row locks, optimistic versioning, or a distributed lease. 
Only the owner of the lease should dispatch the task.
*/
