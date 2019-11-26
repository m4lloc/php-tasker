<?php

  namespace Tasker;

	abstract class Task extends \Tasker {

    protected static $execution_delay     = 0;        // Execution delay in seconds
    protected static $max_execution_time  = 600;      // Max execution time in seconds
    protected static $max_retry_count     = 5;        // Max execution time in seconds

    protected $log_prefix                 = '  ';     // We prefix the logs for forked processes

    private $arguments                    = [];       // Task arguments
    private $retry_count                  = 0;        // Current attempts to retry

    public function __construct(array $arguments = []) {
      $this->arguments = $arguments;
    }

    public static function perform_async(array $arguments = []) : \Tasker\Task {
      $class = get_called_class();
      $task = new $class($arguments);
      self::add($task);
      return $task;
    }

    public function perform(array $arguments = []) : void {
      throw new \TaskerPerformNotImplementedException('The task that is being executed does not have a custom perform() method');
    }

    public function key() : string {
      $key = $this->namespace() .':'. $this->queue() .':'. get_class($this) .':'. sha1(json_encode($this->arguments));
      return $key;
    }

    public function retry() {
      throw new \TaskerRetryNotImplementedException('Task retries is not yet implemented');
      if($this->retry_count <= self::$max_retry_count) {
        $this->log('Task failed, but we should retry');
        $this->retry_count++;
      } else {
        $this->log('Task failed, retry limit reached');
        $this->failed();
      }
    }

    public function failed() {
      $this->log('Task failed');
    }

    public function complete() {
      $this->log('Task completed');
    }

    // public function cancelable() : bool {
    //   return self::$cancelable;
    // }

    public function queue() {
      return 'default';
    }

    public function delay() {
      return time() + self::$execution_delay;
    }
  }
