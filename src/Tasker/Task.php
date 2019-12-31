<?php

  namespace Tasker;

	abstract class Task extends \Tasker {

    protected static $execution_delay     = 0;        // Execution delay in seconds
    protected static $max_execution_time  = 600;      // Max execution time in seconds
    protected static $retry_strategy      = [         // Default retry strategy in seconds
      1, 5, 10, 15, 20
    ];

    protected $log_prefix                 = '  ';     // We prefix the logs for forked processes
    protected $arguments                  = [];       // Task arguments

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
    
    public function before(array $args = []) : bool {
      return true;
    }

    public function after(array $args = []) : bool {
      return true;
    }

    public function perform(array $arguments = []) : void {
      throw new \TaskerPerformNotImplementedException('The task that is being executed does not have a custom perform() method');
    }

    public function key() : string {
      $key = $this->namespace() .':'. $this->queue() .':'. get_class($this) .':'. sha1(json_encode($this->arguments));
      return $key;
    }

    public function retry(\Exception $e) {
      if($this->retry_count <= sizeof(self::$retry_strategy)) {
        $this->log('Task failed, but we should retry. Exception: '. $e->getMessage());
        $this->retry_count++;
        $this->add($this);
      } else {
        $this->log('Task failed, retry limit reached');
        $this->failed($e);
      }
    }

    public function failed(\Exception $e) {
      $this->log('Task failed. Exception: '. $e->getMessage());
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
      $delay = self::$execution_delay;
      if($this->retry_count > 0) {
        if(isset(self::$retry_strategy[-1+$this->retry_count])) {
          $retry_delay = self::$retry_strategy[-1+$this->retry_count];
        } else {
          $this->log('No retry delay configured for retry count '. $this->retry_count .' (index '. (-1+$this->retry_count .')'));
        }
        return $delay = 0;
      }
      return time() + $delay;
    }
  }
