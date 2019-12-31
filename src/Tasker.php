<?php

  // Tasker Exceptions
  class TaskerException extends Exception { }
  class TaskerConnectionException extends \TaskerException { }
  class TaskerExecutionException extends \TaskerException { }
  class TaskerRetryException extends \TaskerException { }
  class TaskerFailedException extends \TaskerException { }

  class Tasker {

    public const HANDLER_MYSQL_RECONNECT = 0;

    private static $options   = null;     // Tasker config
    private static $redis     = false;    // Redis connection
    private static $scheduler = 0;        // PID of scheduler
    private static $workers   = [];       // Information about workers
    private static $handlers  = [];       // List for custom handlers

    private $quiet            = false;    // Should we stop starting new jobs
    private $pid              = null;     // Register PID of current process

    protected $log_prefix     = '';       // We prefix the logs for forked processes

    public function __construct(array $options=[]) {
      $this->setOptions($options);
      $this->connect();
    }

    public function setOptions(array $options) : array {
      self::$options = $options + [
        'host' => '127.0.0.1',
        'port' => 6379,
        'logging' => true,                // Enable or disable logging
        'workers' => [                    // Maximum worker processes
          'high' => 0,
          'default' => 10,
          'low' => 0
        ],
        'persistent' => 1,                // Use persistent connections
        'read_write_timeout' => 0         // Do not drop connections when waiting for new tasks
      ];
      return self::$options;
    }

    public function addHandler(int $handler, $callback) : void {
      self::$handlers[$handler][] = $callback;
    }

    private static function runHandler(int $handler) : void {
      if(isset(self::$handlers[$handler])) {
        foreach(self::$handlers[$handler] as $callback) {
          $callback();
        }
      }
    }

    private function connect() : \Predis\Client {
      $this->log('Connecting to Redis...');
      try {
        self::$redis = new \Predis\Client(self::$options);
      } catch (RedisException $e) {
        $this->log('Connection to Redis FAILED, see error log for more information');
        throw new \TaskerConnectionException($e->getMessage());
      }
      $this->log('Connected');
      return self::$redis;
    }

    public function run() : void {
      $this->log('Starting Tasker environment...');
      $this->setSignalHandlers();
      $this->forkScheduler();
      $this->log('Dispatcher ready to execute tasks');
      do {
        $this->running();
        $this->forkWorker();
        sleep(1); // Let the CPU rest for a moment
      } while(!$this->stop());
      $this->log('Tasker stopped');
    }

    private function setSignalHandlers() : void {
      $this->log('Setting signal handlers...');
      $this->pcntl_async(true);

      // Signal used to cause program termination
      $this->pcntl_signal_handler(SIGTERM, array($this, 'stopGracefully'));
            
      // Signal sent to a process when its controlling terminal is closed
      // $this->pcntl_signal_handler(SIGHUP, array($this, 'stopGracefully'));
    }

    private function pcntl_async(bool $enable=true) : bool {
      $this->log('Retrieve signals async='. $enable);
      return pcntl_async_signals($enable);
    }

    private function pcntl_signal_handler(int $signal, Callable $callback) : bool {
      $this->log('Setting handler for signal: '. $signal);
      return pcntl_signal($signal, $callback);
    }

    public function stopGracefully() : void {
      $this->log('Stopping gracefully...');
      $this->quiet = true;
    }

    public function quiet() : bool {
      return $this->quiet;
    }

    private function stop() : bool {
      if($this->running() == 0 && $this->quiet) {
        return true;
      }
      return false;
    }

    private function forkScheduler() : bool {
      $this->log('Trying to start scheduler...');
      $pid = pcntl_fork();
      if($pid == -1) {
        $this->log('Failed to start scheduler');
        $this->exit(1);
      } elseif($pid) {
        // We are the parent task and have nothing 
        // else to do except register the PID
        self::$scheduler = $pid;
      } else {
        $this->log('Scheduler started');
        while(true) {
          $this->scheduler();
          sleep(1);
        }
      }
      return $pid;
    }

    private function scheduler() : void {
      $ns = $this->namespace() .':scheduled';
      self::$redis->watch($ns);
      foreach(self::$options['workers'] as $queue => $max) {
        $rs = self::$redis->zrangebyscore($ns, '-inf', time());
        foreach($rs as $r) {
          $j = json_decode($r);

          self::$redis->multi();
          self::$redis->rpush($this->namespace() .':'. $j->queue, $j->data);
          self::$redis->zrem($ns, $r);
          self::$redis->exec();
        }
      }
      self::$redis->unwatch();
    }

    private function running() : int {
      $count = 0;
      foreach(self::$options['workers'] as $queue => $max) {
        if(!empty(self::$workers[$queue])) {
          foreach(self::$workers[$queue] as $key => $pid) {
            $count++;
            $r_pid = pcntl_waitpid($pid, $status, WNOHANG);
            if($r_pid == -1 || $r_pid > 0) {
              $this->log('Worker stopped with exit code: '. $status);
              unset(self::$workers[$queue][$key]);
              $count--;
            }
          }
        }
      }
      return $count;
    }

    private function forkWorker() : bool {
      if(!$this->quiet()) {
        foreach(self::$options['workers'] as $queue => $max) {
          $min = (!empty(self::$workers[$queue])) ? sizeof(self::$workers[$queue]) : 0 ;
          for($i=$min; $i<$max; $i++) {
            $this->worker($queue);
          }
        }
      } else {
        $this->log('We should stop, not starting any new tasks');
      }
      return false;
    }

    private function worker(string $queue) : int {
      $pid = pcntl_fork();
      if($pid == -1) {
        $this->log('Failed to fork process in order to start a new job');
      } elseif($pid) {
        // We are the parent task and have nothing 
        // else to do except register the PID
        self::$workers[$queue][] = $pid;
      } else {
        $this->process($queue);
      }
      return $pid;
    }

    private function process(string $queue) {
      $t = $this->get($queue);
      $this->log('Found task '. $t->key());
      self::runHandler(self::HANDLER_MYSQL_RECONNECT);
      try {
        $t->perform($t->arguments);
      } catch(\TaskerFailedException $e) {
        $t->failed($e);
        $this->exit(1);
      } catch(\TaskerRetryException | Exception $e) {
        $t->retry($e);
        $this->exit();
      }
      $t->complete();
      $this->exit();
    }

    private function get(string $queue) : \Tasker\Task {
      list( , $t) = self::$redis->blpop($this->namespace() .':'. $queue, 0);
      if($t) {
        $t = base64_decode($t);
        $o = unserialize($t);
        if($o) {
          return $o;
        } else {
          $this->log('Unable to unserialize data: '. $t);
        }
      }
      $this->exit(1);
    }

    public static function add(\Tasker\Task $task) : bool {
      $r = self::$redis->zadd(self::namespace() .':scheduled', $task->delay(), json_encode([
        'queue' => $task->queue(),
        'data' => base64_encode(serialize($task))
      ]));
      if($r === 1) {
        return true;
      }
      return false;
    }

    // protected function remove(\Tasker\Task $task) : int {
    //   $this->log('Removing task '. $task->key());
    //   $rows = self::$redis->del($task->key());
    //   // $this->log('Count '. $task->key() .' '. $rows);
    //   if($rows > 0) {
    //     $this->log('Task '. $task->key() .' removed');
    //     return true;
    //   }
    //   $this->log('Failed to remove task '. $task->key());
    //   return false;
    // }

    protected function log($msg) {
      if(self::$options['logging']) {
        echo '['. date('r') .'] '. getmypid() .' - '. $this->log_prefix . $msg . PHP_EOL;
      }
    }

    protected static function namespace() : string {
      return 'php-tasker';
    }

    private function exit(int $code = 0) {
      // self::$redis->close();
      exit($code);
    }
  }
