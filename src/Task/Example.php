<?php

  namespace Task;

  class Example extends \Tasker\Task {
    
    public function perform(array $arguments = []) : void {
      usleep(rand(0, 100));
      // throw new \TaskerFailedException();
    }
  }