<?php

namespace sergebezborodov\beanstalk;

use yii\base\Event;

class JobEvent extends Event
{
    /**
     * @var array
     */
    public $job;

    /**
     * @param array $job
     * @param array $config
     */
    public function __construct($job, $config = [])
    {
        parent::__construct($config);
        $this->job = $job;
    }
}