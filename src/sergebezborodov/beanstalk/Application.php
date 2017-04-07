<?php

namespace sergebezborodov\beanstalk;

use yii\helpers\Console;

/**
 * Application for beanstalk worker
 * @package sergebezborodov\beanstalk
 */
class Application extends \yii\console\Application
{
    const EVENT_BEFORE_JOB = 'beforeJob';
    const EVENT_AFTER_JOB = 'afterJob';

    const EXIT_PARAM = '--exit-after-complete';

    public $enableCoreCommands = false;

    /**
     * Handle system signals
     * works when pcntl enabled
     *
     * @var bool
     */
    public $handleSignals = true;


    /**
     * Exit worker when handle database exception
     *
     * @var bool
     */
    public $exitOnDbException = false;

    /**
     * Flag when script need to be terminated
     *
     * @var bool
     */
    private $_needTerminate = false;

    /**
     * Flat when task is currently working
     *
     * @var bool
     */
    private $_isWorkingNow = false;

    /**
     * @inheritdoc
     */
    public function coreComponents()
    {
        return array_merge(parent::coreComponents(), [
            'router'    => ['class' => '\sergebezborodov\beanstalk\Router'],
            'beanstalk' => ['class' => '\sergebezborodov\beanstalk\Beanstalk'],
        ]);
    }

    /**
     * @inheritdoc
     */
    public function handleRequest($request)
    {
        $request = $this->getRequest();
        $response = $this->getResponse();
        /** @var Beanstalk $info */
        $beanstalk = $this->get('beanstalk');
        /** @var Router $router */
        $router = $this->get('router');
        \Yii::info(
            "Start working.\n " . json_encode(['params' => $request->getParams()], JSON_UNESCAPED_UNICODE),
            'grnrbt\beanstalk'
        );
        
        $exitAfterComplete = false;
        try {
            $params = $request->getParams();
            if ($pos = array_search(self::EXIT_PARAM, $params)) {
                $exitAfterComplete = true;
                unset($params[$pos]);
            }

            $tubes = $params;
            if ($tubes) {
                \Yii::info(
                    "Start to listen custom tubes.\n " . json_encode($tubes, JSON_UNESCAPED_UNICODE),
                    'grnrbt\beanstalk'
                );
                foreach ($tubes as $tube) {
                    if (!$beanstalk->watch($tube)) {
                        throw new Exception("Unable to watch {$tube}");
                    }
                }
            } else {
                \Yii::info(
                    "Start to listen default tubes.\n " . json_encode($tubes, JSON_UNESCAPED_UNICODE),
                    'grnrbt\beanstalk'
                );
                $tubes = $beanstalk->listTubes();
            }
            $onlyOneTube = count($tubes) == 1;
            $tube = reset($tubes);
            $route = $router->getRoute($tube);

            while (true) {
                $this->unregisterSignalHandler();
                $job = $beanstalk->reserve();
                $this->registerSignalHandler();
                \Yii::info("Reserve job.\n " . json_encode($job, JSON_UNESCAPED_UNICODE), 'grnrbt\beanstalk');
                $this->trigger(static::EVENT_BEFORE_JOB, new JobEvent($job));

                if (!$onlyOneTube) {
                    $info = $beanstalk->statsJob($job['id']);
                    $tube = $info['tube'];
                    $route = $router->getRoute($tube);
                }

                try {
                    $this->_isWorkingNow = true;
                    $actResp = $this->runAction($route, [$job['body'], $job['id']]);
                    \Yii::info(
                        "Run action.\n " . json_encode(['route' => $route, 'result' => $actResp], JSON_UNESCAPED_UNICODE),
                        'grnrbt\beanstalk'
                    );
                    if ($actResp) {
                        $beanstalk->delete($job['id']);
                        \Yii::info("Delete job.\n " . json_encode($job, JSON_UNESCAPED_UNICODE), 'grnrbt\beanstalk');
                    } else {
                        $beanstalk->bury($job['id'], 0);
                        \Yii::info("Bury job.\n " . json_encode($job, JSON_UNESCAPED_UNICODE), 'grnrbt\beanstalk');
                    }
                    $this->trigger(static::EVENT_AFTER_JOB, new JobEvent($job));

                    $this->_isWorkingNow = false;
                    $this->signalDispatch();
                    if ($this->_needTerminate || $exitAfterComplete) {
                        \Yii::info('Stop working.', 'grnrbt\beanstalk');
                        $this->endApp();
                    }
                } catch (\Exception $e) {
                    \Yii::error("Exception. Message: {$e->getMessage()}.", 'grnrbt\beanstalk');
                    fwrite(STDERR, Console::ansiFormat($e."\n", [Console::FG_RED]));
                    $beanstalk->bury($job['id'], 0);
                    \Yii::info("Bury job.\n " . json_encode($job, JSON_UNESCAPED_UNICODE), 'grnrbt\beanstalk');
                    $this->trigger(static::EVENT_AFTER_JOB, new JobEvent($job));

                    if ($e instanceof \yii\db\Exception && $this->exitOnDbException) {
                        $this->_needTerminate = true;
                    }

                    $this->_isWorkingNow = false;
                    if ($this->_needTerminate) {
                        \Yii::info("Stop working.", 'grnrbt\beanstalk');
                        $this->endApp();
                    }
                }
            }
        } catch (\Exception $e) {
            \Yii::error("Exception. Message: {$e->getMessage()}.", 'grnrbt\beanstalk');
            $response->exitStatus = 1;
            fwrite(STDERR, Console::ansiFormat($e."\n", [Console::FG_RED]));
        }

        return $response;
    }

    protected function endApp()
    {
        exit;
    }

    private function registerSignalHandler()
    {
        if (!extension_loaded('pcntl')) {
            return;
        }

        pcntl_signal(SIGINT, function ($signal) {
            fwrite(STDOUT, Console::ansiFormat("Received SIGINT will exit soon\n", [Console::FG_RED]));
            if ($this->_isWorkingNow) {
                $this->_needTerminate = true;
            } else {
                $this->endApp();
            }
        });
        declare(ticks = 1);
        register_tick_function([$this, 'signalDispatch']);
    }

    private function unregisterSignalHandler()
    {
        if (!extension_loaded('pcntl')) {
            return;
        }
        pcntl_signal(SIGINT, SIG_DFL);
        unregister_tick_function([$this, 'signalDispatch']);
    }

    public function signalDispatch()
    {
        if (!extension_loaded('pcntl')) {
            return;
        }
        pcntl_signal_dispatch();
    }
}
