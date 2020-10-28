<?php

declare(strict_types=1);

namespace App\Consumer;

use App\Common\ConsumerStopWaitGroup;
use App\Consumer\MonitorInfo\MonitorTaskInfo;
use App\Consumer\MonitorTask\MonitorTask;
use App\Events\QueueReStarted;
use App\Events\QueueStarted;
use App\Events\QueueStatus;
use App\Events\QueueStoped;
use App\Model\Datasource;
use App\Model\Model;
use App\Model\MonitorCycleCompare;
use App\Model\MonitorProtocolDetect;
use App\Model\MonitorUniversal;
use App\Model\MonitorUprushDownrush;
use Hyperf\Contract\StdoutLoggerInterface;
use Hyperf\Di\Annotation\Inject;
use Hyperf\Di\Container;
use Hyperf\Logger\LoggerFactory;
use Psr\Container\ContainerInterface;
use Psr\EventDispatcher\EventDispatcherInterface;
use Swoole\Coroutine;
use Swoole\Process;

class MonitorConsumer extends Consumer
{
    const WATCH_COMMAND_TIME = 8;

    const MAX_STOP_LOCK_TIMEOUT = 180;

    public static $dataSource = [];

    /**
     * @var Process
     */
    public $process;

    /**
     * 所有启动的消费者id数组(type.'_'.topic->id).
     *
     * @var array
     */
    public $startedTopicIdArr = [];

    /**
     * @var StdoutLoggerInterface
     */
    protected $stdOutLogger;

    /**
     * 消费者到协程id的映射.
     *
     * @var array
     */
    protected $topicIdMapToCid = [];

    /**
     * 消费者协程实例数组.
     *
     * @var MonitorTask[]
     */
    protected $mainConsumerObjArr = [];

    /**
     * @var string
     */
    private $name = 'alarm-dog-monitor';

    /**
     * 系统是否运行.
     * @var bool
     */
    private $running = true;

    /**
     * @var Container
     */
    private $container;

    /**
     * @Inject
     * @var EventDispatcherInterface
     */
    private $eventDispatcher;

    /**
     * @var ConsumerStopWaitGroup
     */
    private $consumerStopWaitGroup;

    /**
     * 停止消费进程锁， 必须按照顺序一组一组topic run停止.
     * @var
     */
    private $stopMutexLock = false;

    /**
     * @var \Psr\Log\LoggerInterface
     */
    private $fileLogger;

    /**
     * RabbitMqConsumer constructor.
     *
     * @param KafkaConsumerInstance $consumerInstance
     */
    public function __construct(
        ContainerInterface $container,
        LoggerFactory $loggerFactory,
        StdoutLoggerInterface $stdOutlogger
    ) {
        $this->container = $container;
        $this->stdOutLogger = $stdOutlogger;
        $this->fileLogger = $loggerFactory->get('MonitorConsumer', 'default');
    }

    /**
     * start.
     */
    public function start()
    {
        $this->stdOutLogger->info('Start to monitor!');
        $this->init();
        $this->stdOutLogger->info('(cid:' . \Hyperf\Utils\Coroutine::id() . ') Init process at ' . date('Y-m-d H:i:s'));
        $this->registerSignal();
        $this->stdOutLogger->info('(cid:' . \Hyperf\Utils\Coroutine::id() . ') Register signal at' . date('Y-m-d H:i:s'));
        $this->main();
        $this->stdOutLogger->info('(cid:' . \Hyperf\Utils\Coroutine::id() . ') Start main at ' . date('Y-m-d H:i:s'));
    }

    /**
     * register signal.
     */
    public function registerSignal(): void
    {
        $this->process->signal(SIGTERM, function () {
            \Hyperf\Utils\Coroutine::create(function () {
                $this->fileLogger->info("process [{$this->process->pid}]" . $this->name . ' receive sigterm signal and to stop', []);
                $this->stdOutLogger->info("process [{$this->process->pid}]" . $this->name . ' receive sigterm signal and to stop', []);
                //stop all coroutine consumer
                $this->stopAllCoroutineConsumer();
                //stop watch listener
                $this->stopMaster();
                $this->stdOutLogger->info("process [{$this->process->pid}]" . $this->name . ' stop all coroutine consumer and master', []);
                $this->fileLogger->info("process [{$this->process->pid}]" . $this->name . ' stop all coroutine consumer and master', []);
            });
        });
    }

    /**
     * stop all coroutine consumer.
     */
    public function stopAllCoroutineConsumer(): void
    {
        if (! empty($this->startedTopicIdArr)) {
            foreach ($this->startedTopicIdArr as $consumerId) {
                $this->stopConsumer($consumerId, 1);
            }
        }
    }

    /**
     * Stop master process.
     */
    public function stopMaster(): void
    {
        $this->running = false;
    }

    /**
     * @param object $run
     * @param mixed $model
     */
    public function gotoStart($model)
    {
        /** @var $monitorTaskInfo */
        $monitorTaskInfo = $this->generateConsumerInfo($model);
        $res = $this->startCoroutineConsumer($monitorTaskInfo);
        if ($res) {
            $this->eventDispatcher->dispatch(new QueueStarted($monitorTaskInfo));
        }
    }

    /**
     * Stop coroutine consumer.
     *
     * @param $consumerId //queue_type_topicid
     * @param int $signal
     *                    1:消费者被sigterm信号停止，不修改消费者状态值，下次服务自动启动还可以保证消费者正常激活
     *                    0:正常通过后台操作停止消费者需要更新数据库消费者状态为停止状态
     * @param mixed $flag 0代表正常的停止服务，1代表重启服务
     */
    public function stopConsumer($consumerId, $signal = 0, $flag = 0): bool
    {
        [$taskType, $taskId] = explode('_', $consumerId);
        /**
         * @var Model $model
         */
        $model = MonitorTask::$taskMapModel[$taskType];
        $run = $model::query()->where('id', $taskId)->first();
        $run->queue_type = $taskType;
        $monitorTaskInfo = MonitorConsumer::generateConsumerInfo($run);

        //check is not has task running
        if (! in_array($consumerId, $this->startedTopicIdArr)) {
            $this->fileLogger->info($consumerId . ' is not running');
            if ($flag == 1) {
                goto restart;
            }
            return true;
        }

        if ($this->stopMutexLock) {
            $currentLockTimes = 0;
            while ($this->stopMutexLock) {
                Coroutine::sleep(1);
                if (self::MAX_STOP_LOCK_TIMEOUT < $currentLockTimes++) {
                    $this->stopMutexLock = false;
                    Coroutine::sleep(1);
                }
                $noticeMessage = "The coroutine monitor topicid ({$consumerId}) wait stop lock at" . date('Y-m-d H:i:s');
                $this->stdOutLogger->info($noticeMessage);
                $this->fileLogger->info($noticeMessage);
            }
        }

        try {
            $this->stopMutexLock = true;
            $noticeLockMessage = "【monitor】The coroutine monitor topicid ({$consumerId}) try lock success at " . date('Y-m-d H:i:s');
            $this->stdOutLogger->info($noticeLockMessage);
            $this->fileLogger->info($noticeLockMessage);

            $cidArr = isset($this->topicIdMapToCid[$consumerId]) ? $this->topicIdMapToCid[$consumerId] : null;
            if (! empty($cidArr)) {
                //正常退出，非信号打断
                if ($signal == 0) {
                    $this->eventDispatcher->dispatch(new QueueStoped($monitorTaskInfo, $flag));
                }

                //stop all coroutine consumer
                foreach ($cidArr as $ckey => $cid) {
                    $this->mainConsumerObjArr[$cid]->stop();
                    $this->consumerStopWaitGroup->add();
                }
                $this->stdOutLogger->info("【monitor】The coroutine monitor topicid ({$consumerId}) waiting at " . date('Y-m-d H:i:s'));
                $this->consumerStopWaitGroup->wait();
                $this->stdOutLogger->info("【monitor】The coroutine monitor topicid ({$consumerId}) waitted at " . date('Y-m-d H:i:s'));
                //移除启动的程序
                unset($this->startedTopicIdArr[$consumerId]);
                foreach ($cidArr as $ckey => $cid) {
                    unset($this->mainConsumerObjArr[$cid], $this->topicIdMapToCid[$consumerId][$ckey]);
                }
                $noticeUnlockMessage = "【monitor】The coroutine consumer ({$monitorTaskInfo->getTaskFlag()}) all child coroutine has stoped at " . date('Y-m-d H:i:s');
                $this->stdOutLogger->info($noticeUnlockMessage);
                $this->fileLogger->info($noticeUnlockMessage);
            }

            //restart
            restart:
            if ($flag == 1) {
                $this->eventDispatcher->dispatch(new QueueReStarted($taskType, $taskId));
                $noticeUnlockMessage = "The coroutine consumer ({$monitorTaskInfo->getTaskFlag()}) restarting at " . date('Y-m-d H:i:s');
                $this->stdOutLogger->info($noticeUnlockMessage);
                $this->fileLogger->info($noticeUnlockMessage);
            }
        } catch (\Throwable $e) {
            $msg = 'stop consumer fail--' . $e->getMessage() . '---' . $e->getTraceAsString();
            $this->stdOutLogger->error($msg);
            $this->fileLogger->error($msg);
        } finally {
            $this->stopMutexLock = false;
        }

        return true;
    }

    /**
     * @param $dataSourceId
     * @param $data
     * @return mixed
     */
    public static function filterData($dataSourceId, $data)
    {
        $dataSource = self::$dataSource[$dataSourceId];
        foreach ($data as $field => $val) {
            $dataType = self::$dataSource[$dataSourceId][$field];
            switch ($dataType) {
                case 'integer':
                    if (is_array($val)) {
                        foreach ($val as $valKey => $valVal) {
                            $val[$valKey] = intval($valVal);
                        }
                        $data[$field] = $val;
                    } else {
                        $data[$field] = intval($val);
                    }
                    break;
                case 'float':
                    if (is_array($val)) {
                        foreach ($val as $valKey => $valVal) {
                            $val[$valKey] = floatval($valVal);
                        }
                        $data[$field] = $val;
                    } else {
                        $data[$field] = floatval($val);
                    }
                    break;
            }
        }

        return $data;
    }

    /**
     * set log.
     */
    private function init(): void
    {
        $this->consumerStopWaitGroup = $this->container->get(ConsumerStopWaitGroup::class);
    }

    /**
     * init data source.
     */
    private function initDataSource()
    {
        $this->stdOutLogger->info('Init data source info ing!');
        $this->updateDataSource();
        $this->stdOutLogger->info('Init data source info done!');
        //定时更新
        \Hyperf\Utils\Coroutine::create(function () {
            while (1) {
                try {
                    \Swoole\Coroutine::sleep(20);
                    $this->updateDataSource();
                } catch (\Throwable $e) {
                    $this->logger->error('Update data source error! ' . $e->getMessage());
                    $this->stdOutLogger->error('Update data source error! ' . $e->getMessage());
                    \Swoole\Coroutine::sleep(10);
                }
            }
        });
    }

    /**
     * 更新数据源.
     */
    private function updateDataSource()
    {
        $dataSource = Datasource::query()->select(['id', 'fields'])->get()->toArray();
        foreach ($dataSource as $key => $value) {
            $fieldArr = json_decode($value['fields'], true);
            if (! empty($fieldArr['fields']) && count($fieldArr['fields']) > 0) {
                foreach ($fieldArr['fields'] as $fieldVal) {
                    if (isset($fieldVal['field'], $fieldVal['type'])) {
                        self::$dataSource[$value['id']][$fieldVal['field']] = $fieldVal['type'];
                    }
                }
            }
        }
    }

    /**
     * Start and listen command from admin manager.
     */
    private function main()
    {
        $this->initDataSource();
        while ($this->running) {
            try {
                $this->eventDispatcher->dispatch(new QueueStatus($this));
            } catch (\Throwable $e) {
                $this->stdOutLogger->error('Watch coroutine exception, msg:' . $e->getMessage());
                $this->fileLogger->alert('Watch coroutine exception', [
                    'file' => $e->getFile(),
                    'line' => $e->getLine(),
                    'msg' => $e->getMessage(),
                    'trace' => $e->getTraceAsString(),
                ]);
            }

            Coroutine::sleep(self::WATCH_COMMAND_TIME);
        }
        $this->stdOutLogger->info('消费进程（' . $this->process->pid . '）退出');
        $this->fileLogger->info('消费进程（' . $this->process->pid . '）退出');
    }

    /**
     * @param MonitorCycleCompare|MonitorProtocolDetect|MonitorUniversal|MonitorUprushDownrush $model
     */
    private function generateConsumerInfo($model): ?MonitorTaskInfo
    {
        /**
         * @var MonitorTaskInfo $monitorInfo
         */
        $monitorInfo = make(MonitorTaskInfo::class);
        $monitorInfo->setModel($model);
        return $monitorInfo;
    }

    /**
     * @param MonitorTaskInfo $monitorTaskInfo
     * @param $mqConsumerInfo
     */
    private function startCoroutineConsumer($monitorTaskInfo): bool
    {
        $queueType = $monitorTaskInfo->getTaskType();
        $taskId = $monitorTaskInfo->model->id;
        $consumerId = $queueType . '_' . $taskId;
        if (in_array($consumerId, $this->startedTopicIdArr)) {
            $this->fileLogger->info($consumerId . ' has running');
            return true;
        }
        \Hyperf\Utils\Coroutine::create(function () use ($consumerId, $monitorTaskInfo) {
            $cid = \Hyperf\Utils\Coroutine::id();
            $this->topicIdMapToCid[$consumerId][] = $cid;
            try {
                $monitorTaskInfo->setCid($cid);
                /** @var MonitorTask $monitorTask */
                $monitorTask = make(MonitorTask::class);
                $monitorTask->setTaskInfo($monitorTaskInfo);
                $monitorTask->initMonitor();
                $trName = $monitorTaskInfo->getTaskFlag();
                $noticeMessage = '【monitor】The ' . ucfirst($monitorTaskInfo->getTaskFlag()) .
                    " coroutine consumer (cid:{$cid}) of " . $trName .
                    " --(hostname:{$monitorTaskInfo->getHostname()}) start success";
                $this->fileLogger->info($noticeMessage);
                $this->stdOutLogger->info($noticeMessage);
                //Save topic->id to global var
                $startTaskId = $monitorTaskInfo->getTaskType() . '_' . $monitorTaskInfo->model->id;
                $this->startedTopicIdArr[$startTaskId] = $startTaskId;
                $this->mainConsumerObjArr[$cid] = $monitorTask;
                $this->mainConsumerObjArr[$cid]->run();
            } catch (\Throwable $e) {
                $msg = 'The ' . ucfirst($monitorTaskInfo->getTaskFlag()) . ' coroutine consumer（' . $cid . '）异常退出';
                $this->stdOutLogger->error($msg . $e->getMessage() . ' file :' . $e->getFile() . ' line:' . $e->getLine());
                $this->fileLogger->error($msg, [
                    'file' => $e->getFile(),
                    'line' => $e->getLine(),
                    'msg' => $e->getMessage(),
                    'trace' => $e->getTraceAsString(),
                ]);
            }
        });
        return true;
    }
}
