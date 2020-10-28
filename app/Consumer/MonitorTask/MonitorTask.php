<?php

declare(strict_types=1);

namespace App\Consumer\MonitorTask;

use App\Common\ConsumerStopWaitGroup;
use App\Consumer\DataSources\Cycle\CycleEsDataSource;
use App\Consumer\DataSources\Cycle\CycleKafkaDataSource;
use App\Consumer\DataSources\Cycle\CycleMysqlDataSource;
use App\Consumer\DataSources\EsDataSource;
use App\Consumer\DataSources\IDataSource;
use App\Consumer\DataSources\KafkaDataSource;
use App\Consumer\DataSources\MysqlDataSource;
use App\Consumer\DataSources\ProtocolDataSource;
use App\Consumer\DataSources\WebHookDataSource;
use App\Consumer\MonitorInfo\MonitorTaskInfo;
use App\Exception\Handler\ConsumerStopException;
use App\Model\MonitorCycleCompare;
use App\Model\MonitorProtocolDetect;
use App\Model\MonitorUniversal;
use App\Model\MonitorUprushDownrush;
use Hyperf\Contract\StdoutLoggerInterface;
use Hyperf\Logger\LoggerFactory;
use Psr\Container\ContainerInterface;

class MonitorTask
{
    const TOPIC_STARTING = 1;

    const TOPIC_STARTED = 2;

    const TOPIC_STOPING = 3;

    const TOPIC_STOPED = 4;

    const TOPIC_DELETED = 5;

    const TOPIC_EDITED = 6;

    const DATASOURCE_ES = 1;

    const DATASOURCE_MYSQL = 2;

    const DATASOURCE_KAFKA = 3;

    const DATASOURCE_WEBHOOK = 4;

    const MONITORUNIVERSAL = 1;

    const MONITORCYCLE = 2;

    const MONITORUPDOWN = 3;

    const MONITORPROTOCOL = 4;

    const UNIVERSAL_TASK = 'universal';

    const UPDOWN_TASK = 'updown';

    const CYCLE_TASK = 'cycle';

    const PROTOCOL_TASK = 'protocol';

    //阈值类型（threshold_type）
    const MONITOR_COMPARE_THRESHOLD = 'threshold';

    const MONITOR_COMPARE_PERCENT = 'percent';

    /**
     * @var array
     */
    public static $taskMapModel = [
        self::UNIVERSAL_TASK => MonitorUniversal::class,
        self::UPDOWN_TASK => MonitorUprushDownrush::class,
        self::CYCLE_TASK => MonitorCycleCompare::class,
        self::PROTOCOL_TASK => MonitorProtocolDetect::class,
    ];

    /**
     * @var array
     */
    public static $dataSourceType = [
        self::DATASOURCE_ES => 'es',
        self::DATASOURCE_MYSQL => 'mysql',
        self::DATASOURCE_KAFKA => 'kafka',
        self::DATASOURCE_WEBHOOK => 'webhook',
    ];

    /**
     * @var StdoutLoggerInterface
     */
    protected $stdOutLogger;

    /**
     * @var StdoutLoggerInterface
     */
    protected $logger;

    /**
     * @var MonitorTaskInfo
     */
    protected $monitorInfo;

    /**
     * @var IDataSource
     */
    private $dataSource;

    /**
     * @var ConsumerStopWaitGroup
     */
    private $consumerStopWaitGroup;

    /**
     * KafkaConsumerInstance constructor.
     * @param MonitorTaskInfo $taskInfo
     */
    public function __construct(
        ContainerInterface $container,
        LoggerFactory $loggerFactory,
        StdoutLoggerInterface $stdOutlogger
    ) {
        $this->logger = $loggerFactory->get('kafka_consumer_instance', 'default');
        $this->stdOutLogger = $stdOutlogger;
        $this->consumerStopWaitGroup = $container->get(ConsumerStopWaitGroup::class);
    }

    public function setTaskInfo(MonitorTaskInfo $monitorTaskInfo)
    {
        $this->monitorInfo = $monitorTaskInfo;
    }

    /**
     * @param array $topic
     * @throws \ReflectionException
     */
    public function initMonitor()
    {
        $taskType = $this->monitorInfo->getTaskType();
        switch ($taskType) {
            case self::UNIVERSAL_TASK:
                $this->universalTask();
                break;
            case self::PROTOCOL_TASK:
                $this->protocolTask();
                break;
            case self::CYCLE_TASK:
                $this->cycleTask();
                break;
        }
    }

    /**
     * run.
     */
    public function run()
    {
        try {
            $this->dataSource->start();
            //消费者停止后标识自己运行结束
            $this->consumerStop();
        } catch (ConsumerStopException $e) {
            $msg = $e->getMessage() . '---' . $e->getFile() . '---' . $e->getLine();
            $this->logger->error($msg);
        } catch (\Throwable $e) {
            $msg = $e->getMessage() . '---' . $e->getFile() . '---' . $e->getLine();
            $this->logger->error($msg);
        }
    }

    /**
     * stoptopic.
     */
    public function stop()
    {
        $this->dataSource->stop();
    }

    /**
     * 消费者停止被动回调通知.
     */
    public function consumerStop()
    {
        $trName = $this->monitorInfo->getTaskFlag();
        $msg = 'The coroutine consumer (cid:' . $this->monitorInfo->getCid() . ') of ' . $trName . " --(hostname:{$this->monitorInfo->getHostname()}) stop success!";
        $this->stdOutLogger->info($msg);
        $this->logger->info($msg);
        $this->consumerStopWaitGroup->done();
    }

    /**
     * 通用监控任务
     * @throws \ReflectionException
     */
    private function universalTask()
    {
        /**
         * @var EsDataSource|KafkaDataSource|MysqlDataSource|WebHookDataSource $dataSourceObj
         */
        $dataSourceObj = null;
        $dataSource = $this->monitorInfo->getDataSource();
        if (! empty($dataSource)) {
            switch ($dataSource['type']) {
                case MonitorTask::DATASOURCE_ES:
                    $dataSourceObj = make(EsDataSource::class);
                    break;
                case MonitorTask::DATASOURCE_MYSQL:
                    $dataSourceObj = make(MysqlDataSource::class);
                    break;
                case MonitorTask::DATASOURCE_KAFKA:
                    $dataSourceObj = make(KafkaDataSource::class);
                    break;
                case MonitorTask::DATASOURCE_WEBHOOK:
                    $dataSourceObj = make(WebHookDataSource::class);
                    break;
            }
            $dataSourceObj->setMonitorInfo($this->monitorInfo);
            $dataSourceObj->init();
            $this->dataSource = $dataSourceObj;
        }
    }

    /**
     * config protocol task.
     */
    private function protocolTask()
    {
        /** @var ProtocolDataSource $dataSourceObj */
        $dataSourceObj = make(ProtocolDataSource::class);
        $dataSourceObj->setMonitorInfo($this->monitorInfo);
        $dataSourceObj->init();
        $this->dataSource = $dataSourceObj;
    }

    /**
     * cycle task.
     */
    private function cycleTask()
    {
        /**
         * @var CycleEsDataSource|CycleKafkaDataSource|CycleMysqlDataSource|WebHookDataSource $dataSourceObj
         */
        $dataSourceObj = null;
        $dataSource = $this->monitorInfo->getDataSource();
        if (! empty($dataSource)) {
            switch ($dataSource['type']) {
                case MonitorTask::DATASOURCE_ES:
                    $dataSourceObj = make(CycleEsDataSource::class);
                    break;
                case MonitorTask::DATASOURCE_MYSQL:
                    $dataSourceObj = make(CycleMysqlDataSource::class);
                    break;
                case MonitorTask::DATASOURCE_KAFKA:
                    $dataSourceObj = make(CycleKafkaDataSource::class);
                    break;
                case MonitorTask::DATASOURCE_WEBHOOK:
                    $dataSourceObj = make(WebHookDataSource::class);
                    break;
            }
            $dataSourceObj->setMonitorInfo($this->monitorInfo);
            $dataSourceObj->init();
            $this->dataSource = $dataSourceObj;
        }
    }
}
