<?php

declare(strict_types=1);

namespace App\Consumer\MonitorInfo;

use App\Consumer\MonitorTask\MonitorTask;
use App\Model\MonitorCycleCompare;
use App\Model\MonitorProtocolDetect;
use App\Model\MonitorUniversal;
use App\Model\MonitorUprushDownrush;
use Hyperf\Contract\StdoutLoggerInterface;
use Hyperf\Logger\LoggerFactory;
use Psr\Container\ContainerInterface;

class MonitorTaskInfo
{
    /**
     * @var MonitorCycleCompare|MonitorProtocolDetect|MonitorUniversal|MonitorUprushDownrush
     */
    public $model;

    /**
     * coroutine id.
     * @var int
     */
    public $cid;

    /**
     * @var ContainerInterface
     */
    private $container;

    /**
     * @var StdoutLoggerInterface
     */
    private $stdOutLogger;

    /**
     * @var \Psr\Log\LoggerInterface
     */
    private $logger;

    /**
     * MonitorTaskInfo constructor.
     */
    public function __construct(ContainerInterface $container, LoggerFactory $loggerFactory, stdoutLoggerInterface $stdOutlogger)
    {
        $this->stdOutLogger = $stdOutlogger;
        $this->logger = $loggerFactory->get('monitor-data-source', 'default');
        $this->container = $container;
    }

    public function getCid(): int
    {
        return $this->cid;
    }

    public function setCid(int $cid): void
    {
        $this->cid = $cid;
    }

    /**
     * get hostname.
     * @return false|string
     */
    public function getHostname()
    {
        return gethostname();
    }

    /**
     * @return string
     */
    public function getTaskType()
    {
        return $this->model->queue_type;
    }

    /**
     * @return string
     */
    public function getTaskFlag()
    {
        $flag = '';
        $taskType = $this->getTaskType();
        switch ($taskType) {
            case MonitorTask::UNIVERSAL_TASK:
            case MonitorTask::CYCLE_TASK:
            case MonitorTask::UPDOWN_TASK:
                $ds = $this->getDataSource();
                $flag = $this->getTaskType() . '-' . MonitorTask::$dataSourceType[$ds['type']] . '-' . $this->model->id;
                break;
            case MonitorTask::PROTOCOL_TASK:
                $flag = $this->getTaskType() . '-' . $this->model->id;
                break;
        }
        return $flag;
    }

    /**
     * @return array
     */
    public function getDataSource()
    {
        return $this->model->getDataSource();
    }

    /**
     * @return MonitorCycleCompare|MonitorProtocolDetect|MonitorUniversal|MonitorUprushDownrush
     */
    public function getModel()
    {
        return $this->model;
    }

    /**
     * @param MonitorCycleCompare|MonitorProtocolDetect|MonitorUniversal|MonitorUprushDownrush $model
     */
    public function setModel($model): void
    {
        $this->model = $model;
    }
}
