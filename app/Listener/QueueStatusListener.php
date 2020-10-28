<?php

declare(strict_types=1);

namespace App\Listener;

use App\Consumer\MonitorConsumer;
use App\Consumer\MonitorTask\MonitorTask;
use App\Events\QueueStatus;
use App\Model\Model;
use App\Model\MonitorCycleCompare;
use App\Model\MonitorProtocolDetect;
use App\Model\MonitorUniversal;
use App\Model\MonitorUprushDownrush;
use Hyperf\Contract\StdoutLoggerInterface;
use Hyperf\Event\Annotation\Listener;
use Hyperf\Event\Contract\ListenerInterface;
use Hyperf\Logger\LoggerFactory;
use Psr\Container\ContainerInterface;

/**
 * @Listener
 */
class QueueStatusListener implements ListenerInterface
{
    /**
     * @var ContainerInterface
     */
    private $container;

    /**
     * @var array
     */
    private $operationList = [];

    /**
     * @var MonitorConsumer
     */
    private $consumerProcess;

    /**
     * @var StdoutLoggerInterface
     */
    private $stdOutLogger;

    /**
     * @var \Psr\Log\LoggerInterface
     */
    private $logger;

    public function __construct(ContainerInterface $container, LoggerFactory $loggerFactory, stdoutLoggerInterface $stdOutlogger)
    {
        $this->stdOutLogger = $stdOutlogger;
        $this->logger = $loggerFactory->get('monitor-listen-status', 'default');
        $this->container = $container;
    }

    public function listen(): array
    {
        return [
            QueueStatus::class,
        ];
    }

    public function process(object $event)
    {
        $this->consumerProcess = $event->consumerProcess;
        $this->operationList = [];
        $this->operationList['protocol']['start'] = [];
        $this->operationList['protocol']['stop'] = [];
        $this->operationList['protocol']['restart'] = [];

        $this->operationList['universal']['start'] = [];
        $this->operationList['universal']['stop'] = [];
        $this->operationList['universal']['restart'] = [];

        $this->operationList['updown']['start'] = [];
        $this->operationList['updown']['stop'] = [];
        $this->operationList['updown']['restart'] = [];

        $this->operationList['cycle']['start'] = [];
        $this->operationList['cycle']['stop'] = [];
        $this->operationList['cycle']['restart'] = [];
        $this->generatorOperationlist('universal', MonitorUniversal::class);
        $this->generatorOperationlist('updown', MonitorUprushDownrush::class);
        $this->generatorOperationlist('cycle', MonitorCycleCompare::class);
        $this->generatorOperationlist('protocol', MonitorProtocolDetect::class);

        foreach ($this->operationList as $taskType => $taskArr) {
            array_walk($this->operationList[$taskType]['start'], function (&$item, $key, $pre) {
                $item = $pre . $item;
            }, $taskType . '_');

            array_walk($this->operationList[$taskType]['stop'], function (&$item, $key, $pre) {
                $item = $pre . $item;
            }, $taskType . '_');

            array_walk($this->operationList[$taskType]['restart'], function (&$item, $key, $pre) {
                $item = $pre . $item;
            }, $taskType . '_');
        }
        $this->startCommand();
        $this->stopCommand();
        $this->restart();
    }

    /**
     * @param $taskType
     * @param $class Model|string
     */
    private function generatorOperationlist($taskType, $class)
    {
        try {
            //开启的topic
            $kfQuery = $class::query()
                ->whereIn('status', [
                    MonitorTask::TOPIC_STARTING,
                    MonitorTask::TOPIC_STARTED,
                    MonitorTask::TOPIC_STOPING,
                    MonitorTask::TOPIC_STOPED,
                    MonitorTask::TOPIC_EDITED,
                ])
                ->select();
            $startTopicList = $kfQuery->get()->toArray();

            foreach ($startTopicList as $tlKey => $tlValue) {
                switch ($tlValue['status']) {
                    case MonitorTask::TOPIC_STARTING:
                    case MonitorTask::TOPIC_STARTED:
                        $this->operationList[$taskType]['start'][$tlValue['id']] = $tlValue['id'];
                        break;
                    case MonitorTask::TOPIC_STOPING:
                    case MonitorTask::TOPIC_STOPED:
                        $this->operationList[$taskType]['stop'][$tlValue['id']] = $tlValue['id'];
                        break;
                    case MonitorTask::TOPIC_EDITED:
                        $this->operationList[$taskType]['restart'][$tlValue['id']] = $tlValue['id'];
                        break;
                }
            }
        } catch (\Throwable $e) {
            $msg = $e->getMessage() . '====' . $e->getTraceAsString();
            $this->stdOutLogger->error($msg);
            $this->logger->error($msg);
        }
    }

    /**
     * @param $status
     * @return array
     */
    private function getAllTaskIdByStatus($status)
    {
        $taskIdArr = [];
        foreach ($this->operationList as $taskType => $taskArr) {
            if (array_key_exists($status, $taskArr)) {
                $taskIdArr = array_merge($taskIdArr, $taskArr[$status]);
            }
        }
        return $taskIdArr;
    }

    /**
     * start queue consumer.
     */
    private function startCommand()
    {
        $startArr = $this->getAllTaskIdByStatus('start');
        //检查是否startArr中有没有待启动的消费者, 取差集
        $difference = array_diff($startArr, $this->consumerProcess->startedTopicIdArr);
        if (! empty($difference)) {
            foreach ($difference as $val) {
                [$taskType, $taskId] = explode('_', $val);
                /**
                 * @var Model $model
                 */
                $model = MonitorTask::$taskMapModel[$taskType];
                $run = $model::query()->where('id', $taskId)->first();
                $run->queue_type = $taskType;
                $this->consumerProcess->gotoStart($run);
            }
        }
    }

    /**
     * stop queue consumer.
     */
    private function stopCommand()
    {
        $stopArr = $this->getAllTaskIdByStatus('stop');
        //检查是否stopArr中有没有待停止的消费者, 取交集
        $difference = array_intersect($stopArr, $this->consumerProcess->startedTopicIdArr);
        if (! empty($difference)) {
            foreach ($difference as $val) {
                $this->consumerProcess->stopConsumer($val);
            }
        }
    }

    /**
     * restart queue consumer.
     */
    private function restart()
    {
        $stopArr = $this->getAllTaskIdByStatus('restart');
        if (! empty($stopArr)) {
            //每次都停止下状态为6重启中的服务，指到等到所有容器中的服务都停止完成
            foreach ($stopArr as $val) {
                $this->consumerProcess->stopConsumer($val, 0, 1);
            }
        }
    }
}
