<?php

declare(strict_types=1);

namespace App\Listener;

use App\Consumer\MonitorInfo\MonitorTaskInfo;
use App\Consumer\MonitorTask\MonitorTask;
use App\Events\QueueStarted;
use App\Model\Model;
use Hyperf\Database\Events\QueryExecuted;
use Hyperf\Event\Annotation\Listener;
use Hyperf\Event\Contract\ListenerInterface;
use Hyperf\Logger\LoggerFactory;
use Psr\Container\ContainerInterface;
use Psr\Log\LoggerInterface;

/**
 * @Listener
 */
class QueueStartedListener implements ListenerInterface
{
    /**
     * @var LoggerInterface
     */
    private $logger;

    public function __construct(ContainerInterface $container)
    {
        $this->logger = $container->get(LoggerFactory::class)->get('queue', 'default');
    }

    public function listen(): array
    {
        return [
            QueueStarted::class,
        ];
    }

    /**
     * @param QueryExecuted $event
     */
    public function process(object $event)
    {
        if ($event instanceof QueueStarted) {
            /** @var MonitorTaskInfo $mqConsumerInfo */
            $monitorTaskInfo = $event->getMonitorTaskInfo();
            $taskType = $monitorTaskInfo->getTaskType();
            /**
             * @var Model $modelClass
             */
            $modelClass = MonitorTask::$taskMapModel[$taskType];
            $modelClass::query()
                ->where('id', $monitorTaskInfo->model->id)
                ->update([
                    'status' => MonitorTask::TOPIC_STARTED,
                    'updated_at' => time(),
                    'started_at' => time(),
                ]);
        }
    }
}
