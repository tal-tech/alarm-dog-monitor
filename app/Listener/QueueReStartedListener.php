<?php

declare(strict_types=1);

namespace App\Listener;

use App\Consumer\MonitorInfo\MonitorTaskInfo;
use App\Consumer\MonitorTask\MonitorTask;
use App\Events\QueueReStarted;
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
class QueueReStartedListener implements ListenerInterface
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
            QueueReStarted::class,
        ];
    }

    /**
     * @param QueryExecuted $event
     */
    public function process(object $event)
    {
        if ($event instanceof QueueReStarted) {
            /** @var MonitorTaskInfo $mqConsumerInfo */
            $taskType = $event->getTaskType();
            $taskId = $event->getTaskId();
            /**
             * @var Model $modelClass
             */
            /**
             * @var Model $model
             */
            $model = MonitorTask::$taskMapModel[$taskType];
            $model::query()
                ->where('id', $taskId)->update([
                    'status' => MonitorTask::TOPIC_STARTING,
                    'updated_at' => time(),
                ]);
        }
    }
}
