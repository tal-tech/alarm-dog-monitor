<?php

declare(strict_types=1);

namespace App\Listener;

use App\Consumer\MonitorInfo\MonitorTaskInfo;
use App\Consumer\MonitorTask\MonitorTask;
use App\Events\QueueStoped;
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
class QueueStopedListener implements ListenerInterface
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
            QueueStoped::class,
        ];
    }

    /**
     * @param QueryExecuted $event
     */
    public function process(object $event)
    {
        if ($event instanceof QueueStoped) {
            /** @var MonitorTaskInfo $mqConsumerInfo */
            $monitorTaskInfo = $event->getMonitorTaskInfo();
            $flag = $event->getFlag();
            /*
             * 0代表正常的停止服务
             * 1代表重启服务
             */
            if ($flag == 0) {
                $taskType = $monitorTaskInfo->getTaskType();
                /**
                 * @var Model $modelClass
                 */
                $modelClass = MonitorTask::$taskMapModel[$taskType];
                $modelClass::query()
                    ->where('id', $monitorTaskInfo->model->id)
                    ->update([
                        'status' => MonitorTask::TOPIC_STOPED,
                        'updated_at' => time(),
                    ]);
            }
        }
    }
}
