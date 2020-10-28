<?php

declare(strict_types=1);

namespace App\Events;

class QueueReStarted
{
    /**
     * @var string
     */
    private $taskType;

    /**
     * @var int
     */
    private $taskId;

    /**
     * QueueStarted constructor.
     * @param $taskType
     * @param $taskId
     */
    public function __construct($taskType, $taskId)
    {
        $this->taskType = $taskType;
        $this->taskId = $taskId;
    }

    public function getTaskType(): string
    {
        return $this->taskType;
    }

    public function getTaskId(): int
    {
        return (int) $this->taskId;
    }
}
