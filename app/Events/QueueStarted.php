<?php

declare(strict_types=1);

namespace App\Events;

use App\Consumer\MonitorInfo\MonitorTaskInfo;

class QueueStarted
{
    /**
     * @var MonitorTaskInfo
     */
    private $monitorTaskInfo;

    /**
     * QueueStarted constructor.
     * @param $monitorTaskInfo
     */
    public function __construct($monitorTaskInfo)
    {
        $this->monitorTaskInfo = $monitorTaskInfo;
    }

    /**
     * @return MonitorTaskInfo
     */
    public function getMonitorTaskInfo()
    {
        return $this->monitorTaskInfo;
    }
}
