<?php

declare(strict_types=1);

namespace App\Events;

use App\Consumer\MonitorInfo\MonitorTaskInfo;

class QueueStoped
{
    /**
     * @var MonitorTaskInfo
     */
    private $monitorTaskInfo;

    private $flag;

    /**
     * QueueStoped constructor.
     * @param $monitorTaskInfo
     * @param $flag
     */
    public function __construct($monitorTaskInfo, $flag)
    {
        $this->monitorTaskInfo = $monitorTaskInfo;
        $this->flag = $flag;
    }

    /**
     * @return MonitorTaskInfo
     */
    public function getMonitorTaskInfo()
    {
        return $this->monitorTaskInfo;
    }

    /**
     * @return mixed
     */
    public function getFlag()
    {
        return $this->flag;
    }
}
