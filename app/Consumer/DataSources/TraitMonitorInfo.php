<?php

declare(strict_types=1);

namespace App\Consumer\DataSources;

use App\Consumer\MonitorInfo\MonitorTaskInfo;

trait TraitMonitorInfo
{
    /**
     * @var MonitorTaskInfo
     */
    public $monitorInfo;

    public function getMonitorInfo(): MonitorTaskInfo
    {
        return $this->monitorInfo;
    }

    /**
     * @param MonitorTaskInfo $monitorInfo
     */
    public function setMonitorInfo($monitorInfo): void
    {
        $this->monitorInfo = $monitorInfo;
    }
}
