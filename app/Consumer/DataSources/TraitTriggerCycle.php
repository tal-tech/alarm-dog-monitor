<?php

declare(strict_types=1);

namespace App\Consumer\DataSources;

use App\Consumer\MonitorInfo\MonitorTaskInfo;

trait TraitTriggerCycle
{
    /**
     * @param null|mixed $currentTime
     * @return array
     */
    public function calcMinuteTriggerCycle($currentTime = null)
    {
        if (empty($currentTime)) {
            $current = (int) strtotime(date('Y-m-d H:i:s'));
        } else {
            $current = $currentTime;
        }
        /** @var MonitorTaskInfo $monitorTaskInfo */
        $monitorTaskInfo = $this->getMonitorInfo();
        $aggCycle = $monitorTaskInfo->getModel()->agg_cycle;
        $compareCycle = isset($monitorTaskInfo->getModel()->compare_cycle) ? $monitorTaskInfo->getModel()->compare_cycle : 0;
        $remainder = $current % $aggCycle;
        $this->stdOutLogger->info('【monitor】Task ' . $monitorTaskInfo->getTaskFlag() . ' current date ' . date('Y-m-d H:i:s', $current));
        if ($remainder > round($aggCycle / 2)) {
            $start = $current + ($aggCycle - $remainder);
            $this->stdOutLogger->info('【monitor】Task ' . $monitorTaskInfo->getTaskFlag() . ' agg cycle up ' . $start . ', date，' . date('Y-m-d H:i:s', $start));
        } else {
            $start = $current - $remainder;
            $this->stdOutLogger->info('【monitor】Task ' . $monitorTaskInfo->getTaskFlag() . ' agg cycle down ' . $start . ', date，' . date('Y-m-d H:i:s', $start));
        }
        $end = (int) $start + (int) $aggCycle;
        $data = [
            'start' => $start,
            'end' => $end,
            'tosleep' => (int) abs(($end - $current)),
            'previousSecondToDb' => (int) $end - (int) $compareCycle,
            'currentSecondToDb' => (int) $end,
        ];
        foreach ($data as $key => $value) {
            $this->stdOutLogger->info('【monitor】Task ' . $monitorTaskInfo->getTaskFlag() . ' time ' . $key . ' field time value ' . $value . ', date value ' . date('Y-m-d H:i:s', $value));
        }
        return $data;
    }

    /**
     * @return array bak
     */
    public function calcMinuteTriggerCycle1()
    {
        /** @var MonitorTaskInfo $monitorTaskInfo */
        $monitorTaskInfo = $this->getMonitorInfo();
        $aggCycle = $monitorTaskInfo->getModel()->agg_cycle;
        $compareCycle = $monitorTaskInfo->getModel()->compare_cycle;
        //transform to minuter
        $aggCycleMinute = $aggCycle / 60;
        $this->stdOutLogger->info('【monitor】Task ' . $monitorTaskInfo->getTaskFlag() . ' agg cycle ' . $aggCycleMinute);
        //get current minute
        $currentMinute = date('i');
        $current = (int) strtotime(date('Y-m-d H:i:s'));
        $start = (int) strtotime(date('Y-m-d H:i'));
        $remainder = $currentMinute % $aggCycleMinute;
        if ($remainder > 0) {
            $diff = $aggCycleMinute - $remainder;
            $this->stdOutLogger->info('【monitor】' . $monitorTaskInfo->getTaskFlag() . 'calcMinuteTriggerCycle calc cycle, diff ' . $diff);
            $start = $start + $diff * 60;
            $this->stdOutLogger->info('【monitor】' . $monitorTaskInfo->getTaskFlag() . 'calcMinuteTriggerCycle calc start cycle, actualy start at ' . $start . ' date' . date('Y-m-d H:i:s', $start));
        }
        //agg end minute
        $end = (int) $start + (int) $aggCycle;
        return [
            'start' => $start,
            'end' => $end,
            'tosleep' => $end - $current,
            'previousSecondToDb' => (int) $start + 60 - (int) $compareCycle,
            'currentSecondToDb' => (int) $start + (int) $aggCycle,
        ];
    }
}
