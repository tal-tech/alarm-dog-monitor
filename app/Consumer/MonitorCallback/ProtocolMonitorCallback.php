<?php

declare(strict_types=1);

namespace App\Consumer\MonitorCallback;

use App\Common\CustomerCommon;
use Hyperf\Utils\Coroutine;

class ProtocolMonitorCallback extends ConsumerCallback
{
    /**
     * implements this method to handle message from kafka.
     *
     * @param $msgs
     * @param array $options
     */
    public function handle($msgs, $options = [])
    {
        try {
            $this->alarm($msgs);
        } catch (\Throwable $e) {
            $this->operationMsg($e);
        }
    }

    /**
     * 告警收敛.
     * @param $msg
     * @param mixed $msgs
     * @return bool
     */
    public function filter($msgs)
    {
    }

    /**
     * @param $msgs
     * @return bool
     */
    private function alarm($msgs)
    {
        Coroutine::create(function () use ($msgs) {
            $monitorTaskInfo = $this->getMonitorInfo();
            $alarmCondition = empty($monitorTaskInfo->getModel()->alarm_condition) ? [] : json_decode($monitorTaskInfo->getModel()->alarm_condition, true);
            if (isset($alarmCondition['conditions'])) {
                $alarmCondition = $alarmCondition['conditions'];
            }
            $this->stdOutLogger->info('protocol detect alarm condition:' . json_encode($alarmCondition));
            $this->consumerCallbackLogger->info('protocol detect alarm condition:' . json_encode($alarmCondition));
            foreach ($alarmCondition as $key => $arr) {
                $judgeResult = CustomerCommon::validate($msgs, $arr['rule']);
                if ($judgeResult) {
                    $this->sendAlarm($msgs, $arr['level']);
                    return true;
                }
            }
        });
    }
}
