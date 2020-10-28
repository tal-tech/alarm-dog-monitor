<?php

declare(strict_types=1);

namespace App\Consumer\MonitorCallback\Cycle;

use App\Common\CustomerCommon;
use App\Consumer\DataSources\TraitTriggerCycle;
use App\Consumer\MonitorCallback\ConsumerCallback;
use App\Consumer\MonitorTask\MonitorTask;
use App\Model\Datasource;
use Hyperf\Utils\Coroutine;
use XesMq\Mq\Business\Callback;
use XesMq\Mq\Consumer;

class CycleKafkaMonitorCallback extends ConsumerCallback implements Callback
{
    use TraitTriggerCycle;

    /**
     * implements this method to handle message from kafka.
     *
     * @param $msgs
     * @param array $options
     * @return array
     */
    public function handle($msgs, $options = [])
    {
        $offsets = [];
        $tick = null;
        foreach ($msgs as $item) {
            try {
                $msg = json_decode($item['payload'], true);
                if (! empty($msg)) {
                    $this->filter($msg, $options);
                }
                Consumer::markSuccess($offsets, $item['queue'], $item['props']['partition'], $item['props']['offset']);
            } catch (\Throwable $e) {
                $this->operationMsg($e);
            }
        }
        return $offsets;
    }

    /**
     * 告警收敛.
     * @param $msg
     * @param mixed $options
     * @return bool
     */
    public function filter($msg, $options = [])
    {
        $this->stdOutLogger->info('cycle kafka callback msg:' . json_encode($msg));
        $monitorTaskInfo = $this->getMonitorInfo();
        $filterConfig = empty($monitorTaskInfo->getModel()->config) ? [] : json_decode($monitorTaskInfo->getModel()->config, true);
        if (isset($filterConfig['filter']['conditions'])) {
            $filterConfig = $filterConfig['filter']['conditions'];
        }
        foreach ($filterConfig as $key => $arr) {
            $judgeResult = CustomerCommon::validate($msg, $arr['rule']);
            if ($judgeResult) {
                $this->alarm($msg, $options);
                return true;
            }
        }
        return false;
    }

    /**
     * @param $msgs
     * @param array $options
     * @return bool
     */
    private function alarm($msgs, $options = [])
    {
        Coroutine::create(function () use ($msgs, $options) {
            $monitorTaskInfo = $this->getMonitorInfo();
            $taskModel = $monitorTaskInfo->getModel();
            $aggCycle = $taskModel->agg_cycle;
            $datasource = $monitorTaskInfo->getDataSource();
            $timestampField = $datasource['timestamp_field'];
            $timestampUnit = $datasource['timestamp_unit'];
            $alarmCondition = empty($taskModel->alarm_condition) ? [] : json_decode($taskModel->alarm_condition, true);
            if (isset($alarmCondition['conditions'])) {
                $alarmCondition = $alarmCondition['conditions'];
            }
            $filterConfig = empty($taskModel->config) ? [] : json_decode($taskModel->config, true);
            if (isset($filterConfig['filter']['conditions'])) {
                $filterConfig = $filterConfig['filter']['conditions'];
            }
            $this->stdOutLogger->info('cycle kafka alarm condition:' . json_encode($alarmCondition));
            $timestampFieldValue = $msgs[$timestampField];
            switch ($timestampUnit) {
                case 2:
                    $timestampFieldValue = round($timestampFieldValue / 1000);
                    break;
                case 3:
                    $timestampFieldValue = round($timestampFieldValue / 1000000);
                    break;
                case 4:
                    $timestampFieldValue = strtotime($timestampFieldValue);
                    break;
            }
            $this->stdOutLogger->info('==============================timestamp' . $timestampFieldValue . '===========date' . date('Y-m-d H:i:s', $timestampFieldValue));
            //如果当前获取数据的时间小于两个时间周期则抛弃该数据
            if ((time() - $timestampFieldValue) > 2 * $aggCycle) {
                return true;
            }
            $calcTime = $this->calcMinuteTriggerCycle($timestampFieldValue);
            $previousSecondToDb = $calcTime['previousSecondToDb'];
            $currentSecondToDb = $calcTime['currentSecondToDb'];
            //查询老数据做对比
            $this->stdOutLogger->info('To get previous data to mysql current time:' . $currentSecondToDb . ' date:' . date('Y-m-d H:i:s', $currentSecondToDb) . ' , prev time:' . $previousSecondToDb . ' date:' . date('Y-m-d H:i:s', $previousSecondToDb));
            $oldDataArr = $this->getMonitorData(MonitorTask::MONITORCYCLE, $previousSecondToDb);
            $alarmRuleId = '';
            foreach ($alarmCondition as $key => $arr) {
                $judgeResult = CustomerCommon::validate($msgs, $arr['rule'], ['oldData' => $oldDataArr]);
                if ($judgeResult) {
                    $sendMsgs = [
                        'source' => $msgs,
                        'task' => [
                            'id' => $taskModel->id,
                            'name' => $taskModel->name,
                            'remark' => $taskModel->remark,
                            'agg_cycle' => $taskModel->agg_cycle,
                            'compare_cycle' => $taskModel->compare_cycle,
                        ],
                        'config' => [
                            'type' => 'cycle_compare',
                            'type_text' => Datasource::$monitors['cycle_compare'],
                            'datasource' => [
                                'id' => $datasource['id'],
                                'name' => $datasource['name'],
                                'type' => $datasource['type'],
                                'type_text' => Datasource::$types[$datasource['type']] ?? 'UNKNOWN',
                            ],
                            'filter_condition' => $filterConfig,
                            'alarm_condition' => $alarmCondition,
                        ],
                        'hit' => $arr,
                    ];
                    $alarmRuleId = $arr['id'];
                    $this->sendAlarm($sendMsgs, $arr['level']);
                    break;
                }
            }

            $fields = json_encode($msgs);
            $this->insertRecord(MonitorTask::MONITORCYCLE, $alarmRuleId, $fields, $currentSecondToDb);
        });
    }
}
