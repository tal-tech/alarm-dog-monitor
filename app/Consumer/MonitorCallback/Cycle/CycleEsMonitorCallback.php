<?php

declare(strict_types=1);

namespace App\Consumer\MonitorCallback\Cycle;

use App\Common\CustomerCommon;
use App\Consumer\MonitorCallback\ConsumerCallback;
use App\Consumer\MonitorTask\MonitorTask;
use App\Model\Datasource;
use Hyperf\Utils\Coroutine;

class CycleEsMonitorCallback extends ConsumerCallback
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
            $this->alarm($msgs, $options);
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
     * @param $options
     * @return bool
     */
    private function alarm($msgs, $options)
    {
        Coroutine::create(function () use ($msgs, $options) {
            $this->stdOutLogger->info('cycle es callback msg:' . json_encode($msgs));
            $monitorTaskInfo = $this->getMonitorInfo();
            $taskModel = $monitorTaskInfo->getModel();
            $datasource = $monitorTaskInfo->getDataSource();
            $alarmCondition = empty(${$taskModel}->alarm_condition) ? [] : json_decode($taskModel->alarm_condition, true);
            if (isset($alarmCondition['conditions'])) {
                $alarmCondition = $alarmCondition['conditions'];
            }
            //查询老数据做对比
            $previousSecondToDb = $options['previousSecondToDb'];
            $currentSecondToDb = $options['currentSecondToDb'];
            $this->stdOutLogger->info('To get previous data to mysql current time:' . $currentSecondToDb . ' date:' . date('Y-m-d H:i:s', $currentSecondToDb) . ' , prev time:' . $previousSecondToDb . ' date:' . date('Y-m-d H:i:s', $previousSecondToDb));
            $oldDataArr = $this->getMonitorData(MonitorTask::MONITORCYCLE, $previousSecondToDb);
            $alarmRuleId = '';
            $this->stdOutLogger->info('cycle es alarm condition:' . json_encode($alarmCondition));
            $filterConfig = empty($taskModel->config) ? [] : json_decode($taskModel->config, true);
            if (isset($filterConfig['filter']['conditions'])) {
                $filterConfig = $filterConfig['filter']['conditions'];
            }

            foreach ($alarmCondition as $key => $arr) {
                $judgeResult = CustomerCommon::validate($msgs, $arr['rule'], ['oldData' => $oldDataArr]);
                if ($judgeResult) {
                    $alarmRuleId = $arr['id'];
                    $msgs = [
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
                    $this->sendAlarm($msgs, $arr['level']);
                    break;
                }
                $alarmRuleId = '';
            }
            $fields = json_encode($msgs);
            $this->insertRecord(MonitorTask::MONITORCYCLE, $alarmRuleId, $fields, $currentSecondToDb);
        });
    }
}
