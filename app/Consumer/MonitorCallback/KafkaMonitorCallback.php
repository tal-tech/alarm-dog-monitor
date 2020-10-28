<?php

declare(strict_types=1);

namespace App\Consumer\MonitorCallback;

use App\Common\CustomerCommon;
use App\Model\Datasource;
use Hyperf\Utils\ApplicationContext;
use Hyperf\Utils\Coroutine;
use XesMq\Mq\Business\Callback;
use XesMq\Mq\Consumer;

class KafkaMonitorCallback extends ConsumerCallback implements Callback
{
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
                    $this->filter($msg);
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
     * @return bool
     */
    public function filter($msg)
    {
        $this->stdOutLogger->info('universal kafka callback msg:' . json_encode($msg));
        $monitorTaskInfo = $this->getMonitorInfo();
        $filterConfig = empty($monitorTaskInfo->getModel()->config) ? [] : json_decode($monitorTaskInfo->getModel()->config, true);
        if (isset($filterConfig['filter']['conditions'])) {
            $filterConfig = $filterConfig['filter']['conditions'];
        }
        foreach ($filterConfig as $key => $arr) {
            $judgeResult = CustomerCommon::validate($msg, $arr['rule']);
            if ($judgeResult) {
                $this->alarm($msg);
                return true;
            }
        }
        return false;
    }

    /**
     * @param $msg
     * @return bool
     */
    private function alarm($msg)
    {
        Coroutine::create(function () use ($msg) {
            $container = ApplicationContext::getContainer();
            /** @var \Redis $redis */
            $redis = $container->get(\Redis::class);
            $monitorTaskInfo = $this->getMonitorInfo();
            $taskModel = $monitorTaskInfo->getModel();
            $aggCycle = $taskModel->agg_cycle;
            $datasource = $monitorTaskInfo->getDataSource();
            $timestampField = $datasource['timestamp_field'];
            $timestampUnit = $datasource['timestamp_unit'];
            $timestampFieldValue = $msg[$timestampField];
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
            //如果当前获取数据的时间小于两个时间周期则抛弃该数据
            if ((time() - $timestampFieldValue) > 2 * $aggCycle) {
                return true;
            }
            $alarmCondition = empty($taskModel->alarm_condition) ? [] : json_decode($taskModel->alarm_condition, true);
            $this->stdOutLogger->info('universal kafka alarm condition:' . json_encode($alarmCondition));
            if (isset($alarmCondition['conditions'])) {
                $alarmCondition = $alarmCondition['conditions'];
            }
            $filterConfig = empty($taskModel->config) ? [] : json_decode($taskModel->config, true);
            if (isset($filterConfig['filter']['conditions'])) {
                $filterConfig = $filterConfig['filter']['conditions'];
            }
            foreach ($alarmCondition as $key => $arr) {
                $judgeResult = CustomerCommon::validate($msg, $arr['rule']);
                $ruleId = isset($arr['id']) ? $arr['id'] : rand(10000000, 99999999);
                $continuousCycle = isset($arr['continuous_cycle']) ? $arr['continuous_cycle'] : 1;
                if ($judgeResult) {
                    $redisKey = self::$redisPrex . $this->getMonitorInfo()->getTaskFlag();
                    $count = $redis->incr($redisKey);
                    if ($count == 1) {
                        $redis->expire($redisKey, (int) self::$defaultRedisExpireTime);
                    }
                    //计算该策略命中的次数
                    if ($count >= $continuousCycle) {
                        $sendMsgs = [
                            'source' => $msg,
                            'task' => [
                                'id' => $taskModel->id,
                                'name' => $taskModel->name,
                                'remark' => $taskModel->remark,
                                'agg_cycle' => $taskModel->agg_cycle,
                            ],
                            'config' => [
                                'type' => 'universal',
                                'type_text' => Datasource::$monitors['universal'],
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
                        $this->sendAlarm($sendMsgs, $arr['level']);
                        $redis->del($redisKey);
                    }
                    return true;
                }
            }
            $redis = $container->get(\Redis::class);
            $redisKey = self::$redisPrex . $this->getMonitorInfo()->getTaskFlag();
            $redis->exists($redisKey) && $redis->del($redisKey);
        });
    }
}
