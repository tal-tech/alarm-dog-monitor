<?php

declare(strict_types=1);

namespace App\Consumer\MonitorCallback;

use App\Common\CustomerCommon;
use App\Model\Datasource;
use Hyperf\Utils\ApplicationContext;
use Hyperf\Utils\Coroutine;

class MysqlMonitorCallback extends ConsumerCallback
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
            $container = ApplicationContext::getContainer();
            /** @var \Redis $redis */
            $redis = $container->get(\Redis::class);
            $this->stdOutLogger->info('universal mysql callback msg:' . json_encode($msgs));
            $monitorTaskInfo = $this->getMonitorInfo();
            $taskModel = $monitorTaskInfo->getModel();
            $datasource = $monitorTaskInfo->getDataSource();
            $alarmCondition = empty($taskModel->alarm_condition) ? [] : json_decode($taskModel->alarm_condition, true);
            if (isset($alarmCondition['conditions'])) {
                $alarmCondition = $alarmCondition['conditions'];
            }
            $this->stdOutLogger->info('universal mysql alarm condition:' . json_encode($alarmCondition));
            $filterConfig = empty($taskModel->config) ? [] : json_decode($taskModel->config, true);
            if (isset($filterConfig['filter']['conditions'])) {
                $filterConfig = $filterConfig['filter']['conditions'];
            }

            foreach ($alarmCondition as $key => $arr) {
                //@todo 可以按照同一个字段多中聚合方式过滤
                //$judgeResult = CustomerCommon::validate($msgs, $arr['rule'], 'agg_method');
                $judgeResult = CustomerCommon::validate($msgs, $arr['rule']);
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
                            'source' => $msgs,
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
