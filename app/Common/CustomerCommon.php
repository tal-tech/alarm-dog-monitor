<?php

declare(strict_types=1);

namespace App\Common;

use App\Consumer\MonitorTask\MonitorTask;
use GuzzleHttp\Client;
use Hyperf\Contract\StdoutLoggerInterface;
use Hyperf\Guzzle\HandlerStackFactory;
use Hyperf\Guzzle\RetryMiddleware;
use Hyperf\Logger\LoggerFactory;
use Hyperf\Utils\ApplicationContext;
use XesMq\Mq\Producer;

class CustomerCommon
{
    const XTQNOFIELD = 'xtq-no-field';

    public static $webHookPool = [];

    /**
     * @var \Psr\Log\LoggerInterface
     */
    protected $logger;

    public function __construct(LoggerFactory $loggerFactory)
    {
        $this->logger = $loggerFactory->get('consumer_common', 'default');
    }

    /**
     * 生成签名.
     *
     * @param $secret
     * @param $data
     * @return string
     */
    public function getSign($secret, $data)
    {
        // 对数组的值按key排序
        ksort($data);
        // 生成url的形式  a=1&b=2&c=3
        $params = http_build_query($data);
        // 生成sign
        return md5($params . $secret);
    }

    /**
     * Retry an operation a given number of times.
     *
     * @param $times
     * @param int $sleep
     * @throws \Throwable
     * @return mixed
     */
    public static function retry($times, callable $callback, $sleep = 0)
    {
        beginning:
        try {
            return $callback();
        } catch (\Throwable $e) {
            if (--$times < 0) {
                throw $e;
            }
            if ($sleep) {
                usleep($sleep * 1000);
            }
            goto beginning;
        }
    }

    /**
     * @param $target
     * @param null $key
     * @param null $default
     * @return null|mixed
     */
    public static function getValueOfFieldByTarget($target, $key = null, $default = null)
    {
        if (is_null($key)) {
            return $target;
        }

        $key = is_array($key) ? $key : explode('.', is_int($key) ? (string) $key : $key);

        while (! is_null($segment = array_shift($key))) {
            if (is_array($target) && isset($target[$segment])) {
                $target = $target[$segment];
            } else {
                return $default;
            }
        }
        return $target;
    }

    /**
     * @param $operator
     * @param $threshold
     * @param $metric
     * @return bool|false|string
     */
    public static function compare($operator, $threshold, $metric)
    {
        switch ($operator) {
            case 'eq':
                return $metric == $threshold;
                break;
            case 'gt':
                return $metric > $threshold;
                break;
            case 'gte':
                return $metric >= $threshold;
                break;
            case 'lt':
                return $metric < $threshold;
                break;
            case 'lte':
                return $metric <= $threshold;
                break;
            case 'neq':
                return $metric != $threshold;
                break;
            case 'in':
                return in_array($metric, $threshold);
                break;
            case 'not-in':
                return ! in_array($metric, $threshold);
                break;
            case 'contain':
                return strstr($metric, $threshold);
                break;
            case 'not-contain':
                return ! strstr($metric, $threshold);
                break;
            case 'isset':
                return ! ($metric == '');
                break;
            case 'not-isset':
                return $metric == '';
                break;
        }
    }

    /**
     * 验证
     *
     * @param $ctn
     * @param $rules
     * @param mixed $options
     * @return bool|false|string
     */
    public static function validate($ctn, &$rules, $options = [])
    {
        $otherField = isset($options['otherField']) ? $options['otherField'] : '';
        $oldData = isset($options['oldData']) ? $options['oldData'] : [];
        foreach ($rules as $hashKey => &$rulevalue) {
            $field = $rulevalue['field'];
            if (! empty($otherField)) {
                $field .= $rulevalue[$otherField];
            }
            $operatorType = isset($rulevalue['operator_type']) ? $rulevalue['operator_type'] : '';
            $operator = isset($rulevalue['operator']) ? $rulevalue['operator'] : '';
            $threshold = isset($rulevalue['threshold']) ? $rulevalue['threshold'] : '';
            $metric = CustomerCommon::getValueOfFieldByTarget($ctn, $field, self::XTQNOFIELD);
            if ($metric === self::XTQNOFIELD) {
                switch ($operator) {
                    case 'not-isset':
                    case 'not-in':
                    case 'not-contain':
                        return true;
                }
                return false;
            }
            if (in_array($operatorType, ['float', 'float-up', 'float-down'])) {
                $operator = $operatorType;
            }
//            print_r($rulevalue);
//            print_r($ctn);
//            print_r($oldData);
            switch ($operator) {
                case 'float':
                case 'float-up':
                case 'float-down':
                    $judgeResult = self::cycleCompare($rulevalue, $ctn, $oldData);
                    return $judgeResult;
                    break;
                case 'eq':
                    if (! ($metric == $threshold)) {
                        return false;
                    }
                    break;
                case 'gt':
                    if (! ($metric > $threshold)) {
                        return false;
                    }
                    break;
                case 'gte':
                    if (! ($metric >= $threshold)) {
                        return false;
                    }
                    break;
                case 'lt':
                    if (! ($metric < $threshold)) {
                        return false;
                    }
                    break;
                case 'lte':
                    if (! ($metric <= $threshold)) {
                        return false;
                    }
                    break;
                case 'neq':
                    if (! ($metric != $threshold)) {
                        return false;
                    }
                    break;
                case 'in':
                    if (! (in_array($metric, $threshold))) {
                        return false;
                    }
                    break;
                case 'not-in':
                    if (! (! in_array($metric, $threshold))) {
                        return false;
                    }
                    break;
                case 'contain':
                    if (! (strstr((string) $metric, $threshold))) {
                        return false;
                    }
                    break;
                case 'not-contain':
                    if (! (! strstr((string) $metric, $threshold))) {
                        return false;
                    }
                    break;
//                case 'isset':
//                    break;
                case 'not-isset':
                    return false;
                    break;
                case 'eq-self':
                    $rules[$hashKey]['threshold'] = $metric;
                    break;
            }
        }
        return true;
    }

    public static function cycleCompare(array $rulevalue, array $ctn, array $oldData): bool
    {
        $field = $rulevalue['field'];
        $currentMetric = CustomerCommon::getValueOfFieldByTarget($ctn, $field, self::XTQNOFIELD);
        $oldMetric = CustomerCommon::getValueOfFieldByTarget($oldData, $field, self::XTQNOFIELD);
        if (($currentMetric === self::XTQNOFIELD) || $oldMetric === self::XTQNOFIELD) {
            return false;
        }
        $thresholdType = isset($rulevalue['threshold_type']) ? $rulevalue['threshold_type'] : '';
        $operatorType = isset($rulevalue['operator_type']) ? $rulevalue['operator_type'] : '';
        $operator = isset($rulevalue['operator']) ? $rulevalue['operator'] : '';
        $threshold = isset($rulevalue['threshold']) ? $rulevalue['threshold'] : '';
        switch ($operatorType . '-' . $operator) {
            case 'float-exceed':
                $metric = abs($currentMetric - $oldMetric);
                if ($thresholdType == MonitorTask::MONITOR_COMPARE_PERCENT) {
                    if ($oldMetric == 0) {
                        $oldMetric = 1;
                    }
                    $metric = round(($metric / $oldMetric) * 100);
                }
                if (! ($metric >= $threshold)) {
                    return false;
                }
                break;
            case 'float-up-exceed':
                $metric = $currentMetric - $oldMetric;
                if ($thresholdType == MonitorTask::MONITOR_COMPARE_PERCENT) {
                    if ($oldMetric == 0) {
                        $oldMetric = 1;
                    }
                    $metric = round(($metric / $oldMetric) * 100);
                }
                if (! ($metric >= $threshold)) {
                    return false;
                }
                break;
            case 'float-down-exceed':
                $metric = $currentMetric - $oldMetric;
                $metric = (0 - $metric);
                if ($thresholdType == MonitorTask::MONITOR_COMPARE_PERCENT) {
                    if ($oldMetric == 0) {
                        $oldMetric = 1;
                    }
                    $metric = round(($metric / $oldMetric) * 100);
                }
                if (! ($metric >= $threshold)) {
                    return false;
                }
                break;
            case 'float-not-exceed':
                $metric = abs($currentMetric - $oldMetric);
                if ($thresholdType == MonitorTask::MONITOR_COMPARE_PERCENT) {
                    if ($oldMetric == 0) {
                        $oldMetric = 1;
                    }
                    $metric = round(($metric / $oldMetric) * 100);
                }
                if (! (($metric <= $threshold) && $metric > 0)) {
                    return false;
                }
                break;
            case 'float-up-not-exceed':
                $metric = $currentMetric - $oldMetric;
                if ($thresholdType == MonitorTask::MONITOR_COMPARE_PERCENT) {
                    if ($oldMetric == 0) {
                        $oldMetric = 1;
                    }
                    $metric = round(($metric / $oldMetric) * 100);
                }
                if (! (($metric <= $threshold) && $metric > 0)) {
                    return false;
                }
                break;
            case 'float-down-not-exceed':
                $metric = $currentMetric - $oldMetric;
                $metric = (0 - $metric);
                if ($thresholdType == MonitorTask::MONITOR_COMPARE_PERCENT) {
                    if ($oldMetric == 0) {
                        $oldMetric = 1;
                    }
                    $metric = round(($metric / $oldMetric) * 100);
                }
                if (! (($metric <= $threshold) && ($metric > 0))) {
                    return false;
                }
                break;
            case 'float-eq':
                $metric = abs($currentMetric - $oldMetric);
                if ($thresholdType == MonitorTask::MONITOR_COMPARE_PERCENT) {
                    if ($oldMetric == 0) {
                        $oldMetric = 1;
                    }
                    $metric = round(($metric / $oldMetric) * 100);
                }
                if (! ($metric == $threshold)) {
                    return false;
                }
                break;
            case 'float-up-eq':
                $metric = $currentMetric - $oldMetric;
                if ($thresholdType == MonitorTask::MONITOR_COMPARE_PERCENT) {
                    if ($oldMetric == 0) {
                        $oldMetric = 1;
                    }
                    $metric = round(($metric / $oldMetric) * 100);
                }
                if (! ($metric == $threshold)) {
                    return false;
                }
                break;
            case 'float-down-eq':
                $metric = $currentMetric - $oldMetric;
                $metric = 0 - $metric;
                if ($thresholdType == MonitorTask::MONITOR_COMPARE_PERCENT) {
                    if ($oldMetric == 0) {
                        $oldMetric = 1;
                    }
                    $metric = round(($metric / $oldMetric) * 100);
                }
                if (! ($metric == $threshold)) {
                    return false;
                }
                break;
            case 'float-neq':
                $metric = abs($currentMetric - $oldMetric);
                if ($thresholdType == MonitorTask::MONITOR_COMPARE_PERCENT) {
                    if ($oldMetric == 0) {
                        $oldMetric = 1;
                    }
                    $metric = round(($metric / $oldMetric) * 100);
                }
                if (! ($metric != $threshold)) {
                    return false;
                }
                break;
            case 'float-up-neq':
                $metric = $currentMetric - $oldMetric;
                if ($thresholdType == MonitorTask::MONITOR_COMPARE_PERCENT) {
                    if ($oldMetric == 0) {
                        $oldMetric = 1;
                    }
                    $metric = round(($metric / $oldMetric) * 100);
                }
                if (! ($metric != $threshold)) {
                    return false;
                }
                break;
            case 'float-down-neq':
                $metric = $currentMetric - $oldMetric;
                $metric = 0 - $metric;
                if ($thresholdType == MonitorTask::MONITOR_COMPARE_PERCENT) {
                    if ($oldMetric == 0) {
                        $oldMetric = 1;
                    }
                    $metric = round(($metric / $oldMetric) * 100);
                }
                if (! ($metric != $threshold)) {
                    return false;
                }
                break;
        }
        return true;
    }

    /**
     * record send fail msg.
     * @param \Exception $e
     * @param mixed $ctx
     */
    public static function recordLog(object $e, string $sendContent, array $receiver, $ctx = [])
    {
        $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('send_notice_fail', 'default');
        $stdOutLogger = ApplicationContext::getContainer()->get(StdoutLoggerInterface::class);
        $errorMsg = "\r\nerror:\n" . $e->getMessage() . "\r\ncontent:\n" . $sendContent . "---\r\nStack Trace:\n" . $e->getTraceAsString();
        if (! empty($ctx)) {
            if (is_array($ctx)) {
                $ctx = \GuzzleHttp\json_encode($ctx);
            }
            $errorMsg .= "\r\ncontext:\n " . $ctx;
        }
        $logger->error($errorMsg);
        $stdOutLogger->error($errorMsg);
        try {
            $producer = new Producer();
            $producer->setAppid(config('kafka_product_appid'));
            $producer->setAppkey(config('kafka_product_appkey'));
            $producer->setQueue(config('send_fail_queue'));
            $producer->setProxy(config('httpcallbackproxy.product'));
            $producer->setTimeout(3);
            $sendContent = "content:{$sendContent}||key:{$receiver[0]}||value:" . json_encode($receiver[1]);
            $producer->produce($sendContent);
        } catch (\Exception $e) {
            $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('save_send_notice_fail', 'default');
            $logger->error($sendContent);
        }
    }

    /**
     * get callback pool.
     * @param $uri
     * @return mixed
     */
    public static function getCallbackClient($uri)
    {
        $factory = new HandlerStackFactory();
        $stack = $factory->create([
            'min_connections' => 2,
            'max_connections' => (int) (10),
            'wait_timeout' => 3.0,
            'max_idle_time' => 60,
        ], [
            'retry' => [RetryMiddleware::class, [1, 10]],
        ]);

        return make(Client::class, [
            'config' => [
                'base_uri' => $uri,
                'handler' => $stack,
                'timeout' => 1,
            ],
        ]);
    }

    /**
     * @return mixed|string
     */
    public static function getLocalIp()
    {
        $currentIp = '127.0.0.1';
        $ipArr = swoole_get_local_ip();
        if (! empty($ipArr)) {
            $currentIp = current($ipArr);
        }

        return $currentIp;
    }

    /**
     * 上报信息.
     *
     * @param $tick
     * @param $status
     * @param $code
     * @param $ip
     */
    public static function reportTick($tick, $status, $code, $ip)
    {
        if (empty($tick)) {
            return;
        }
        try {
            $tick && $tick->report($status, $code, $ip);
        } catch (\Throwable $e) {
            $logger = ApplicationContext::getContainer()->get(LoggerFactory::class)->get('trace_tick', 'default');
            $logger->error("Tick report error, status {$status}, code {$code}, ip {$ip} :" . $e->getMessage(), [
                'file' => $e->getFile(),
                'line' => $e->getLine(),
                'info' => "{$status}--{$code}--{$ip}",
            ]);
        }
    }

    /**
     * @param $aggCycle
     * @return array
     */
    public static function calcSecondTriggerCycle($aggCycle)
    {
        //transform to minuter
        $aggCycleMinute = $aggCycle / 60;
        //get current minute
        $currentMinute = date('i');
        $current = (int) strtotime(date('Y-m-d H:i:s'));
        $start = (int) strtotime(date('Y-m-d H:i'));
        $remainder = $currentMinute % $aggCycleMinute;
        if ($remainder > 0) {
            $diff = $aggCycleMinute - $remainder;
            $start = $start + $diff * 60;
        }
        //agg end minute
        $end = (int) $start + (int) $aggCycle;
        return [
            'start' => $start,
            'end' => $end,
            'tosleep' => $end - $current,
        ];
    }
}
