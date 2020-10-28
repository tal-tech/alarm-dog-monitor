<?php

declare(strict_types=1);

namespace XesMq\Mq;

use Exception;
use XesMq\Mq\Business\Callback;
use XesMq\Mq\Http\Curl;

class Consumer
{
    const CONSUME_URI_KAFKA = '/v1/kafka/fetch';

    const COMMIT_URI_KAFKA = '/v1/kafka/commit';

    const CONSUME_URI_RABBIT = '/v1/rabbit/fetch';

    /**
     * 简单模式下通过业务逻辑返回true or false.
     */
    const COMMIT_MODE_SIMPLE = 1;

    /**
     * OFFSET模式下通过业务逻辑返回要提交的offset数组来处理.
     */
    const COMMIT_MODE_OFFSET = 2;

    public static $logDir = '/var/log/mq';

    public static $logLevel = 'DEBUG';

    /**
     * logger.
     */
    protected $logger;

    protected $appid = '';

    protected $appkey = '';

    protected $appInfo = '';

    /**
     * Kafka 消费者组的名称
     * RabbitMQ 账号信息json.
     */
    protected $group;

    /**
     * queues/topics.
     */
    protected $queue;

    /**
     * kafka: reset策略 值为latest/earliest
     * rabbitmq: 表明是rabbitmq 值为rabbitmq.
     */
    protected $reset;

    /**
     * curl请求超时时间.
     */
    protected $curlTimeout = 60;

    /**
     * only for kafka
     * 此参数表明一次消费中最大的处理时间（提交前）
     * 服务端会在此时间之后认为消费者程序异常，从而把这部分消息自动转移给其他消费请求
     *
     * RabbitMQ只会自动ACK，不需要提交请求
     */
    protected $commitTimeout = 60;

    /**
     * only for kafka
     * 消费有可能是重复消费的
     * 当此参数值为-1，代表无限重复消费
     * 当此参数为正整数，代表消息最大可重复消费的次数，消息将在最大重试次数之后记录日志并强制提交.
     */
    protected $maxConsumeTimes = -1;

    /**
     * only for kafka
     * 一次消费请求中，最大返回条数.
     */
    protected $maxMsgs = 1;

    /**
     * 业务逻辑回调.
     */
    protected $callback;

    /**
     * for pcntl control
     * 代表是否还能继续消费.
     * @var int
     */
    protected $canNotConsume = 1;

    /**
     * only for kafka
     * offset提交模式，默认为简单模式.
     */
    protected $commitMode = self::COMMIT_MODE_SIMPLE;

    protected $proxy = '127.0.0.1:8888';

    protected $proxyList = ['127.0.0.1:8888'];

    /**
     * only for kafka
     * 有状态服务，标记当前服务端位置.
     */
    protected $currentProxy;

    public function setProxy($proxy)
    {
        $this->proxy = $proxy;
        if (strpos($proxy, ',') === false) {
            $this->proxyList = [$proxy];
        } else {
            $this->proxyList = array_filter(explode(',', $proxy));
        }
    }

    /**
     * 停止消费者.
     */
    public function stopConsume()
    {
        $this->canNotConsume = 0;
    }

    public function setGroup($group)
    {
        $this->group = $group;
    }

    public function setQueue($queue)
    {
        $this->queue = $queue;
    }

    public function setReset($reset)
    {
        $this->reset = $reset;
    }

    public function setCurlTimeOut($curlTimeout)
    {
        $this->curlTimeout = intval($curlTimeout);
    }

    public function setCommitTimeout($timeout)
    {
        $this->commitTimeout = intval($timeout);
    }

    public function setMaxConsumeTimes($maxConsumeTimes)
    {
        $this->maxConsumeTimes = intval($maxConsumeTimes);
    }

    public function setMaxMsgs($maxMsgs)
    {
        $this->maxMsgs = intval($maxMsgs);
    }

    public function setCallback($callback)
    {
        $this->callback = $callback;
    }

    public function setLogger($logger)
    {
        $this->logger = $logger;
    }

    public function setAppid($appid)
    {
        $this->appid = strval($appid);
        $this->appInfo = $this->appid . ',' . $this->appkey;
    }

    public function setAppkey($appkey)
    {
        $this->appkey = strval($appkey);
        $this->appInfo = $this->appid . ',' . $this->appkey;
    }

    /**
     * 设置提交模式
     * since 0.1.10.
     * @param mixed $mode
     */
    public function setCommitMode($mode)
    {
        if ($mode == self::COMMIT_MODE_SIMPLE || $mode == self::COMMIT_MODE_OFFSET) {
            $this->commitMode = $mode;
        }
    }

    /**
     * 获取MQ消息.
     */
    public function fetch()
    {
        $postData = json_encode([
            'instanceId' => $this->appInfo,
            'group' => $this->group,
            'queues' => $this->queue,
            'reset' => $this->reset,
            'commitTimeout' => $this->commitTimeout,
            'maxConsumeTimes' => $this->maxConsumeTimes,
            'maxMsgs' => $this->maxMsgs,
        ]);

        if ($this->currentProxy != null) {
            $urlList = [$this->currentProxy . self::CONSUME_URI_KAFKA];
        } else {
            $urlList = array_map(function ($item) {
                return $item . self::CONSUME_URI_KAFKA;
            }, $this->proxyList);
        }

        try {
            $data = Curl::post($urlList, $this->curlTimeout, $postData, $this->appid, $this->appkey);
        } catch (Exception $e) {
            //fetch异常，自动清理当前proxy节点
            $this->currentProxy = null;
            throw $e;
        }

        if ($data == null) {
            $this->currentProxy = null;
            return [
                'code' => 500,
                'msg' => 'get null form server',
                'data' => null,
            ];
        }

        //检查代理是否在其他机器，如果在其他机器，访问其他机器
        if (isset($data['code']) && $data['code'] == 404 && $data['data'] != '') {
            $this->currentProxy = $data['data'];
            return $this->fetch();
        }

        //状态码不为0
        if (isset($data['code']) && $data['code'] != 0) {
            $this->logger != null && $this->logger->error('状态码' . $data['code'] . ',错误信息' . $data['msg']);
        }

        return $data;
    }

    /**
     * 获取MQ消息.
     */
    public function fetchRabbit()
    {
        $postData = json_encode([
            'instanceId' => $this->appInfo,
            'group' => $this->group,
            'queues' => $this->queue,
            'reset' => $this->reset,
            'commitTimeout' => $this->commitTimeout,
            'maxConsumeTimes' => $this->maxConsumeTimes,
            'maxMsgs' => $this->maxMsgs,
        ]);

        if ($this->currentProxy != null) {
            $urlList = [$this->currentProxy . self::CONSUME_URI_RABBIT];
        } else {
            $urlList = array_map(function ($item) {
                return $item . self::CONSUME_URI_RABBIT;
            }, $this->proxyList);
        }

        return Curl::post($urlList, $this->curlTimeout, $postData, $this->appid, $this->appkey);
    }

    /**
     * 提交offset.
     * @param mixed $offsetRange
     */
    public function commit($offsetRange)
    {
        $ttc = $this->_sortOffset($offsetRange);
        $this->logger != null && $this->logger->debug('提交OFFSET');
        $this->logger != null && $this->logger->debug('ttc', ['ttc' => $ttc]);

        $postData = json_encode([
            'instanceId' => $this->appInfo,
            'group' => $this->group,
            'queues' => $this->queue,
            'data' => $ttc,
        ]);

        if ($this->currentProxy != null) {
            $urlList = [$this->currentProxy . self::COMMIT_URI_KAFKA];
        } else {
            $urlList = array_map(function ($item) {
                return $item . self::COMMIT_URI_KAFKA;
            }, $this->proxyList);
        }

        try {
            $commitResult = Curl::post($urlList, $this->curlTimeout, $postData, $this->appid, $this->appkey);
        } catch (\Exception $e) {
            //curl 请求有异常
            $this->currentProxy = null;
            throw $e;
        }

        //代理不在本节点自动修正
        if ($commitResult['code'] == 404 && $commitResult['data'] != '') {
            $this->currentProxy = $commitResult['data'];
            return $this->commit($offsetRange);
        }

        return $commitResult;
    }

    /**
     * 标记offset为成功
     * @param mixed $successOffsets
     * @param mixed $topic
     * @param mixed $partition
     * @param mixed $offset
     */
    public static function markSuccess(&$successOffsets, $topic, $partition, $offset)
    {
        if (! isset($successOffsets[$topic])) {
            $successOffsets[$topic] = [];
        }
        if (! isset($successOffsets[$topic][$partition])) {
            $successOffsets[$topic][$partition] = [];
        }
        if (! in_array($offset, $successOffsets[$topic][$partition])) {
            $successOffsets[$topic][$partition][] = $offset;
        }
    }

    /**
     * 消费.
     */
    public function consume()
    {
        if ($this->callback == null) {
            var_dump('callback未设置');
            exit;
        }
        while ($this->canNotConsume) {
            //信号停止
            if (function_exists('pcntl_fork')) {
                pcntl_signal_dispatch();
            }

            //拉取数据
            $this->logger != null && $this->logger->debug('发送消费请求' . \date('Y-m-d H:i:s'));
            try {
                $data = $this->fetch();
                //var_dump($data);
            } catch (Exception $e) {
                $this->logger != null && $this->logger->error('消费请求异常:' . $e->getTraceAsString());
                sleep(1);
                continue;
            }

            //处理返回的数据
            $this->logger != null && $this->logger->debug('接收到数据' . \date('Y-m-d H:i:s'));
            if (isset($data['code']) && $data['code'] == 0) {
                //数据正常
                $this->logger != null && $this->logger->debug('消息条数为' . count($data['data']));
                //数据条数为0，自动等待防止空轮询
                if (count($data['data']) == 0) {
                    sleep(1);
                    continue;
                }
            } else {
                continue;
            }

            //调用业务方回调方法
            $callbackStart = microtime(true);
            try {
                if (is_callable($this->callback)) {
                    $result = call_user_func($this->callback, $data['data']);
                } elseif ($this->callback instanceof Callback) {
                    $result = $this->callback->handle($data['data']);
                } else {
                    $result = false;
                }
            } catch (Exception $e) {
                //如果业务逻辑有异常
                $this->logger != null && $this->logger->error('业务逻辑有异常');
                $this->logger != null && $this->logger->error($e->getTraceAsString());
                continue;
            }
            $callbackEnd = microtime(true);

            //判断提交模式，进行提交
            $ttc = null;
            if ($this->commitMode == self::COMMIT_MODE_SIMPLE) {
                if ($result === true) {
                    //对offset进行主题分区分组
                    $ttc = array_reduce($data['data'], function ($carry, $item) {
                        if (! isset($carry[$item['queue']])) {
                            $carry[$item['queue']] = [];
                        }
                        if (! isset($carry[$item['queue']][$item['props']['partition']])) {
                            $carry[$item['queue']][$item['props']['partition']] = [];
                        }
                        $carry[$item['queue']][$item['props']['partition']][] = $item['props']['offset'];
                        return $carry;
                    }, []);
                } else {
                    $this->logger != null && $this->logger->warning('回调业务逻辑返回值不为true, 消息无法被消费');
                    continue;
                }
            } elseif ($this->commitMode == self::COMMIT_MODE_OFFSET) {
                //取得业务逻辑返回的带提交offset
                if (! is_array($result) || count($result) == 0) {
                    continue;
                }
                //提交请求
                $ttc = $result;
            } else {
                continue;
            }

            if ($ttc == null) {
                $this->logger != null && $this->logger->warning('ttc equals null');
                continue;
            }

            //提交请求
            $commitResult = null;
            do {
                try {
                    $commitResult = $this->commit($ttc);
                    //var_dump($commitResult);
                } catch (\Exception $e) {
                    $this->logger != null && $this->logger->warning('提交失败');
                    continue;
                }
            } while (! ($commitResult != null && isset($commitResult['code']) && $commitResult['code'] == 0));
        }
    }

    private function _sortOffset(&$tpo)
    {
        //$topic to commit
        $ttc = [];
        //对offset排序
        foreach ($tpo as $t => $po) {
            foreach ($po as $p => $o) {
                sort($o);
                $oCount = count($o);
                $start = $o[0];
                for ($i = 0; $i < count($o); $i++) {
                    $next = $i + 1;
                    if ($next != $oCount && $o[$next] != $o[$i] + 1) {
                        //区间不连续
                        $end = $o[$i];
                        $ttc[] = [
                            'topic' => $t,
                            'partition' => $p,
                            'left' => $start,
                            'right' => $end,
                        ];
                        $start = $o[$next];
                    }
                }
                $end = $o[$oCount - 1];
                $ttc[] = [
                    'topic' => $t,
                    'partition' => $p,
                    'left' => $start,
                    'right' => $end,
                ];
            }
        }

        return $ttc;
    }
}
