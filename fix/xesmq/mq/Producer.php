<?php

declare(strict_types=1);

namespace XesMq\Mq;

use XesMq\Mq\Http\Curl;

class Producer
{
    const PRODUCE_URI_KAFKA = '/v1/kafka/send';

    const PRODUCE_URI_BATCH_KAFKA = '/v1/kafka/send_batch';

    const CREATE_TOPIC_URI_KAFKA = '/v1/kafka/create_topic';

    const PRODUCE_URI_RABBIT = '/v1/rabbit/send';

    const PRODUCE_URI_BATCH_RABBIT = '/v1/rabbit/send_batch';

    protected $proxy = '127.0.0.1:8888';

    protected $timeout = 5;

    protected $appid;

    protected $appkey;

    protected $queue;

    protected $proxyList = ['127.0.0.1:8888'];

    protected $batchMessages = [];

    public function setProxy($proxy)
    {
        $this->proxy = $proxy;

        if (strpos($proxy, ',') === false) {
            $this->proxyList = [$proxy];
        } else {
            $this->proxyList = array_filter(explode(',', $proxy));
        }
    }

    public function setTimeout($timeout)
    {
        $this->timeout = $timeout;
    }

    public function setAppid($appid)
    {
        $this->appid = strval($appid);
    }

    public function setAppkey($appkey)
    {
        $this->appkey = strval($appkey);
    }

    public function setQueue($queue)
    {
        $this->queue = $queue;
    }

    /**
     * 批量发送
     * 添加.
     * @param mixed $payload
     * @param null|mixed $key
     * @param null|mixed $headers
     * @param null|mixed $props
     * @param null|mixed $extra
     */
    public function batchAdd($payload, $key = null, $headers = null, $props = null, $extra = null)
    {
        $message = [
            'queue' => $this->queue,
            'payload' => strval($payload),
        ];
        if ($headers != null) {
            $message['headers'] = $headers;
        }

        if ($key != null) {
            $message['key'] = strval($key);
        }
        if ($props != null) {
            $message['props'] = $props;
        }
        if ($extra != null) {
            $message['extra'] = $extra;
        } else {
            $message['extra'] = [];
        }
        $message['extra']['appid'] = $this->appid;
        $message['extra']['appkey'] = $this->appkey;

        $this->batchMessages[] = $message;
    }

    /**
     * 批量发送
     */
    public function batchProduce()
    {
        $urlList = array_map(function ($item) {
            return $item . self::PRODUCE_URI_BATCH_KAFKA;
        }, $this->proxyList);

        return Curl::post($urlList, $this->timeout, json_encode(['messages' => $this->batchMessages]), $this->appid, $this->appkey);
    }

    /**
     * 批量发送
     */
    public function batchProduceRabbit()
    {
        $urlList = array_map(function ($item) {
            return $item . self::PRODUCE_URI_BATCH_RABBIT;
        }, $this->proxyList);

        return Curl::post($urlList, $this->timeout, json_encode(['messages' => $this->batchMessages]), $this->appid, $this->appkey);
    }

    /**
     * 批量发送
     * 清空.
     */
    public function batchClear()
    {
        $this->batchMessages = [];
    }

    /**
     * 生产消息
     * 统一入口.
     * @param mixed $payload
     * @param null|mixed $key
     * @param null|mixed $headers
     * @param null|mixed $props
     * @param null|mixed $extra
     */
    public function produce($payload, $key = null, $headers = null, $props = null, $extra = null)
    {
        //检查
        //headers
        $postData = [
            'queue' => $this->queue,
            'payload' => strval($payload),
        ];

        if ($headers != null) {
            $postData['headers'] = $headers;
        }

        if ($key != null) {
            $postData['key'] = strval($key);
        }

        if ($props != null) {
            $postData['props'] = $props;
        }

        if ($extra != null) {
            $postData['extra'] = $extra;
        } else {
            $postData['extra'] = [];
        }
        $postData['extra']['appid'] = $this->appid;
        $postData['extra']['appkey'] = $this->appkey;

        $urlList = array_map(function ($item) {
            return $item . self::PRODUCE_URI_KAFKA;
        }, $this->proxyList);

        return Curl::post($urlList, $this->timeout, json_encode($postData), $this->appid, $this->appkey);
    }

    /**
     * 生产消息
     * 统一入口.
     * @param mixed $payload
     * @param null|mixed $key
     * @param null|mixed $headers
     * @param null|mixed $props
     * @param null|mixed $extra
     */
    public function produceRabbit($payload, $key = null, $headers = null, $props = null, $extra = null)
    {
        //检查
        //headers
        $postData = [
            'queue' => $this->queue,
            'payload' => strval($payload),
        ];

        if ($headers != null) {
            $postData['headers'] = $headers;
        }

        if ($key != null) {
            $postData['key'] = strval($key);
        }

        if ($props != null) {
            $postData['props'] = $props;
        }

        if ($extra != null) {
            $postData['extra'] = $extra;
        } else {
            $postData['extra'] = [];
        }
        $postData['extra']['appid'] = $this->appid;
        $postData['extra']['appkey'] = $this->appkey;

        $urlList = array_map(function ($item) {
            return $item . self::PRODUCE_URI_RABBIT;
        }, $this->proxyList);

        return Curl::post($urlList, $this->timeout, json_encode($postData), $this->appid, $this->appkey);
    }

    /**
     * create kafka topic.
     * @param mixed $name
     * @param mixed $partitions
     * @param mixed $replication
     * @param null|mixed $config
     */
    public function createTopic($name, $partitions, $replication, $config = null)
    {
        //检查
        //headers
        $postData = [
            'name' => $name,
            'partitions' => intval($partitions),
            'replication' => intval($replication),
        ];
        if ($config != null) {
            $postData['config'] = $config;
        }

        $urlList = array_map(function ($item) {
            return $item . self::CREATE_TOPIC_URI_KAFKA;
        }, $this->proxyList);

        return Curl::post($urlList, $this->timeout, json_encode($postData), $this->appid, $this->appkey);
    }

    private function gen_uuid()
    {
        return sprintf(
            '%04x%04x-%04x-%04x-%04x-%04x%04x%04x',
            // 32 bits for "time_low"
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff),

            // 16 bits for "time_mid"
            mt_rand(0, 0xffff),

            // 16 bits for "time_hi_and_version",
            // four most significant bits holds version number 4
            mt_rand(0, 0x0fff) | 0x4000,

            // 16 bits, 8 bits for "clk_seq_hi_res",
            // 8 bits for "clk_seq_low",
            // two most significant bits holds zero and one for variant DCE1.1
            mt_rand(0, 0x3fff) | 0x8000,

            // 48 bits for "node"
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff),
            mt_rand(0, 0xffff)
        );
    }
}
