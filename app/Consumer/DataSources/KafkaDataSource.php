<?php

declare(strict_types=1);

namespace App\Consumer\DataSources;

use App\Consumer\MonitorCallback\KafkaMonitorCallback;
use Hyperf\Utils\Coroutine;
use XesMq\Mq\Consumer;

class KafkaDataSource extends Consumer implements IDataSource
{
    use TraitMonitorInfo;
    use TraitConstruct;

    /**
     * start consume.
     */
    public function start()
    {
        try {
            $this->consume();
        } catch (\Throwable $e) {
            $this->dataSourceLogger->error('Coroutine ' . Coroutine::id() . ' Kafka database throw error ' . $e->getMessage());
            $this->stdOutLogger->notice('Coroutine ' . Coroutine::id() . ' Kafka database throw error ' . $e->getMessage());
            sleep(self::WAIT_BEFORE_RECONNECT);
            $this->start();
        }
    }

    /**
     * stop consume.
     */
    public function stop()
    {
        try {
            $this->stopConsume();
        } catch (\Throwable $e) {
            $msg = 'kafka database stop exception ' . $e->getMessage() . ' at ' . date('Y-m-d H:i:s');
            $this->dataSourceLogger->error($msg);
            $this->stdOutLogger->error($msg);
        }
        return true;
    }

    /**
     * @throws \ReflectionException
     * @return mixed
     */
    public function init()
    {
        $monitorInfo = $this->getMonitorInfo();
        $dataSource = $monitorInfo->getDataSource();
        $dataSourceConfig = json_decode($dataSource['config'], true);
        $this->setAppid(config('kafka_appid'));
        $this->setAppkey(config('kafka_appkey'));
        $this->setLogger($this->dataSourceLogger);
        $this->setProxy($dataSourceConfig['consumer_proxy']);
        $this->setQueue($dataSourceConfig['topic']);
        $this->setGroup($dataSourceConfig['topic'] . '_group');
        $this->setReset('earlies');
        $this->setCommitMode(Consumer::COMMIT_MODE_OFFSET);
//        //一次消费请求中，最大等待时间，也就是说如果没有消息的时候当前的fetch请求要等待多长时间　
//        //（`kafka没有消息的时候fetch要等待的时间）
        $this->setCommitTimeout(5);
        $this->setMaxMsgs(5);  //从服务器拉取消息的时候一次拉取最多返回多少条记录
        $this->setMaxConsumeTimes(2);   //每条消息最多重复消费的次数
        /** @var KafkaMonitorCallback $callback */
        $callback = make(KafkaMonitorCallback::class);
        $rc = new \ReflectionClass(KafkaMonitorCallback::class);
        $rc->hasMethod('setLogger') && $callback->setLogger($this->dataSourceLogger);
        $rc->hasMethod('setMonitorInfo') && $callback->setMonitorInfo($this->monitorInfo)->init(); //模调相关配置
        $this->setCallback($callback);
    }
}
