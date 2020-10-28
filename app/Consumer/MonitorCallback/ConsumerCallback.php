<?php

declare(strict_types=1);

namespace App\Consumer\MonitorCallback;

use App\Common\CustomerCommon;
use App\Consumer\MonitorInfo\MonitorTaskInfo;
use App\Model\AlarmTaskModel;
use GuzzleHttp\Client;
use Hyperf\Contract\StdoutLoggerInterface;
use Hyperf\Database\MySqlConnection;
use Hyperf\DbConnection\Db;
use Hyperf\Di\Annotation\Inject;
use Hyperf\Logger\LoggerFactory;
use Psr\Container\ContainerInterface;
use Psr\EventDispatcher\EventDispatcherInterface;
use Psr\Log\LoggerInterface;

abstract class ConsumerCallback
{
    const SUCCESSCODE = 0;

    /** @var string */
    public static $redisPrex = 'xtq:monitor:';

    public static $defaultRedisExpireTime = 24 * 60 * 60;

    /**
     * @var Client
     */
    public $alarmClient;

    /**
     * @var LoggerInterface
     */
    protected $logger;

    /**
     * @var LoggerInterface
     */
    protected $tickLogger;

    /**
     * @var LoggerInterface
     */
    protected $saveFailMessagelogger;

    /**
     * @var LoggerInterface
     */
    protected $consumerCallbackLogger;

    /**
     * @var StdoutLoggerInterface
     */
    protected $stdOutLogger;

    /**
     * trace module id.
     * @var string
     */
    protected $traceModuleId = '';

    /**
     * 当前消费者协程消费消息接口名称.
     * @var string
     */
    protected $msgInterfaceName = '';

    /**
     * callback ip.
     * @var string
     */
    protected $httpCallbackIp = '';

    /**
     * @var ContainerInterface
     */
    protected $container;

    /**
     * @var MonitorTaskInfo
     */
    protected $monitorInfo;

    /**
     * @Inject
     * @var EventDispatcherInterface
     */
    protected $eventDispatcher;

    /**
     * @var MySqlConnection
     */
    private $connection;

    /**
     * ConsumerCallback constructor.
     */
    public function __construct(
        ContainerInterface $container,
        LoggerFactory $loggerFactory,
        StdoutLoggerInterface $stdOutlogger
    ) {
        $this->container = $container;
        $this->stdOutLogger = $stdOutlogger;
        $this->consumerCallbackLogger = $loggerFactory->get('protocol_monitor', 'default');
        $this->tickLogger = $loggerFactory->get('trace_tick', 'default');
        $this->saveFailMessagelogger = $loggerFactory->get('save_alarm_fail_message', 'default');
    }

    /**
     * @param $msg
     * @param array $options
     * @return mixed
     */
    abstract public function handle($msg, $options = []);

    /**
     * init alarm client.
     */
    public function init()
    {
        $this->alarmClient = CustomerCommon::getCallbackClient(config('monitor.alarm_api_uri'));
    }

    /**
     * @param $msg
     * @param $level
     */
    public function sendAlarm($msg, $level)
    {
        $monitorTaskInfo = $this->getMonitorInfo();
        $taskId = $monitorTaskInfo->getModel()->task_id;
        $token = $this->getTokenOfTask($taskId);
//        $token = 'd0e4aa13b5e1eda0a2e941ee3f40432776d90463';
        $time = time();
        $reqJson = [
            'taskid' => $taskId,
            'timestamp' => $time,
            'sign' => md5($taskId . '&' . $time . $token),
            'level' => $level,
            'ctn' => $msg,
        ];
        try {
            $response = $this->alarmClient->post(config('monitor.alarm_api_uri'), [
                'json' => $reqJson,
            ]);
            $body = $response->getBody();
            $remainingBytes = $body->getContents();
            $responseArr = json_decode($remainingBytes, true);
            $this->stdOutLogger->info('【monitor】Task ' . $this->getMonitorInfo()->getTaskFlag() . ' alarm report return ' . $remainingBytes);
            //判断状态
            if (! isset($responseArr['code']) || $responseArr['code'] != self::SUCCESSCODE) {
                $this->stdOutLogger->error('send alarm fail' . '--' . json_encode($responseArr));
                $this->logger->error('send alarm fail' . '--' . json_encode($responseArr));
            }
        } catch (\Throwable $e) {
            $this->stdOutLogger->notice('Send alarm fail' . $e->getMessage(), [
                'file' => $e->getFile(),
                'line' => $e->getLine(),
                'msg' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
                'kafka_msg' => \GuzzleHttp\json_encode($msg),
            ]);
            $this->logger->notice('Send alarm fail' . $e->getMessage(), [
                'file' => $e->getFile(),
                'line' => $e->getLine(),
                'msg' => $e->getMessage(),
                'trace' => $e->getTraceAsString(),
                'kafka_msg' => \GuzzleHttp\json_encode($msg),
            ]);
        }
    }

    /**
     * @param $logger
     */
    public function setLogger($logger)
    {
        $this->logger = $logger;
    }

    public function getMonitorInfo(): MonitorTaskInfo
    {
        return $this->monitorInfo;
    }

    public function setMonitorInfo(MonitorTaskInfo $monitorInfo)
    {
        $this->monitorInfo = $monitorInfo;
        return $this;
    }

    /**
     * 获取监控数据.
     *
     * @param $monitorType
     * @param $time
     */
    public function getMonitorData($monitorType, $time): array
    {
        $data = [];
        $dataSourceId = $this->monitorInfo->model->datasource_id;
        $taskId = $this->monitorInfo->model->id;
        try {
            $table = 'monitor_record_' . $dataSourceId;
            $query = Db::table($table)
                ->where('monitor_type', $monitorType)
                ->where('taskid', $taskId)
                ->where('created_at', $time)
                ->orderByDesc('id')
                ->select(['fields']);
            $this->stdOutLogger->info($monitorType . ' get monitor record to sql: ' . $query->toSql());
            $this->saveFailMessagelogger->info($monitorType . ' get monitor record to binds: ' . json_encode($query->getBindings()));
            $obj = $query->first();
            if (! empty($obj)) {
                $data = json_decode($obj->fields, true);
            }
        } catch (\Throwable $e) {
            $msg = 'monitor callback get old data fail, reason:' . $e->getMessage() . '====trace:' . $e->getTraceAsString();
            $this->stdOutLogger->error($msg);
            $this->saveFailMessagelogger->error($msg);
        }
        $this->stdOutLogger->info($monitorType . ' getMonitorData type:' . $monitorType . ', time:' . $time . ', result:' . json_encode($data));
        return $data;
    }

    /**
     * @param $monitorType
     * @param $alarmRuleId
     * @param $fields
     * @param $created_at
     * @param $data_time
     */
    public function insertRecord($monitorType, $alarmRuleId, $fields, $created_at)
    {
        $dataSourceId = $this->monitorInfo->model->datasource_id;
        $taskId = $this->monitorInfo->model->id;
        try {
            $table = 'monitor_record_' . $dataSourceId;
            $insertData = [
                'monitor_type' => $monitorType,
                'taskid' => $taskId,
                'alarm_rule_id' => $alarmRuleId,
                'fields' => $fields,
                'created_at' => $created_at,
            ];
            $this->stdOutLogger->info('【monitor】insert monitor record data: ' . json_encode($insertData));
            Db::table($table)->insert($insertData);
        } catch (\Throwable $e) {
            $msg = 'monitor callback get old data fail, reason:' . $e->getMessage() . '====trace:' . $e->getTraceAsString();
            $this->stdOutLogger->error($msg);
            $this->saveFailMessagelogger->error($msg);
        }
    }

    /**
     * @param $e \Exception
     */
    protected function operationMsg($e)
    {
        $msg = 'Kafka callback monitor important exception' . $e->getTraceAsString();
        $this->stdOutLogger->notice($msg . ' msg:' . $e->getMessage());
        $this->saveFailMessagelogger->notice($msg, [
            'file' => $e->getFile(),
            'line' => $e->getLine(),
            'msg' => $e->getMessage(),
        ]);
    }

    /**
     * 保存最终失败的消息到数据库.
     *
     * @param $cluster
     * @param $queue
     * @param $group
     * @param $type
     * @param $msg
     */
    protected function saveFailMessage($msg)
    {
//        try {
//            $this->saveDb->error(
//                "failure msg",
//                [
//                    'topic_msg' => is_string($msg) ? $msg : json_encode($msg),
//                ]
//            );
//        } catch (\Exception $e) {
//            $this->saveFailMessagelogger->error(
//                "save msg to db failure" . $e->getMessage(),
//                [
//                    'file' => $e->getFile(),
//                    'line' => $e->getLine(),
//                    'msg' => $e->getMessage(),
//                    'trace' => $e->getTraceAsString(),
//                    'topic_msg' => is_string($msg) ? $msg : json_encode($msg),
//                ]
//            );
//        }
    }

    /**
     * @param $taskId
     * @return bool|\Carbon\CarbonInterface|float|\Hyperf\Utils\Collection|int|mixed|string
     */
    private function getTokenOfTask($taskId)
    {
        try {
            $model = AlarmTaskModel::query()->select('token')->where('id', $taskId)->first();
            return isset($model['token']) ? $model['token'] : '';
        } catch (\Throwable $e) {
            $msg = 'Token get errof' . $e->getMessage();
            $this->stdOutLogger->error($msg);
            $this->logger->error($msg);
        }

        return '';
    }
}
