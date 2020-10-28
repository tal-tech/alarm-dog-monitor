<?php

declare(strict_types=1);

namespace App\Consumer\DataSources;

use App\Consumer\MonitorCallback\ProtocolMonitorCallback;
use App\Support\GuzzleCreator;
use GuzzleHttp\Client;
use GuzzleHttp\Exception\GuzzleException;
use Hyperf\Di\Annotation\Inject;
use Hyperf\Utils\Coroutine;

class ProtocolDataSource extends BDataSource implements IDataSource
{
    use TraitMonitorInfo;
    use TraitConstruct;
    use TraitCallback;

    /**
     * @Inject
     * @var GuzzleCreator
     */
    protected $guzzleCreator;

    /**
     * @var array
     */
    private $alarmConfig;

    /**
     * @var Client
     */
    private $httpClient;

    /**
     * @var string
     */
    private $uri;

    /**
     * @var string
     */
    private $method;

    /**
     * @return mixed
     */
    public function start()
    {
        try {
            $monitorFrequency = 60;
            startRun:
            if ($this->running) {
                $monitorTaskInfo = $this->getMonitorInfo();
                $monitorFrequency = $monitorTaskInfo->getModel()->monitor_frequency;
                $startMicroTime = microtime(true);
                $response = $this->httpClient->request($this->method, $this->uri);
                $endMicroTime = microtime(true);
                $code = $response->getStatusCode();
                $body = (string) $response->getBody()->getContents();
                $bodyArr = [];
                try {
                    $bodyArr = \GuzzleHttp\json_decode($body, true);
                } catch (\Throwable $e) {
                    $bodyArr = [];
                    $errMsg = sprintf(
                        '【protocol-monitor】%s protocol data source decode fail on handle response %s --uri: %s' .
                        '---client-config: %s',
                        $this->getMonitorInfo()->getTaskFlag(),
                        $body,
                        $this->uri,
                        json_encode($this->httpClient->getConfig())
                    );
                    $this->stdOutLogger->error($errMsg);
                    $this->dataSourceLogger->error($errMsg);
                }
                $newResponseHeader = [];
                $responseHeader = $response->getHeaders();
                $headerKeysArr = array_keys($responseHeader);
                foreach ($headerKeysArr as $keyVal) {
                    $newResponseHeader[strtolower($keyVal)] = $responseHeader[$keyVal][0];
                }
                $model = $this->monitorInfo->getModel();
                $config = $model['config'];
                $configArr = [];
                if (! empty($config)) {
                    $configArr = json_decode($config, true);
                }
                $url = $configArr['url'];
                $requestTime = $endMicroTime - $startMicroTime;
                $msgs = [
                    'http' => [
                        'status' => $code,
                        'headers' => $newResponseHeader,
                        'body' => $body,
                        'body_length' => mb_strlen($body),
                        'body_json' => $bodyArr,
                        'request_time' => $requestTime,
                    ],
                    'task' => [
                        'id' => $monitorTaskInfo->getModel()->id,
                        'name' => $monitorTaskInfo->getModel()->name,
                    ],
                    'protocol' => [
                        'config' => [
                            'url' => $url,
                        ],
                    ],
                ];
                $this->stdOutLogger->info('【protocol-monitor】' . $this->getMonitorInfo()->getTaskFlag() . ' get date from ' . $this->uri . 'url, response: ' . json_encode($msgs));
                $this->dataSourceLogger->info('【protocol-monitor】' . $this->getMonitorInfo()->getTaskFlag() . ' get date from ' . $this->uri . 'url, response: ' . json_encode($msgs));
                $this->callback->handle($msgs);
            } else {
                return true;
            }
        } catch (GuzzleException $e) {
            $this->stdOutLogger->error('【protocol-monitor】' . $this->getMonitorInfo()->getTaskFlag() . ' error : ' . json_encode($e->getMessage()));
            $this->dataSourceLogger->error('【protocol-monitor】' . $this->getMonitorInfo()->getTaskFlag() . ' error : ' . json_encode($e->getMessage()));
            $this->initHttpClinet();
        } catch (\Throwable $e) {
            $msg = '【protocol-monitor】Coroutine ' . Coroutine::id() . ' protocol data source throw error ' . $e->getMessage() . '===' . $e->getTraceAsString();
            $this->dataSourceLogger->error($msg);
            $this->stdOutLogger->error($msg);
            $this->initHttpClinet();
        }
        $this->stdOutLogger->info('【protocol-monitor】protocol detect ' . $monitorTaskInfo->getTaskFlag() . ' sleep:' . $monitorFrequency);
        $this->dataSourceLogger->info('【protocol-monitor】protocol detect ' . $monitorTaskInfo->getTaskFlag() . ' sleep:' . $monitorFrequency);
        $this->toSleep($monitorFrequency);
        goto startRun;
    }

    /**
     * @return mixed
     */
    public function stop()
    {
        try {
            $this->running = false;
        } catch (\Throwable $e) {
            $msg = 'protocol data source stop exception ' . $e->getMessage() . ' at ' . date('Y-m-d H:i:s');
            $this->dataSourceLogger->error($msg);
            $this->stdOutLogger->error($msg);
        }
        return true;
    }

    /**
     * @return mixed
     */
    public function init()
    {
        /** @var ProtocolMonitorCallback $callback */
        $callback = make(ProtocolMonitorCallback::class);
        $rc = new \ReflectionClass(ProtocolMonitorCallback::class);
        $rc->hasMethod('setLogger') && $callback->setLogger($this->dataSourceLogger);
        $rc->hasMethod('setMonitorInfo') && $callback->setMonitorInfo($this->getMonitorInfo())->init(); //模调相关配置
        $this->setCallback($callback);
        $this->initHttpClinet();
    }

    /**
     * @param array $headers
     * @return array
     */
    protected function withSignHeader()
    {
        $headers = [];
        $appid = config('monitor.gateway.appid', '');
        $appkey = config('monitor.gateway.appkey', '');
        if (! empty($appid) && ! empty($appkey)) {
            $timestamp = time();
            $sign = md5("{$appid}&{$timestamp}{$appkey}");
            $headers['X-Auth-Appid'] = $appid;
            $headers['X-Auth-TimeStamp'] = $timestamp;
            $headers['X-Auth-Sign'] = $sign;
        }
        return $headers;
    }

    /**
     * get http client.
     */
    private function initHttpClinet()
    {
        $model = $this->monitorInfo->getModel();
        $config = $model['config'];
        $configArr = [];
        if (! empty($config)) {
            $configArr = json_decode($config, true);
        }

        $url = $configArr['url'];
        $parseUrl = parse_url($url);
        $queryStr = isset($parseUrl['query']) ? $parseUrl['query'] : '';
        $queryArr = [];
        if ($queryStr != '') {
            parse_str($queryStr, $queryArr);
        }
        $method = $configArr['method'];
        $headers = isset($configArr['headers']) ? $configArr['headers'] : [];
//        $headers['Connection'] = 'keep-alive';
        $queryArrFromCondig = isset($configArr['query']) ? $configArr['query'] : [];
        $queryArr = array_merge($queryArr, $queryArrFromCondig);
        $body = isset($configArr['body']) ? $configArr['body'] : '';
        $needGatewayAuth = isset($configArr['need_gateway_auth']) ? $configArr['need_gateway_auth'] : false;
        $httpUser = isset($configArr['http_user']) ? $configArr['http_user'] : '';
        $httpPassword = isset($configArr['http_password']) ? $configArr['http_password'] : '';

        //header
        $headers['Content-Type'] = isset($body['type']) ? $body['type'] : 'text/html';
//        $headers['Accept'] = 'application/json */*; q=0.01';
        if ($needGatewayAuth) {
            $gwSignHeaders = $this->withSignHeader();
            $headers = array_merge($headers, $gwSignHeaders);
        }

        //auth
        $auth = [];
        if (! empty($httpUser) && ! empty($httpPassword)) {
            $auth = [
                $httpUser,
                $httpPassword,
            ];
        }

        //request body
        $requestBody = [];
        if (strtoupper($method) !== 'GET') {
            switch ($body['type']) {
                case 'text/plain':
                case 'application/json':
                    $requestBody = ['body' => $body['content']];
                    break;
                case 'application/x-www-form-urlencoded':
                    $requestBody = ['form_params' => $body['params']];
                    break;
                case 'multipart/form-data':
                    foreach ($body['params'] as $key => $val) {
                        $requestBody['multipart'][] = [
                            'name' => $key,
                            'contents' => $val,
                        ];
                    }
                    break;
                default:
                    $requestBody = [];
                    break;
            }
        }
        $options = [
            'base_uri' => $parseUrl['scheme'] . '://' . $parseUrl['host'] . ':' . (isset($parseUrl['port']) ? $parseUrl['port'] : ''),
            'timeout' => 10,
            'verify' => false,
            'http_errors' => false,
            'debug' => (env('ENV') == 'proc') ? false : true,
            'swoole' => [
                'timeout' => 10,
                'socket_buffer_size' => 1024 * 1024 * 2,
            ],
            'query' => $queryArr,
            'auth' => $auth,
            'headers' => $headers,
        ];
        $bodyKeyArr = array_keys($requestBody);
        if (count($bodyKeyArr) > 0) {
            $bodyKey = $bodyKeyArr[0];
            $options[$bodyKey] = $requestBody[$bodyKey];
        }
        $this->stdOutLogger->info('【protocol-monitor】 ' . $this->monitorInfo->getTaskFlag() . ' protocol detect client init config ' . json_encode($options) . '---parseUlr' . json_encode($parseUrl));
        $this->dataSourceLogger->info('【protocol-monitor】 ' . $this->monitorInfo->getTaskFlag() . ' protocol detect client init config ' . json_encode($options) . '---parseUlr' . json_encode($parseUrl));
        $this->httpClient = $this->guzzleCreator->create(config('monitor.guzzle', []), $options);
        $this->uri = $parseUrl['path'];
        $this->method = $method;
    }
}
