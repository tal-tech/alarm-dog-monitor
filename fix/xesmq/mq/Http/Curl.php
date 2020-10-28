<?php

declare(strict_types=1);

namespace XesMq\Mq\Http;

use Exception;

class Curl
{
    public static function post($urlList, $timeout, $postData, $appid, $appkey)
    {
        $errorMsg = '';
        foreach ($urlList as $url) {
            $errorMsg .= "{$url}:";
            $curl = curl_init();
            curl_setopt_array($curl, [
                CURLOPT_URL => $url,
                CURLOPT_RETURNTRANSFER => true,
                CURLOPT_TIMEOUT => $timeout,
                CURLOPT_HTTP_VERSION => CURL_HTTP_VERSION_1_1,
                CURLOPT_CUSTOMREQUEST => 'POST',
                CURLOPT_POSTFIELDS => $postData,
                CURLOPT_HTTPHEADER => [
                    'Content-Type: application/json',
                ],
            ]);

            $response = json_decode(curl_exec($curl), true);
            $err = curl_error($curl);

            curl_close($curl);

            if ($err) {
                //TODO LOG
                $errorMsg .= "{$err}\n";
                continue;
            }

            return $response;
        }
        throw new Exception($errorMsg, 500);
    }
}
