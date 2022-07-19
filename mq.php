<?php
class MQ {
    /**
     * 1.安装php-amqplib 服务
     * 在composer.json配置
     *
     * {
     * "require": {
     * "php-amqplib/php-amqplib": ">=2.6.1"
     * }
     * }
     * 2.执行composer.phar install 来安装
     *
     * 3.引入mq文件
     *
     * define('EXTEND_PATH', '../vendor/autoload.php');
     * use PhpAmqpLib\Connection\AMQPStreamConnection;
     */


    /**
     * MQ生产数据
     * @param $queueName 队列名称
     * @param $msg 发送数据
     * @return
     */
    public function MqPublish($queueName, $msg = [])
    {
        try {
            if (empty($queueName))
                return false;
            //获取mq配置
            $mqConfig = $this->getConfig();
            //创建连接和channel
            $connection = new AMQPStreamConnection($mqConfig['host'], $mqConfig['port'], $mqConfig['user'], $mqConfig['password']);
            $channel = $connection->channel();
            $name = $queueName;
            $type = "direct";
            $passive = false;
            $durable = true;
            $auto_delete = true;
            $channel->exchange_declare($name, $type, $passive, $durable, $auto_delete);
            $message = new AMQPMessage('{"data_id":184981}');
            $channel->basic_publish($message, '', $queueName);//发送数据到MQ
            $channel->close();
            $connection->close();

            return true;
        } catch (\Exception $e) {
            return false;
        }
    }

    /**
     * MQ消费数据 监视数据
     * @param $queueName 队列名称
     * @name MqConsumer
     * @return
     */
    public function mqConsumer()
    {
        try {
            $queueName = $this->getx('queue', 'complex_info_test');
            if (empty($queueName)) {
                echo "not queue ";
                die;
            }
            //创建连接和channel
            $connection = new AMQPStreamConnection(C('config_mq.host'), C('config_mq.port'), C('config_mq.user'), C('config_mq.password'));
            $channel = $connection->channel();
            $channel->queue_declare($queueName, false, true, false, false);
            echo ' [*] Waiting for messages. To exit press CTRL+C', "\n";
            $callback = function ($msg) {
                $returnData = json_decode($msg->body, true);
                echo " [x] Received ", $msg->body, "\n";
            };
            $channel->basic_consume($queueName, '', false, false, false, false, $callback);//消费出数据
            while (count($channel->callbacks)) {
                $channel->wait();
            }
            $channel->close();
            $connection->close();
        } catch (\Exception $e) {

            // return false;
        }
    }
}
