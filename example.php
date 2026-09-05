<?php
/**
 * ws-node PHP 命令行客户端示例
 * 用法: php example.php [命令] [参数]
 * 
 * 命令:
 *   connect <节点地址>    - 连接节点并注册
 *   send <目标ID> <消息>  - 发送消息
 *   broadcast <消息>      - 广播消息
 *   online               - 获取在线列表
 *   help                 - 显示帮助
 * 
 * 需要安装: composer require textalk/websocket
 */

require_once __DIR__ . '/vendor/autoload.php';

use WebSocket\Client;

// ============ 配置 ============
define('MY_ID', generateId());
define('WS_NODE', 'ws://localhost:8080');

// ============ 工具函数 ============
function generateId() {
    return substr(md5(uniqid(mt_rand(), true)), 0, 20) .
           substr(md5(uniqid(mt_rand(), true)), 0, 20) .
           substr(md5(uniqid(mt_rand(), true)), 0, 20);
}

function logMsg($type, $message, $data = null) {
    $time = date('Y-m-d H:i:s');
    $prefix = match($type) {
        'info' => 'ℹ️',
        'success' => '✅',
        'error' => '❌',
        'warn' => '⚠️',
        'send' => '📨',
        'receive' => '📩',
        'broadcast' => '📢',
        default => '📌'
    };
    echo "[$time] $prefix $message";
    if ($data) {
        echo ' ' . json_encode($data, JSON_UNESCAPED_UNICODE);
    }
    echo PHP_EOL;
}

// ============ WebSocket 客户端类 ============
class WsNodeClient {
    private $client;
    private $id;
    private $nodeUrl;
    private $isRegistered = false;
    private $running = true;
    
    public function __construct($id, $nodeUrl) {
        $this->id = $id;
        $this->nodeUrl = $nodeUrl;
    }
    
    public function connect() {
        try {
            logMsg('info', "正在连接节点: {$this->nodeUrl}");
            $this->client = new Client($this->nodeUrl);
            logMsg('success', '✅ WebSocket 连接成功');
            
            // 注册
            $this->register();
            
            // 启动消息监听（非阻塞模式，用于命令行交互）
            $this->listen();
            
        } catch (Exception $e) {
            logMsg('error', "连接失败: " . $e->getMessage());
            exit(1);
        }
    }
    
    private function register() {
        $msg = json_encode([
            'type' => 'register',
            'id' => $this->id
        ]);
        $this->client->send($msg);
        logMsg('info', "📤 发送注册: {$this->id}");
        
        // 等待注册响应
        $response = $this->client->receive();
        $data = json_decode($response, true);
        
        if ($data && $data['type'] === 'registered') {
            $this->isRegistered = true;
            logMsg('success', "✅ 注册成功！在线 {$data['onlineCount']} 人");
            logMsg('info', "📌 你的ID: {$this->id}");
        } else {
            logMsg('error', "注册失败: " . ($data['message'] ?? '未知错误'));
            exit(1);
        }
    }
    
    private function listen() {
        // 使用非阻塞方式监听消息
        stream_set_blocking($this->client->getStream(), false);
        
        // 轮询接收消息
        while ($this->running) {
            try {
                // 检查是否有消息
                $read = [$this->client->getStream()];
                $write = null;
                $except = null;
                
                if (stream_select($read, $write, $except, 0, 100000) > 0) {
                    $message = $this->client->receive();
                    if ($message) {
                        $this->handleMessage(json_decode($message, true));
                    }
                }
                
                // 检查用户输入
                if (function_exists('stream_select')) {
                    $stdin = [STDIN];
                    if (stream_select($stdin, $write, $except, 0, 0) > 0) {
                        $input = trim(fgets(STDIN));
                        if ($input) {
                            $this->processCommand($input);
                        }
                    }
                }
                
                usleep(10000); // 10ms
                
            } catch (Exception $e) {
                if (strpos($e->getMessage(), 'closed') !== false) {
                    logMsg('warn', '⚠️ 连接已关闭');
                    $this->running = false;
                } else {
                    logMsg('error', "监听错误: " . $e->getMessage());
                }
            }
        }
    }
    
    private function handleMessage($msg) {
        if (!$msg) return;
        
        switch ($msg['type']) {
            case 'message':
                logMsg('receive', "📩 来自 {$msg['from']}: {$msg['content']}");
                break;
                
            case 'broadcast':
                logMsg('broadcast', "📢 {$msg['from']} 广播: {$msg['content']}");
                break;
                
            case 'offline_message':
                logMsg('receive', "💾 离线消息来自 {$msg['from']}: {$msg['content']}");
                break;
                
            case 'online_list':
                $count = $msg['count'];
                $list = implode(', ', array_slice($msg['clients'], 0, 5));
                if ($count > 5) $list .= " ... 共 {$count} 人";
                logMsg('info', "👥 在线列表 ({$count}人): {$list}");
                break;
                
            case 'sent':
                $status = $msg['status'] ?? 'unknown';
                logMsg('info', "📤 发送状态: {$status}");
                break;
                
            case 'ping':
                // 响应心跳
                $this->client->send(json_encode(['type' => 'pong']));
                break;
                
            case 'error':
                logMsg('error', "❌ 服务器错误: {$msg['message']}");
                break;
                
            default:
                logMsg('info', "📦 收到消息: " . json_encode($msg));
        }
    }
    
    private function processCommand($input) {
        $parts = explode(' ', $input, 3);
        $cmd = $parts[0];
        
        switch ($cmd) {
            case 'send':
            case 's':
                if (isset($parts[1]) && isset($parts[2])) {
                    $this->sendMessage($parts[1], $parts[2]);
                } else {
                    logMsg('warn', '用法: send <目标ID> <消息>');
                }
                break;
                
            case 'broadcast':
            case 'b':
                if (isset($parts[1])) {
                    $this->broadcast($parts[1]);
                } else {
                    logMsg('warn', '用法: broadcast <消息>');
                }
                break;
                
            case 'online':
            case 'o':
                $this->getOnline();
                break;
                
            case 'help':
            case 'h':
                $this->showHelp();
                break;
                
            case 'exit':
            case 'quit':
            case 'q':
                logMsg('info', '👋 退出');
                $this->running = false;
                $this->client->close();
                exit(0);
                break;
                
            default:
                logMsg('warn', "未知命令: {$cmd}，输入 help 查看帮助");
        }
    }
    
    public function sendMessage($targetId, $content) {
        if (!$this->isRegistered) {
            logMsg('error', '请先注册');
            return;
        }
        
        $msg = json_encode([
            'type' => 'send',
            'targetId' => $targetId,
            'content' => $content
        ]);
        $this->client->send($msg);
        logMsg('send', "📤 发送到 {$targetId}: {$content}");
    }
    
    public function broadcast($content) {
        if (!$this->isRegistered) {
            logMsg('error', '请先注册');
            return;
        }
        
        $msg = json_encode([
            'type' => 'broadcast',
            'content' => $content
        ]);
        $this->client->send($msg);
        logMsg('broadcast', "📢 广播: {$content}");
    }
    
    public function getOnline() {
        if (!$this->isRegistered) {
            logMsg('error', '请先注册');
            return;
        }
        
        $msg = json_encode(['type' => 'get_online']);
        $this->client->send($msg);
    }
    
    private function showHelp() {
        echo PHP_EOL;
        echo "📖 可用命令:" . PHP_EOL;
        echo "  send <目标ID> <消息>   - 发送私聊消息" . PHP_EOL;
        echo "  s <目标ID> <消息>      - 同上（简写）" . PHP_EOL;
        echo "  broadcast <消息>        - 广播消息给所有人" . PHP_EOL;
        echo "  b <消息>               - 同上（简写）" . PHP_EOL;
        echo "  online                 - 获取在线列表" . PHP_EOL;
        echo "  o                      - 同上（简写）" . PHP_EOL;
        echo "  help                   - 显示此帮助" . PHP_EOL;
        echo "  h                      - 同上（简写）" . PHP_EOL;
        echo "  exit / quit / q        - 退出程序" . PHP_EOL;
        echo PHP_EOL;
        echo "💡 提示: 直接输入命令即可，无需前缀" . PHP_EOL;
        echo "📌 你的ID: {$this->id}" . PHP_EOL;
        echo PHP_EOL;
    }
}

// ============ 主程序 ============
echo PHP_EOL;
echo "╔═══════════════════════════════════════════════════════╗" . PHP_EOL;
echo "║  🌐 ws-node PHP 命令行客户端                         ║" . PHP_EOL;
echo "║  去中心化 WebSocket 公共节点网络                     ║" . PHP_EOL;
echo "╚═══════════════════════════════════════════════════════╝" . PHP_EOL;
echo PHP_EOL;

// 解析命令行参数
$args = array_slice($argv, 1);
$nodeUrl = WS_NODE;
$id = MY_ID;

if (count($args) > 0) {
    $cmd = $args[0];
    
    switch ($cmd) {
        case 'connect':
            if (isset($args[1])) {
                $nodeUrl = $args[1];
            }
            // 继续执行
            break;
            
        case 'help':
        case '--help':
        case '-h':
            echo "用法: php example.php [命令] [参数]" . PHP_EOL . PHP_EOL;
            echo "命令:" . PHP_EOL;
            echo "  connect <节点地址>  - 连接到指定节点" . PHP_EOL;
            echo "  help                - 显示此帮助" . PHP_EOL . PHP_EOL;
            echo "示例:" . PHP_EOL;
            echo "  php example.php connect ws://192.168.1.100:8080" . PHP_EOL;
            exit(0);
            
        case 'send':
            if (count($args) < 3) {
                echo "❌ 用法: php example.php send <目标ID> <消息>" . PHP_EOL;
                exit(1);
            }
            // 快速发送模式
            $client = new WsNodeClient($id, $nodeUrl);
            $client->connect();
            $client->sendMessage($args[1], $args[2]);
            $client->getOnline();
            sleep(1);
            exit(0);
            
        default:
            // 可能是直接指定节点地址
            if (strpos($cmd, 'ws://') === 0 || strpos($cmd, 'wss://') === 0) {
                $nodeUrl = $cmd;
            }
    }
}

// 交互模式
echo "📌 你的ID: {$id}" . PHP_EOL;
echo "🔗 连接节点: {$nodeUrl}" . PHP_EOL;
echo PHP_EOL;

$client = new WsNodeClient($id, $nodeUrl);
$client->connect();

// 显示帮助
echo PHP_EOL;
echo "💡 输入 help 查看所有命令" . PHP_EOL;
echo "📌 你的ID: {$id}" . PHP_EOL;
echo PHP_EOL;

// 保持程序运行
while (true) {
    usleep(100000);
}