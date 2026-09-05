const WebSocket = require('ws');
const http = require('http');
const https = require('https');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

// ============ 配置 ============
const CONFIG = {
  port: 8080,
  ssl: {
    enabled: false,            // 设为 true 启用 WSS
    key: '/path/to/privkey.pem',
    cert: '/path/to/fullchain.pem'
  },
  maxConnections: 10000,
  maxMessageSize: 1024 * 10,   // 10KB 单条消息限制
  rateLimit: {
    enabled: true,
    maxMessagesPerMinute: 60,  // 每分钟最多 60 条
    maxConnectionsPerIP: 5,     // 单 IP 最多 5 个连接
    banDuration: 3600000       // 封禁时长 1小时 (毫秒)
  },
  auth: {
    enabled: false,            // 设为 true 启用 Token 验证
    tokenSecret: 'your-secret-key-change-me'
  },
  contentFilter: {
    enabled: true,
    blockedWords: ['fuck', 'shit', '混蛋', '傻逼']  // 敏感词列表
  }
};

// ============ 日志 ============
function log(type, message, data) {
  const time = new Date().toISOString();
  const entry = `[${time}] [${type}] ${message} ${data ? JSON.stringify(data) : ''}\n`;
  console.log(entry);
  const logDir = path.join(__dirname, 'logs');
  if (!fs.existsSync(logDir)) fs.mkdirSync(logDir);
  fs.appendFileSync(path.join(logDir, 'server.log'), entry, { flag: 'a' });
}

// ============ 创建服务器 ============
let server;

if (CONFIG.ssl.enabled) {
  server = https.createServer({
    key: fs.readFileSync(CONFIG.ssl.key),
    cert: fs.readFileSync(CONFIG.ssl.cert)
  }, handleHttp);
} else {
  server = http.createServer(handleHttp);
}

function handleHttp(req, res) {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      clients: clients.size,
      uptime: process.uptime(),
      mode: CONFIG.ssl.enabled ? 'wss' : 'ws',
      banned: banList.size
    }));
    return;
  }
  if (req.url === '/stats') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      clients: clients.size,
      banned: banList.size,
      totalMessages: totalMessages,
      uptime: process.uptime()
    }));
    return;
  }
  res.writeHead(404).end();
}

// ============ WebSocket 服务器 ============
const wss = new WebSocket.Server({
  server,
  maxPayload: CONFIG.maxMessageSize,
  verifyClient: (info, cb) => {
    // TLS 证书验证（仅 WSS 模式）
    if (CONFIG.ssl.enabled) {
      const cert = info.req.socket.getPeerCertificate();
      if (!cert || !cert.subject) {
        log('WARN', '客户端 TLS 证书验证失败', info.req.socket.remoteAddress);
        cb(false, 403, 'TLS 证书验证失败');
        return;
      }
    }
    cb(true);
  }
});

// ============ 存储 ============
const clients = new Map();
const ipConnections = new Map();
const ipMessageCount = new Map();
const banList = new Map();        // ip -> banUntil
const offlineMessages = new Map();
const authTokens = new Map();     // clientId -> token
const failedAttempts = new Map(); // ip -> {count, firstAttempt}

let totalMessages = 0;

// ============ 安全检查函数 ============

// 1. 检查是否被封禁
function isBanned(ip) {
  if (banList.has(ip)) {
    const banUntil = banList.get(ip);
    if (Date.now() < banUntil) {
      return true;
    } else {
      banList.delete(ip);
    }
  }
  return false;
}

// 2. 封禁 IP
function banIP(ip, duration = CONFIG.rateLimit.banDuration) {
  banList.set(ip, Date.now() + duration);
  log('WARN', `🚫 IP 被封禁 ${ip}，时长 ${duration / 60000} 分钟`);
  
  // 踢掉该 IP 的所有连接
  if (ipConnections.has(ip)) {
    const connections = ipConnections.get(ip);
    for (const ws of connections) {
      try {
        ws.close(1008, 'IP 被封禁');
      } catch (e) {}
    }
    ipConnections.delete(ip);
  }
}

// 3. 记录失败尝试
function recordFailedAttempt(ip) {
  const now = Date.now();
  if (!failedAttempts.has(ip)) {
    failedAttempts.set(ip, { count: 1, firstAttempt: now });
    return;
  }
  
  const record = failedAttempts.get(ip);
  // 如果超过 10 分钟，重置
  if (now - record.firstAttempt > 600000) {
    failedAttempts.set(ip, { count: 1, firstAttempt: now });
    return;
  }
  
  record.count++;
  if (record.count >= 10) {
    banIP(ip, 3600000); // 封禁 1 小时
    failedAttempts.delete(ip);
  }
}

// 4. 内容过滤（敏感词检测）
function containsBlockedWords(content) {
  if (!CONFIG.contentFilter.enabled) return false;
  
  const lowerContent = content.toLowerCase();
  for (const word of CONFIG.contentFilter.blockedWords) {
    if (lowerContent.includes(word.toLowerCase())) {
      return true;
    }
  }
  return false;
}

// 5. 生成 Token（用于身份验证）
function generateToken(clientId) {
  const payload = `${clientId}:${Date.now()}`;
  const signature = crypto
    .createHmac('sha256', CONFIG.auth.tokenSecret)
    .update(payload)
    .digest('hex');
  return Buffer.from(`${payload}:${signature}`).toString('base64');
}

// 6. 验证 Token
function verifyToken(token) {
  try {
    const decoded = Buffer.from(token, 'base64').toString();
    const [clientId, timestamp, signature] = decoded.split(':');
    
    // 验证签名
    const expectedSignature = crypto
      .createHmac('sha256', CONFIG.auth.tokenSecret)
      .update(`${clientId}:${timestamp}`)
      .digest('hex');
    
    if (signature !== expectedSignature) return null;
    
    // Token 有效期 24 小时
    if (Date.now() - parseInt(timestamp) > 86400000) return null;
    
    return clientId;
  } catch (e) {
    return null;
  }
}

// ============ 发送消息 ============
function sendToClient(clientId, data) {
  const client = clients.get(clientId);
  if (!client) return false;
  try {
    if (client.ws.readyState === WebSocket.OPEN) {
      client.ws.send(JSON.stringify(data));
      return true;
    }
  } catch (e) {
    log('ERROR', `发送失败 ${clientId}`, e.message);
  }
  return false;
}

// ============ 限流检查 ============
function checkRateLimit(ip) {
  if (!CONFIG.rateLimit.enabled) return true;
  
  const now = Date.now();
  const record = ipMessageCount.get(ip);
  
  if (!record) {
    ipMessageCount.set(ip, { count: 1, resetTime: now + 60000 });
    return true;
  }
  
  if (now > record.resetTime) {
    ipMessageCount.set(ip, { count: 1, resetTime: now + 60000 });
    return true;
  }
  
  if (record.count >= CONFIG.rateLimit.maxMessagesPerMinute) {
    record.count++;
    return false;
  }
  
  record.count++;
  return true;
}

// ============ 清理客户端 ============
function cleanupClient(clientId) {
  const client = clients.get(clientId);
  if (client) {
    const ipSet = ipConnections.get(client.ip);
    if (ipSet) {
      ipSet.delete(client.ws);
      if (ipSet.size === 0) ipConnections.delete(client.ip);
    }
    clients.delete(clientId);
    authTokens.delete(clientId);
    log('INFO', `❌ 客户端断开 ${clientId}`);
  }
}

// ============ WebSocket 连接处理 ============
wss.on('connection', (ws, req) => {
  const ip = req.socket.remoteAddress || 'unknown';
  
  // ===== 安全检查 1: 是否被封禁 =====
  if (isBanned(ip)) {
    log('WARN', `拒绝被封禁 IP 连接: ${ip}`);
    ws.close(1008, 'IP 被封禁');
    return;
  }
  
  // ===== 安全检查 2: IP 连接数限制 =====
  if (!ipConnections.has(ip)) {
    ipConnections.set(ip, new Set());
  }
  const ipSet = ipConnections.get(ip);
  if (ipSet.size >= CONFIG.rateLimit.maxConnectionsPerIP) {
    log('WARN', `IP ${ip} 连接数已达上限`);
    recordFailedAttempt(ip);
    ws.close(1008, '连接数过多');
    return;
  }
  ipSet.add(ws);
  
  // ===== 安全检查 3: 总连接数限制 =====
  if (clients.size >= CONFIG.maxConnections) {
    log('WARN', `连接数已达上限 ${CONFIG.maxConnections}`);
    ws.close(1008, '服务器已满');
    return;
  }
  
  let clientId = null;
  let isRegistered = false;
  
  log('INFO', `新连接来自 ${ip}`);
  
  // ===== 消息处理 =====
  ws.on('message', (raw) => {
    try {
      // ===== 安全检查 4: 消息大小限制 =====
      if (raw.length > CONFIG.maxMessageSize) {
        ws.send(JSON.stringify({ type: 'error', message: '消息过大' }));
        recordFailedAttempt(ip);
        return;
      }
      
      // ===== 安全检查 5: 限流 =====
      if (!checkRateLimit(ip)) {
        const msg = JSON.parse(raw);
        // 如果超限，记录失败尝试
        if (msg.type !== 'pong') {
          recordFailedAttempt(ip);
        }
        ws.send(JSON.stringify({ type: 'error', message: '消息频率过高，请稍后再试' }));
        return;
      }
      
      const msg = JSON.parse(raw);
      totalMessages++;
      
      // ===== 安全检查 6: 内容过滤（敏感词） =====
      if (msg.content && containsBlockedWords(msg.content)) {
        log('WARN', `敏感词拦截 ${ip}: ${msg.content}`);
        ws.send(JSON.stringify({ 
          type: 'error', 
          message: '消息包含敏感词' 
        }));
        recordFailedAttempt(ip);
        return;
      }
      
      // ----- 1. 注册（带 Token 验证） -----
      if (msg.type === 'register') {
        if (isRegistered) {
          ws.send(JSON.stringify({ type: 'error', message: '已经注册过了' }));
          return;
        }
        
        clientId = msg.id;
        const token = msg.token || null;
        
        // ID 格式验证
        if (!clientId || typeof clientId !== 'string' || clientId.length !== 60) {
          ws.send(JSON.stringify({ type: 'error', message: 'ID必须是60位字符串' }));
          recordFailedAttempt(ip);
          return;
        }
        
        // ID 格式检查：只允许字母和数字
        if (!/^[a-zA-Z0-9]{60}$/.test(clientId)) {
          ws.send(JSON.stringify({ type: 'error', message: 'ID只能包含字母和数字' }));
          recordFailedAttempt(ip);
          return;
        }
        
        // Token 验证（如果启用）
        if (CONFIG.auth.enabled) {
          if (!token) {
            ws.send(JSON.stringify({ type: 'error', message: '需要 Token 验证' }));
            recordFailedAttempt(ip);
            return;
          }
          
          const verifiedId = verifyToken(token);
          if (!verifiedId || verifiedId !== clientId) {
            ws.send(JSON.stringify({ type: 'error', message: 'Token 无效或已过期' }));
            recordFailedAttempt(ip);
            return;
          }
        }
        
        // 重复登录处理
        if (clients.has(clientId)) {
          const oldClient = clients.get(clientId);
          log('INFO', `重复登录 ${clientId}，踢掉旧连接`);
          try {
            oldClient.ws.send(JSON.stringify({ 
              type: 'error', 
              message: '其他设备登录，你被踢下线' 
            }));
            oldClient.ws.close(1008, '重复登录');
          } catch (e) {}
          cleanupClient(clientId);
        }
        
        // 注册新客户端
        const client = {
          ws: ws,
          clientId: clientId,
          ip: ip,
          messageQueue: [],
          connectedAt: Date.now()
        };
        clients.set(clientId, client);
        isRegistered = true;
        
        // 生成并保存 Token（如果启用）
        if (CONFIG.auth.enabled) {
          const newToken = generateToken(clientId);
          authTokens.set(clientId, newToken);
          ws.send(JSON.stringify({
            type: 'registered',
            message: '连接成功',
            clientId: clientId,
            onlineCount: clients.size,
            token: newToken  // 返回 Token 给客户端
          }));
        } else {
          ws.send(JSON.stringify({
            type: 'registered',
            message: '连接成功',
            clientId: clientId,
            onlineCount: clients.size
          }));
        }
        
        // 发送离线消息
        if (offlineMessages.has(clientId)) {
          const queue = offlineMessages.get(clientId);
          queue.forEach(msg => {
            ws.send(JSON.stringify({
              type: 'offline_message',
              from: msg.from,
              content: msg.content,
              time: msg.time
            }));
          });
          offlineMessages.delete(clientId);
          log('INFO', `发送离线消息 ${queue.length} 条给 ${clientId}`);
        }
        
        // 清除失败记录
        failedAttempts.delete(ip);
        log('INFO', `✅ 客户端注册成功 ${clientId} (IP: ${ip})`);
        return;
      }
      
      // ----- 2. 未注册拒绝服务 -----
      if (!isRegistered) {
        ws.send(JSON.stringify({ type: 'error', message: '请先注册' }));
        recordFailedAttempt(ip);
        return;
      }
      
      // ----- 3. 发送消息 -----
      if (msg.type === 'send') {
        const targetId = msg.targetId;
        const content = msg.content;
        
        if (!targetId || !content) {
          ws.send(JSON.stringify({ type: 'error', message: '缺少targetId或content' }));
          return;
        }
        
        // 不能给自己发消息
        if (targetId === clientId) {
          ws.send(JSON.stringify({ type: 'error', message: '不能给自己发消息' }));
          return;
        }
        
        const target = clients.get(targetId);
        if (target && target.ws.readyState === WebSocket.OPEN) {
          const success = sendToClient(targetId, {
            type: 'message',
            from: clientId,
            content: content,
            time: Date.now()
          });
          
          if (success) {
            log('INFO', `📨 ${clientId} -> ${targetId}: ${content.substring(0, 20)}`);
            ws.send(JSON.stringify({
              type: 'sent',
              targetId: targetId,
              status: 'delivered'
            }));
          } else {
            ws.send(JSON.stringify({
              type: 'error',
              message: '目标连接已断开'
            }));
          }
        } else {
          // 离线存储
          if (!offlineMessages.has(targetId)) {
            offlineMessages.set(targetId, []);
          }
          const queue = offlineMessages.get(targetId);
          queue.push({
            from: clientId,
            content: content,
            time: Date.now()
          });
          if (queue.length > 100) queue.shift();
          
          log('INFO', `💾 离线消息 ${clientId} -> ${targetId}`);
          ws.send(JSON.stringify({
            type: 'sent',
            targetId: targetId,
            status: 'offline_stored'
          }));
        }
        return;
      }
      
      // ----- 4. 广播消息 -----
      if (msg.type === 'broadcast') {
        const content = msg.content;
        if (!content) {
          ws.send(JSON.stringify({ type: 'error', message: '缺少content' }));
          return;
        }
        
        let count = 0;
        for (const [id, client] of clients) {
          if (id === clientId) continue;
          if (client.ws.readyState === WebSocket.OPEN) {
            client.ws.send(JSON.stringify({
              type: 'broadcast',
              from: clientId,
              content: content,
              time: Date.now()
            }));
            count++;
          }
        }
        log('INFO', `📢 广播消息 from ${clientId} 发送给 ${count} 人`);
        ws.send(JSON.stringify({
          type: 'sent',
          status: `broadcast_delivered to ${count} clients`
        }));
        return;
      }
      
      // ----- 5. 获取在线列表 -----
      if (msg.type === 'get_online') {
        const onlineList = Array.from(clients.keys());
        ws.send(JSON.stringify({
          type: 'online_list',
          clients: onlineList,
          count: onlineList.length
        }));
        return;
      }
      
      // ----- 6. 刷新 Token -----
      if (msg.type === 'refresh_token') {
        if (!CONFIG.auth.enabled) {
          ws.send(JSON.stringify({ type: 'error', message: 'Token 功能未启用' }));
          return;
        }
        
        const newToken = generateToken(clientId);
        authTokens.set(clientId, newToken);
        ws.send(JSON.stringify({
          type: 'token_refreshed',
          token: newToken
        }));
        return;
      }
      
      // ----- 7. 未知消息类型 -----
      ws.send(JSON.stringify({
        type: 'error',
        message: `未知消息类型: ${msg.type}`
      }));
      
    } catch (e) {
      log('ERROR', '消息解析失败', e.message);
      ws.send(JSON.stringify({
        type: 'error',
        message: '无效的JSON格式'
      }));
      recordFailedAttempt(ip);
    }
  });
  
  // ===== 断开连接 =====
  ws.on('close', () => {
    if (clientId && clients.has(clientId)) {
      cleanupClient(clientId);
    }
  });
  
  ws.on('error', (error) => {
    log('ERROR', `WebSocket错误 ${clientId || 'unknown'}`, error.message);
  });
});

// ============ 启动服务器 ============
const port = CONFIG.ssl.enabled ? CONFIG.ssl.port || 8443 : CONFIG.port;
server.listen(port, () => {
  log('INFO', `🚀 WebSocket服务器启动成功`);
  log('INFO', `📡 监听端口: ${port}`);
  log('INFO', `🔒 模式: ${CONFIG.ssl.enabled ? 'WSS (加密)' : 'WS (明文)'}`);
  log('INFO', `❤️  健康检查: http://localhost:${port}/health`);
  log('INFO', `👥 最大连接数: ${CONFIG.maxConnections}`);
  log('INFO', `🛡️  安全配置:`);
  log('INFO', `   - 限流: ${CONFIG.rateLimit.enabled ? '启用' : '禁用'}`);
  log('INFO', `   - 敏感词过滤: ${CONFIG.contentFilter.enabled ? '启用' : '禁用'}`);
  log('INFO', `   - Token验证: ${CONFIG.auth.enabled ? '启用' : '禁用'}`);
});

// ============ 优雅关闭 ============
process.on('SIGINT', () => {
  log('INFO', '🛑 收到关闭信号，正在清理...');
  
  for (const [id, client] of clients) {
    try {
      client.ws.close(1000, '服务器关闭');
    } catch (e) {}
  }
  
  server.close(() => {
    log('INFO', '✅ 服务器已关闭');
    process.exit(0);
  });
});

console.log('\n' + '='.repeat(60));
console.log('🛡️  安全增强版 WebSocket 服务器');
console.log('='.repeat(60));
console.log('📖 支持的指令:');
console.log('   register        - 注册客户端ID（支持Token）');
console.log('   send            - 发送消息给指定ID');
console.log('   broadcast       - 广播消息给所有人');
console.log('   get_online      - 获取在线列表');
console.log('   refresh_token   - 刷新Token（需启用）');
console.log('='.repeat(60));
console.log('🛡️  安全防护:');
console.log('   ✅ IP封禁机制 (10次失败自动封禁)');
console.log('   ✅ 连接数限制 (单IP最多5个)');
console.log('   ✅ 消息限流 (每分钟最多60条)');
console.log('   ✅ 敏感词过滤');
console.log('   ✅ Token身份验证 (可选)');
console.log('   ✅ 消息大小限制 (10KB)');
console.log('   ✅ WSS加密支持 (可选)');
console.log('='.repeat(60) + '\n');