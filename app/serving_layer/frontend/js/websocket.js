class WebSocketManager {
    constructor(dashboard) {
        this.dashboard = dashboard;
        this.socket = null;
        this.reconnectAttempts = 0;
        this.maxReconnectAttempts = 5;
        this.reconnectDelay = 1000;
        
        this.init();
    }
    
    init() {
        this.connect();
    }
    
    connect() {
        try {
            this.socket = io(window.location.origin);
            this.setupEventListeners();
            this.dashboard.addLog('info', 'WebSocket connection initiated');
        } catch (error) {
            this.dashboard.addLog('error', `WebSocket connection failed: ${error.message}`);
            this.scheduleReconnect();
        }
    }
    
    setupEventListeners() {
        this.socket.on('connect', () => {
            this.dashboard.addLog('info', 'WebSocket connected');
            this.dashboard.updateStatus('online');
            this.reconnectAttempts = 0;
        });
        
        this.socket.on('disconnect', () => {
            this.dashboard.addLog('warning', 'WebSocket disconnected');
            this.dashboard.updateStatus('offline');
            this.scheduleReconnect();
        });
        
        this.socket.on('status', (data) => {
            this.dashboard.addLog('info', `Server: ${data.msg}`);
        });
        
        this.socket.on('batch_update', (data) => {
            this.handleBatchUpdate(data);
        });
        
        this.socket.on('new_alert', (alert) => {
            this.handleNewAlert(alert);
        });
        
        this.socket.on('connect_error', (error) => {
            this.dashboard.addLog('error', `Connection error: ${error.message}`);
            this.scheduleReconnect();
        });
    }
    
    handleBatchUpdate(data) {
        this.dashboard.addLog('info', 'Received batch data update');
        
        if (data.analytics) {
            this.dashboard.updateMetrics(data.analytics);
            this.dashboard.updateCharts(data.analytics);
            this.dashboard.updateKeywords(data.analytics.top_keywords);
        }
    }
    
    handleNewAlert(alert) {
        this.dashboard.addLog('warning', `New alert: ${alert.comment.detected_categories.join(', ')}`);
        
        this.showNotification(alert);

        this.dashboard.updateAlerts();
    }
    
    showNotification(alert) {
        const notification = document.createElement('div');
        notification.className = 'toast-notification';
        notification.innerHTML = `
            <div class="toast-header">
                <strong>🚨 New Alert</strong>
                <button class="toast-close" onclick="this.parentElement.parentElement.remove()">×</button>
            </div>
            <div class="toast-body">
                ${alert.comment.detected_categories.join(', ')} detected with ${Math.round(alert.comment.confidence_score * 100)}% confidence
            </div>
        `;
        
        document.body.appendChild(notification);
        
        // Auto remove after 5 seconds
        setTimeout(() => {
            if (notification.parentElement) {
                notification.remove();
            }
        }, 5000);
        
        this.addToastStyles();
    }
    
    addToastStyles() {
        if (document.getElementById('toast-styles')) return;
        
        const style = document.createElement('style');
        style.id = 'toast-styles';
        style.textContent = `
            .toast-notification {
                position: fixed;
                top: 20px;
                right: 20px;
                background: white;
                border-radius: 8px;
                box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
                min-width: 300px;
                z-index: 1000;
                animation: slideInRight 0.3s ease;
            }
            
            .toast-header {
                display: flex;
                justify-content: space-between;
                align-items: center;
                padding: 12px 16px;
                background: #f8d7da;
                color: #721c24;
                border-radius: 8px 8px 0 0;
                border-bottom: 1px solid #f5c6cb;
            }
            
            .toast-body {
                padding: 12px 16px;
                color: #333;
                font-size: 14px;
            }
            
            .toast-close {
                background: none;
                border: none;
                font-size: 18px;
                cursor: pointer;
                color: #721c24;
                padding: 0;
                width: 20px;
                height: 20px;
                display: flex;
                align-items: center;
                justify-content: center;
            }
            
            @keyframes slideInRight {
                from {
                    transform: translateX(100%);
                    opacity: 0;
                }
                to {
                    transform: translateX(0);
                    opacity: 1;
                }
            }
        `;
        document.head.appendChild(style);
    }
    
    scheduleReconnect() {
        if (this.reconnectAttempts >= this.maxReconnectAttempts) {
            this.dashboard.addLog('error', 'Max reconnection attempts reached');
            return;
        }
        
        this.reconnectAttempts++;
        const delay = this.reconnectDelay * Math.pow(2, this.reconnectAttempts - 1);
        
        this.dashboard.addLog('info', `Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`);
        
        setTimeout(() => {
            this.connect();
        }, delay);
    }
    
    disconnect() {
        if (this.socket) {
            this.socket.disconnect();
            this.socket = null;
        }
    }
}

if (typeof Dashboard !== 'undefined') {
    const originalInit = Dashboard.prototype.init;
    
    Dashboard.prototype.init = function() {
        originalInit.call(this);
        this.wsManager = new WebSocketManager(this);
    };
}
