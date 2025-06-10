class Dashboard {
    constructor() {
        this.charts = {};
        this.lastUpdateTime = null;
        this.alertCount = 0;
        this.logs = [];
        this.maxLogs = 100;
        
        this.init();
    }
    
    init() {
        this.setupEventListeners();
        this.startDataPolling();
        this.addLog('info', 'Dashboard initialized');
        
        // Load initial batch results if needed
        this.loadBatchResults();
    }
    
    setupEventListeners() {
        // Refresh button
        document.addEventListener('keydown', (e) => {
            if (e.key === 'r' && e.ctrlKey) {
                e.preventDefault();
                this.refreshData();
            }
        });
        
        // Handle visibility change to pause/resume updates
        document.addEventListener('visibilitychange', () => {
            if (document.hidden) {
                this.addLog('info', 'Dashboard paused (tab hidden)');
            } else {
                this.addLog('info', 'Dashboard resumed');
                this.refreshData();
            }
        });
    }
    
    async loadBatchResults() {
        try {
            const response = await fetch('/api/batch-metrics');
            if (response.ok) {
                const result = await response.json();
                this.addLog('info', result.message || 'Batch results loaded');
            } else {
                this.addLog('warning', 'No batch results endpoint available');
            }
        } catch (error) {
            this.addLog('warning', 'No batch results to load initially');
        }
    }
    
    async startDataPolling() {
        // Initial load
        await this.refreshData();
        
        // Poll every 30 seconds for batch data
        setInterval(() => {
            if (!document.hidden) {
                this.refreshData();
            }
        }, 30000);
        
        // Poll every 15 seconds for alerts (if real-time is available)
        setInterval(() => {
            if (!document.hidden) {
                this.updateAlerts();
            }
        }, 15000);
    }
    
    async refreshData() {
        try {
            this.updateStatus('loading');
            const response = await fetch('/api/dashboard-stats');
            
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }
            
            const data = await response.json();
            console.log('Dashboard data received:', data);
            
            if (data.error) {
                throw new Error(data.error);
            }
            
            this.updateDashboard(data);
            this.updateStatus('online');
            this.addLog('info', 'Data refreshed successfully');
            
        } catch (error) {
            console.error('Error fetching data:', error);
            this.updateStatus('offline');
            this.addLog('error', `Failed to refresh data: ${error.message}`);
            
            // Show fallback data or empty state
            this.showNoDataState();
        }
    }
    
    showNoDataState() {
        const elements = {
            'total-comments': '0',
            'hate-percentage': '0%',
            'avg-comment-length': '0',
            'sentiment-score': '0',
            'realtime-comments': '0',
            'alerts-today': '0',
            'unique-users': '0',
            'processing-rate': '0/min'
        };
        
        Object.entries(elements).forEach(([id, value]) => {
            const element = document.getElementById(id);
            if (element) {
                element.textContent = value;
            }
        });
        
        // Clear containers
        const containers = ['keywords-container', 'alerts-container', 'samples-container'];
        containers.forEach(containerId => {
            const container = document.getElementById(containerId);
            if (container) {
                container.innerHTML = '<div class="no-data">No data available</div>';
            }
        });
    }
    
    updateDashboard(data) {
        console.log('Updating dashboard with data:', data);
        
        // Handle direct batch results format
        if (data.sentiment_distribution) {
            this.updateBatchMetrics(data);
            this.updateCharts(data);
            this.updateTopWords(data.top_words);
            this.updateCommentSamples(data.sentiment_samples);
        }
        // Handle wrapped format
        else if (data.batch_analytics) {
            this.updateBatchMetrics(data.batch_analytics);
            this.updateCharts(data.batch_analytics);
            this.updateTopWords(data.batch_analytics.top_words);
            this.updateCommentSamples(data.batch_analytics.sentiment_samples);
        }
        
        // Handle alerts
        if (data.recent_alerts) {
            this.displayAlerts(data.recent_alerts);
        }
        
        this.lastUpdateTime = new Date();
        this.addLog('info', `Dashboard updated at ${this.lastUpdateTime.toLocaleTimeString()}`);
    }
    
    updateBatchMetrics(data) {
        console.log('Updating batch metrics with:', data);
        
        // Batch Processing Metrics
        const totalComments = data.hate_speech_metrics?.total_comments || 
                            data.processed_comments?.length || 0;
        const hatePercentage = data.hate_speech_metrics?.hate_speech_percentage || 0;
        const avgLength = data.comment_length_stats?.mean || 0;
        
        // Calculate average sentiment score
        let avgSentiment = 0;
        if (data.sentiment_samples && data.sentiment_samples.length > 0) {
            const totalScore = data.sentiment_samples.reduce((sum, sample) => 
                sum + (sample.sentiment_score || 0), 0);
            avgSentiment = totalScore / data.sentiment_samples.length;
        }
        
        const batchElements = {
            'total-comments': this.formatNumber(totalComments),
            'hate-percentage': `${hatePercentage.toFixed(1)}%`,
            'avg-comment-length': avgLength.toFixed(0),
            'sentiment-score': avgSentiment.toFixed(2)
        };
        
        Object.entries(batchElements).forEach(([id, value]) => {
            const element = document.getElementById(id);
            if (element) {
                this.animateNumberChange(element, value);
            }
        });
        
        // Real-time metrics (placeholder for now)
        const realtimeElements = {
            'realtime-comments': this.formatNumber(totalComments),
            'alerts-today': this.alertCount.toString(),
            'unique-users': this.formatNumber(data.processed_comments?.length || 0),
            'processing-rate': `${Math.round(totalComments / 60)}/min`
        };
        
        Object.entries(realtimeElements).forEach(([id, value]) => {
            const element = document.getElementById(id);
            if (element) {
                this.animateNumberChange(element, value);
            }
        });
    }
    
    updateTopWords(topWords) {
        const container = document.getElementById('keywords-container');
        if (!container) return;
        
        container.innerHTML = '';
        
        if (!topWords || topWords.length === 0) {
            container.innerHTML = '<div class="no-data">No top words available</div>';
            return;
        }
        
        topWords.slice(0, 10).forEach(wordData => {
            const tag = document.createElement('div');
            tag.className = 'keyword-tag';
            tag.innerHTML = `
                <span class="keyword-word">${wordData.word}</span>
                <span class="keyword-count">${wordData.count}</span>
            `;
            container.appendChild(tag);
        });
    }
    
    updateCommentSamples(samples) {
        const container = document.getElementById('samples-container');
        if (!container) return;
        
        container.innerHTML = '';
        
        if (!samples || samples.length === 0) {
            container.innerHTML = '<div class="no-data">No comment samples available</div>';
            return;
        }
        
        samples.slice(0, 5).forEach(sample => {
            const sampleDiv = document.createElement('div');
            sampleDiv.className = `comment-sample sentiment-${sample.sentiment}`;
            sampleDiv.innerHTML = `
                <div class="sample-header">
                    <span class="sample-user">@${sample.username}</span>
                    <span class="sample-sentiment ${sample.sentiment}">${sample.sentiment}</span>
                    <span class="sample-score">${(sample.sentiment_score || 0).toFixed(2)}</span>
                </div>
                <div class="sample-comment">${sample.comment}</div>
            `;
            container.appendChild(sampleDiv);
        });
    }
    
    animateNumberChange(element, newValue) {
        element.style.transform = 'scale(1.1)';
        element.style.transition = 'transform 0.3s ease';
        
        setTimeout(() => {
            element.textContent = newValue;
            element.style.transform = 'scale(1)';
        }, 150);
    }
    
    async updateAlerts() {
        try {
            const response = await fetch('/api/speed-alerts');
            if (response.ok) {
                const data = await response.json();
                if (data.alerts && data.alerts.length > 0) {
                    this.displayAlerts(data.alerts.slice(-10));
                }
            }
        } catch (error) {
            console.log('No real-time alerts available:', error.message);
        }
    }
    
    displayAlerts(alerts) {
        const container = document.getElementById('alerts-container');
        if (!container) return;
        
        container.innerHTML = '';
        
        if (!alerts || alerts.length === 0) {
            container.innerHTML = '<div class="no-data">No recent alerts</div>';
            return;
        }
        
        alerts.reverse().forEach((alert, index) => {
            const alertElement = this.createAlertElement(alert, index === 0);
            container.appendChild(alertElement);
        });
        
        this.alertCount = alerts.length;
    }
    
    createAlertElement(alert, isNew = false) {
        const alertDiv = document.createElement('div');
        alertDiv.className = `alert-item ${isNew ? 'new' : ''}`;
        
        const timestamp = new Date(alert.timestamp || Date.now()).toLocaleTimeString();
        
        alertDiv.innerHTML = `
            <div class="alert-info">
                <div class="alert-type">${alert.event_type || 'System Alert'}</div>
                <div class="alert-time">${timestamp}</div>
                <div class="alert-details">${alert.message || 'No details available'}</div>
            </div>
            <div class="alert-confidence">${alert.confidence || '-'}</div>
        `;
        
        return alertDiv;
    }
    
    updateStatus(status) {
        const dot = document.getElementById('status-dot');
        const text = document.getElementById('status-text');
        
        if (dot && text) {
            dot.className = `status-dot ${status}`;
            
            const statusTexts = {
                'online': 'System Online',
                'offline': 'System Offline',
                'loading': 'Loading...'
            };
            
            text.textContent = statusTexts[status] || 'Unknown Status';
        }
    }
    
    addLog(level, message) {
        const timestamp = new Date().toLocaleTimeString();
        const logEntry = {
            timestamp,
            level,
            message
        };
        
        this.logs.unshift(logEntry);
        if (this.logs.length > this.maxLogs) {
            this.logs = this.logs.slice(0, this.maxLogs);
        }
        
        this.updateLogsDisplay();
    }
    
    updateLogsDisplay() {
        const container = document.getElementById('logs-container');
        if (!container) return;
        
        container.innerHTML = '';
        
        this.logs.slice(0, 20).forEach(log => {
            const logDiv = document.createElement('div');
            logDiv.className = 'log-entry';
            logDiv.innerHTML = `
                <span class="log-timestamp">[${log.timestamp}]</span>
                <span class="log-level-${log.level}">[${log.level.toUpperCase()}]</span>
                ${log.message}
            `;
            container.appendChild(logDiv);
        });
    }
    
    formatNumber(num) {
        if (typeof num !== 'number') return '0';
        
        if (num >= 1000000) {
            return (num / 1000000).toFixed(1) + 'M';
        } else if (num >= 1000) {
            return (num / 1000).toFixed(1) + 'K';
        }
        return num.toString();
    }
}

// Initialize dashboard when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.dashboard = new Dashboard();
});