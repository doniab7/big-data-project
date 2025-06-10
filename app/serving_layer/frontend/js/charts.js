class ChartManager {
    constructor() {
        this.charts = {};
        this.initCharts();
    }
    
    initCharts() {
        this.createSentimentChart();
        this.createCommentLengthChart();
    }
    
    createSentimentChart() {
        const ctx = document.getElementById('sentimentChart');
        if (!ctx) return;
        
        this.charts.sentiment = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: ['Positive', 'Neutral', 'Negative'],
                datasets: [{
                    data: [0, 0, 0], // Start with zeros
                    backgroundColor: [
                        '#27ae60',
                        '#f39c12',
                        '#e74c3c'
                    ],
                    borderWidth: 0,
                    hoverOffset: 10
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            padding: 20,
                            usePointStyle: true,
                            font: {
                                size: 12
                            }
                        }
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const total = context.dataset.data.reduce((sum, val) => sum + val, 0);
                                if (total === 0) return `${context.label}: 0%`;
                                const percentage = ((context.parsed / total) * 100).toFixed(1);
                                return `${context.label}: ${context.parsed} (${percentage}%)`;
                            }
                        }
                    }
                },
                animation: {
                    animateRotate: true,
                    duration: 1000
                }
            }
        });
    }
    
    createCommentLengthChart() {
        const ctx = document.getElementById('volumeChart');
        if (!ctx) return;
        
        // Create bins for comment length distribution
        const lengthBins = ['0-20', '21-40', '41-60', '61-80', '81-100', '100+'];
        
        this.charts.commentLength = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: lengthBins,
                datasets: [{
                    label: 'Number of Comments',
                    data: new Array(6).fill(0), // Start with zeros
                    backgroundColor: 'rgba(102, 126, 234, 0.6)',
                    borderColor: '#667eea',
                    borderWidth: 2,
                    borderRadius: 4,
                    borderSkipped: false,
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    intersect: false,
                    mode: 'index'
                },
                plugins: {
                    legend: {
                        display: false
                    },
                    tooltip: {
                        backgroundColor: 'rgba(0, 0, 0, 0.8)',
                        titleColor: '#ffffff',
                        bodyColor: '#ffffff',
                        borderColor: '#667eea',
                        borderWidth: 1,
                        callbacks: {
                            title: function(context) {
                                return `Length: ${context[0].label} characters`;
                            },
                            label: function(context) {
                                return `Comments: ${context.parsed.y}`;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        grid: {
                            display: false
                        },
                        ticks: {
                            color: '#6c757d'
                        }
                    },
                    y: {
                        beginAtZero: true,
                        grid: {
                            color: 'rgba(0, 0, 0, 0.1)'
                        },
                        ticks: {
                            color: '#6c757d',
                            callback: function(value) {
                                return Math.floor(value);
                            }
                        }
                    }
                },
                animation: {
                    duration: 1500,
                    easing: 'easeInOutQuart'
                }
            }
        });
    }
    
    updateSentimentChart(sentimentData) {
        if (!this.charts.sentiment || !sentimentData) return;
        
        console.log('Updating sentiment chart with:', sentimentData);
        
        const chart = this.charts.sentiment;
        
        // Extract sentiment counts
        const positive = sentimentData.positive || 0;
        const neutral = sentimentData.neutral || 0;
        const negative = sentimentData.negative || 0;
        
        chart.data.datasets[0].data = [positive, neutral, negative];
        chart.update('active');
    }
    
    updateCommentLengthChart(commentSamples) {
        if (!this.charts.commentLength || !commentSamples) return;
        
        console.log('Updating comment length chart with samples:', commentSamples.length);
        
        const chart = this.charts.commentLength;
        
        // Create length distribution from comment samples
        const lengthBins = [0, 0, 0, 0, 0, 0]; // 0-20, 21-40, 41-60, 61-80, 81-100, 100+
        
        commentSamples.forEach(sample => {
            const length = sample.comment ? sample.comment.length : 0;
            
            if (length <= 20) {
                lengthBins[0]++;
            } else if (length <= 40) {
                lengthBins[1]++;
            } else if (length <= 60) {
                lengthBins[2]++;
            } else if (length <= 80) {
                lengthBins[3]++;
            } else if (length <= 100) {
                lengthBins[4]++;
            } else {
                lengthBins[5]++;
            }
        });
        
        chart.data.datasets[0].data = lengthBins;
        chart.update('active');
    }
    
    updateChartsWithNoData() {
        // Show empty state for charts when no data is available
        if (this.charts.sentiment) {
            this.charts.sentiment.data.datasets[0].data = [0, 0, 0];
            this.charts.sentiment.update('none');
        }
        
        if (this.charts.commentLength) {
            this.charts.commentLength.data.datasets[0].data = new Array(6).fill(0);
            this.charts.commentLength.update('none');
        }
    }
    
    destroyCharts() {
        Object.values(this.charts).forEach(chart => {
            if (chart) {
                chart.destroy();
            }
        });
        this.charts = {};
    }
}

// Extend Dashboard class to include chart management
if (typeof Dashboard !== 'undefined') {
    Dashboard.prototype.updateCharts = function(data) {
        if (!this.chartManager) {
            this.chartManager = new ChartManager();
        }
        
        console.log('Updating charts with data:', data);
        
        // Update sentiment chart
        if (data.sentiment_distribution) {
            this.chartManager.updateSentimentChart(data.sentiment_distribution);
        }
        
        // Update comment length chart using sentiment samples
        if (data.sentiment_samples && data.sentiment_samples.length > 0) {
            this.chartManager.updateCommentLengthChart(data.sentiment_samples);
        } else if (data.processed_comments && data.processed_comments.length > 0) {
            // Fallback to processed comments if sentiment samples not available
            this.chartManager.updateCommentLengthChart(data.processed_comments);
        }
        
        // If no data available, show empty state
        if (!data.sentiment_distribution && (!data.sentiment_samples || data.sentiment_samples.length === 0)) {
            this.chartManager.updateChartsWithNoData();
        }
    };
}