package main

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog/log"
)

var (
	rabbitClient  *RabbitMQClient
	rabbitEnabled bool
	rabbitQueue   string
)

type RabbitMQClient struct {
	url           string
	defaultQueue  string
	conn          *amqp091.Connection
	channel       *amqp091.Channel
	mu            sync.RWMutex
	reconnectCh   chan bool
	closeCh       chan *amqp091.Error
	channelCloseCh chan *amqp091.Error
	connected     bool
	ctx           context.Context
	cancel        context.CancelFunc
}

// NewRabbitMQClient creates a new RabbitMQ client with auto-reconnection
func NewRabbitMQClient(url, queue string) *RabbitMQClient {
	ctx, cancel := context.WithCancel(context.Background())
	client := &RabbitMQClient{
		url:          url,
		defaultQueue: queue,
		reconnectCh:  make(chan bool, 1),
		connected:    false,
		ctx:          ctx,
		cancel:       cancel,
	}
	
	go client.connectionManager()
	return client
}

// connectionManager handles automatic reconnection
func (r *RabbitMQClient) connectionManager() {
	for {
		select {
		case <-r.ctx.Done():
			log.Info().Msg("RabbitMQ connection manager shutting down")
			return
		default:
			if err := r.connect(); err != nil {
				log.Error().Err(err).Msg("Failed to connect to RabbitMQ, retrying in 5 seconds...")
				time.Sleep(5 * time.Second)
				continue
			}
			
			log.Info().Msg("RabbitMQ connected successfully, monitoring connection...")
			
			// Monitor connection health
			select {
			case err := <-r.closeCh:
				if err != nil {
					log.Error().Err(err).Msg("RabbitMQ connection lost, attempting reconnection...")
				}
				r.setConnected(false)
			case err := <-r.channelCloseCh:
				if err != nil {
					log.Error().Err(err).Msg("RabbitMQ channel lost, attempting reconnection...")
				}
				r.setConnected(false)
			case <-r.reconnectCh:
				log.Info().Msg("Manual reconnection requested")
				r.setConnected(false)
			case <-r.ctx.Done():
				log.Info().Msg("RabbitMQ connection manager shutting down")
				r.disconnect()
				return
			}
		}
	}
}

// connect establishes connection to RabbitMQ
func (r *RabbitMQClient) connect() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	// Close existing connection if any
	if r.conn != nil && !r.conn.IsClosed() {
		r.conn.Close()
	}
	
	var err error
	r.conn, err = amqp091.Dial(r.url)
	if err != nil {
		return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
	}
	
	r.channel, err = r.conn.Channel()
	if err != nil {
		r.conn.Close()
		return fmt.Errorf("failed to open channel: %w", err)
	}
	
	// Set up connection and channel monitoring
	r.closeCh = make(chan *amqp091.Error)
	r.channelCloseCh = make(chan *amqp091.Error)
	r.conn.NotifyClose(r.closeCh)
	r.channel.NotifyClose(r.channelCloseCh)
	
	r.connected = true
	log.Info().Str("queue", r.defaultQueue).Msg("RabbitMQ connection established")
	return nil
}

// disconnect closes the connection
func (r *RabbitMQClient) disconnect() {
	r.mu.Lock()
	defer r.mu.Unlock()
	
	r.connected = false
	if r.channel != nil {
		r.channel.Close()
	}
	if r.conn != nil && !r.conn.IsClosed() {
		r.conn.Close()
	}
}

// setConnected updates connection status thread-safely
func (r *RabbitMQClient) setConnected(status bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.connected = status
}

// isConnected checks if client is connected thread-safely
func (r *RabbitMQClient) isConnected() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.connected && r.channel != nil && r.conn != nil && !r.conn.IsClosed()
}

// Publish publishes a message to RabbitMQ with automatic retry and reconnection
func (r *RabbitMQClient) Publish(data []byte, queueOverride ...string) error {
	if !r.isConnected() {
		return fmt.Errorf("RabbitMQ not connected")
	}
	
	queueName := r.defaultQueue
	if len(queueOverride) > 0 && queueOverride[0] != "" {
		queueName = queueOverride[0]
	}
	
	return r.publishWithRetry(data, queueName, 3)
}

// publishWithRetry attempts to publish with retry logic
func (r *RabbitMQClient) publishWithRetry(data []byte, queueName string, maxRetries int) error {
	var lastErr error
	
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if !r.isConnected() {
			// Wait a bit for reconnection
			time.Sleep(time.Duration(attempt) * time.Second)
			continue
		}
		
		r.mu.RLock()
		channel := r.channel
		r.mu.RUnlock()
		
		if channel == nil {
			lastErr = fmt.Errorf("channel is nil")
			continue
		}
		
		// Declare queue (idempotent)
		_, err := channel.QueueDeclare(
			queueName,
			true,  // durable
			false, // auto-delete
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			lastErr = fmt.Errorf("failed to declare queue: %w", err)
			log.Error().Err(err).Str("queue", queueName).Int("attempt", attempt).Msg("Could not declare RabbitMQ queue")
			
			// Force reconnection on channel error
			select {
			case r.reconnectCh <- true:
			default:
			}
			
			if attempt < maxRetries {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			return lastErr
		}
		
		// Publish message
		err = channel.Publish(
			"",        // exchange (default)
			queueName, // routing key = queue
			false,     // mandatory
			false,     // immediate
			amqp091.Publishing{
				ContentType:  "application/json",
				Body:         data,
				DeliveryMode: amqp091.Persistent, // Make message persistent
				Timestamp:    time.Now(),
			},
		)
		
		if err != nil {
			lastErr = fmt.Errorf("failed to publish message: %w", err)
			log.Error().Err(err).Str("queue", queueName).Int("attempt", attempt).Msg("Could not publish to RabbitMQ")
			
			// Force reconnection on publish error
			select {
			case r.reconnectCh <- true:
			default:
			}
			
			if attempt < maxRetries {
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			return lastErr
		}
		
		// Success
		log.Debug().Str("queue", queueName).Msg("Published message to RabbitMQ")
		return nil
	}
	
	return lastErr
}

// Close gracefully shuts down the RabbitMQ client
func (r *RabbitMQClient) Close() {
	if r.cancel != nil {
		r.cancel()
	}
	r.disconnect()
}

// InitRabbitMQ initializes the global RabbitMQ client
func InitRabbitMQ() {
	rabbitURL := os.Getenv("RABBITMQ_URL")
	rabbitQueue = os.Getenv("RABBITMQ_QUEUE")
	if rabbitQueue == "" {
		rabbitQueue = "whatsapp_events" // default queue
	}
	
	if rabbitURL == "" {
		rabbitEnabled = false
		log.Info().Msg("RABBITMQ_URL is not set. RabbitMQ publishing disabled.")
		return
	}
	
	rabbitClient = NewRabbitMQClient(rabbitURL, rabbitQueue)
	rabbitEnabled = true
	
	log.Info().
		Str("url", rabbitURL).
		Str("queue", rabbitQueue).
		Msg("RabbitMQ client initialized with auto-reconnection")
}

// PublishToRabbit publishes data to RabbitMQ (maintains backward compatibility)
func PublishToRabbit(data []byte, queueOverride ...string) error {
	if !rabbitEnabled || rabbitClient == nil {
		return nil
	}
	
	return rabbitClient.Publish(data, queueOverride...)
}

// sendToGlobalRabbit sends data to RabbitMQ (maintains backward compatibility)
func sendToGlobalRabbit(jsonData []byte, queueName ...string) {
	if !rabbitEnabled || rabbitClient == nil {
		log.Debug().Msg("RabbitMQ publishing is disabled, not sending message")
		return
	}
	
	err := rabbitClient.Publish(jsonData, queueName...)
	if err != nil {
		log.Error().Err(err).Msg("Failed to publish to RabbitMQ after retries")
	}
}

// GetRabbitMQStatus returns the current connection status
func GetRabbitMQStatus() map[string]interface{} {
	status := map[string]interface{}{
		"enabled":   rabbitEnabled,
		"connected": false,
		"queue":     rabbitQueue,
	}
	
	if rabbitClient != nil {
		status["connected"] = rabbitClient.isConnected()
	}
	
	return status
}

// ForceRabbitMQReconnect forces a reconnection attempt
func ForceRabbitMQReconnect() {
	if rabbitClient != nil {
		select {
		case rabbitClient.reconnectCh <- true:
			log.Info().Msg("Forced RabbitMQ reconnection requested")
		default:
			log.Warn().Msg("Reconnection already in progress")
		}
	}
}
