package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/websocket"
)

////////////////////////////////////////////////////////////////////////////////
// 1) Event hub: broadcast all events to any connected WebSocket client
////////////////////////////////////////////////////////////////////////////////

type Event struct {
	Ts          float64 `json:"ts"`                   // seconds since start
	Type        string  `json:"type"`                 // e.g. "GoStart","ChanSend",...
	Goroutine   string  `json:"goroutine"`            // e.g. "worker-1"
	GoroutineID string  `json:"goroutineId"`          // unique ID for goroutine
	Name        string  `json:"name,omitempty"`       // channel/mutex/region name
	Stack       string  `json:"stack,omitempty"`      // optional stack dump
	Text        string  `json:"text,omitempty"`       // descriptive text for the event
	Params      string  `json:"params,omitempty"`     // parameters sent to goroutine
	ParamTypes  string  `json:"paramTypes,omitempty"` // parameter types
	Response    string  `json:"response,omitempty"`   // response from goroutine
	RespType    string  `json:"respType,omitempty"`   // response type
	Value       string  `json:"value,omitempty"`      // actual value being sent/received
	Status      string  `json:"status,omitempty"`     // running, paused, terminated
	Duration    float64 `json:"duration,omitempty"`   // execution duration
	MemUsage    string  `json:"memUsage,omitempty"`   // memory usage info
	IsUpdate    bool    `json:"isUpdate,omitempty"`   // true if this is an update to existing goroutine

	// APM-style operation tracing fields
	SpanID     string                 `json:"spanId,omitempty"`     // unique span identifier
	ParentSpan string                 `json:"parentSpan,omitempty"` // parent span ID for nested operations
	Operation  string                 `json:"operation,omitempty"`  // operation name (e.g., "db.query", "cache.get")
	Service    string                 `json:"service,omitempty"`    // service name
	Tags       map[string]interface{} `json:"tags,omitempty"`       // operation tags/labels
	Error      *string                `json:"error,omitempty"`      // error message if operation failed
	StartTime  float64                `json:"startTime,omitempty"`  // operation start timestamp
	EndTime    float64                `json:"endTime,omitempty"`    // operation end timestamp
	Metadata   map[string]interface{} `json:"metadata,omitempty"`   // additional metadata
}

// GoroutineInfo tracks detailed information about each goroutine
type GoroutineInfo struct {
	ID               string
	Name             string
	Status           string // running, paused, terminated
	StartTime        time.Time
	EndTime          *time.Time
	Params           interface{}
	ParamTypes       string
	Response         interface{}
	RespType         string
	PauseChannel     chan bool
	ResumeChannel    chan bool
	TerminateChannel chan bool
	IsLongRunning    bool
	LastHeartbeat    time.Time
	CallCount        int64
	Rendered         bool // Track if goroutine has been rendered once for long-running ones
}

// Span represents a traced operation with APM-style details
type Span struct {
	ID        string
	ParentID  string
	Operation string
	Service   string
	StartTime time.Time
	EndTime   *time.Time
	Duration  time.Duration
	Tags      map[string]interface{}
	Error     *string
	Metadata  map[string]interface{}
	Goroutine string
}

// SpanContext holds active spans for correlation
type SpanContext struct {
	spans       map[string]*Span
	activeSpans map[string][]string // goroutine -> stack of active span IDs
	mu          sync.RWMutex
}

func NewSpanContext() *SpanContext {
	return &SpanContext{
		spans:       make(map[string]*Span),
		activeSpans: make(map[string][]string),
	}
}

func (sc *SpanContext) StartSpan(goroutine, operation, service string, tags map[string]interface{}) *Span {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	spanID := fmt.Sprintf("%s-%d", operation, time.Now().UnixNano())

	var parentID string
	if stack, exists := sc.activeSpans[goroutine]; exists && len(stack) > 0 {
		parentID = stack[len(stack)-1]
	}

	span := &Span{
		ID:        spanID,
		ParentID:  parentID,
		Operation: operation,
		Service:   service,
		StartTime: time.Now(),
		Tags:      tags,
		Metadata:  make(map[string]interface{}),
		Goroutine: goroutine,
	}

	sc.spans[spanID] = span
	sc.activeSpans[goroutine] = append(sc.activeSpans[goroutine], spanID)

	return span
}

func (sc *SpanContext) FinishSpan(spanID string, err error) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	span, exists := sc.spans[spanID]
	if !exists {
		return
	}

	endTime := time.Now()
	span.EndTime = &endTime
	span.Duration = endTime.Sub(span.StartTime)

	if err != nil {
		errStr := err.Error()
		span.Error = &errStr
	}

	// Remove from active spans stack
	if stack, exists := sc.activeSpans[span.Goroutine]; exists {
		for i := len(stack) - 1; i >= 0; i-- {
			if stack[i] == spanID {
				sc.activeSpans[span.Goroutine] = append(stack[:i], stack[i+1:]...)
				break
			}
		}
	}
}

func (sc *SpanContext) GetSpan(spanID string) *Span {
	sc.mu.RLock()
	defer sc.mu.RUnlock()
	return sc.spans[spanID]
}

var (
	startTime       = time.Now()
	hub             = NewHub()
	goroutines      = make(map[string]*GoroutineInfo)
	goroutinesMutex sync.RWMutex
	spanContext     = NewSpanContext()
)

// Hub holds client channels and broadcasts to all of them.
type Hub struct {
	mu      sync.Mutex
	clients map[chan Event]struct{}
}

func NewHub() *Hub {
	return &Hub{clients: make(map[chan Event]struct{})}
}

func (h *Hub) Register() chan Event {
	ch := make(chan Event, 256)
	h.mu.Lock()
	h.clients[ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *Hub) Unregister(ch chan Event) {
	h.mu.Lock()
	delete(h.clients, ch)
	h.mu.Unlock()
	close(ch)
}

func (h *Hub) Broadcast(ev Event) {
	h.mu.Lock()
	for ch := range h.clients {
		select {
		case ch <- ev:
		default:
		}
	}
	h.mu.Unlock()
}

func emit(ev Event) {
	// Add memory usage info
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	ev.MemUsage = fmt.Sprintf("Alloc: %d KB, Sys: %d KB", m.Alloc/1024, m.Sys/1024)

	// Ensure goroutine ID is set
	if ev.GoroutineID == "" {
		ev.GoroutineID = ev.Goroutine
	}

	switch ev.Type {
	case "GoStart":
		ev.Text = fmt.Sprintf("Goroutine %s started", ev.Goroutine)
		// Register goroutine
		goroutinesMutex.Lock()
		if info, exists := goroutines[ev.Goroutine]; exists {
			info.Status = "running"
			info.LastHeartbeat = time.Now()
			info.Rendered = false // Reset for new start
		}
		goroutinesMutex.Unlock()
	case "GoEnd":
		ev.Text = fmt.Sprintf("Goroutine %s ended", ev.Goroutine)
		// Update goroutine status
		goroutinesMutex.Lock()
		if info, exists := goroutines[ev.Goroutine]; exists {
			info.Status = "terminated"
			endTime := time.Now()
			info.EndTime = &endTime
			ev.Duration = endTime.Sub(info.StartTime).Seconds()
		}
		goroutinesMutex.Unlock()
	case "ChanSend":
		ev.Text = fmt.Sprintf("Goroutine %s sent to channel %s", ev.Goroutine, ev.Name)
	case "ChanRecv":
		ev.Text = fmt.Sprintf("Goroutine %s received from channel %s", ev.Goroutine, ev.Name)
	case "MutexLock":
		ev.Text = fmt.Sprintf("Goroutine %s locked mutex %s", ev.Goroutine, ev.Name)
	case "MutexUnlock":
		ev.Text = fmt.Sprintf("Goroutine %s unlocked mutex %s", ev.Goroutine, ev.Name)
	case "RegionStart":
		ev.Text = fmt.Sprintf("Goroutine %s entered region %s", ev.Goroutine, ev.Name)
	case "RegionEnd":
		ev.Text = fmt.Sprintf("Goroutine %s exited region %s", ev.Goroutine, ev.Name)
	case "StatusChange":
		ev.IsUpdate = true // Mark status changes as updates
	case "HTTPRequest", "DBQuery", "TaskProcessed":
		ev.IsUpdate = true // Mark activity events as updates
	case "Heartbeat":
		// Update last heartbeat for long-running goroutines
		goroutinesMutex.Lock()
		if info, exists := goroutines[ev.Goroutine]; exists {
			info.LastHeartbeat = time.Now()
			atomic.AddInt64(&info.CallCount, 1)
			ev.Text = fmt.Sprintf("Goroutine %s heartbeat (calls: %d)", ev.Goroutine, info.CallCount)

			// For long-running goroutines, don't broadcast heartbeat events - just update internal state
			if info.IsLongRunning {
				goroutinesMutex.Unlock()
				return // Don't broadcast heartbeat events for long-running goroutines
			}
		}
		goroutinesMutex.Unlock()
	}

	// Check if this is a long-running goroutine and has been rendered
	goroutinesMutex.RLock()
	if info, exists := goroutines[ev.Goroutine]; exists && info.IsLongRunning && info.Rendered {
		// For long-running goroutines, only emit certain types of events after initial render
		allowedTypes := []string{"GoEnd", "Error", "StatusChange", "ServerStarted", "HTTPRequest", "DBQuery", "TaskProcessed"}
		allowed := false
		for _, t := range allowedTypes {
			if ev.Type == t {
				allowed = true
				break
			}
		}
		if !allowed {
			goroutinesMutex.RUnlock()
			return
		}
		// Mark as update for long-running goroutines
		ev.IsUpdate = true
	}
	goroutinesMutex.RUnlock()

	hub.Broadcast(ev)
}

////////////////////////////////////////////////////////////////////////////////
// APM-Style Operation Tracing Functions
////////////////////////////////////////////////////////////////////////////////

// StartOperation begins a traced operation with APM-style details
func StartOperation(ctx context.Context, operation, service string, tags map[string]interface{}) (context.Context, *Span) {
	goroutineName := ctx.Value("g").(string)
	span := spanContext.StartSpan(goroutineName, operation, service, tags)

	// Generate detailed text based on operation type and tags
	var detailedText string
	switch operation {
	case "db.query":
		if stmt, ok := tags["db.statement"].(string); ok {
			detailedText = fmt.Sprintf("Started DB Query: %s", stmt)
		} else {
			detailedText = fmt.Sprintf("Started %s operation in %s", operation, service)
		}
	case "cache.get":
		if key, ok := tags["cache.key"].(string); ok {
			detailedText = fmt.Sprintf("Started Cache GET: %s", key)
		} else {
			detailedText = fmt.Sprintf("Started %s operation in %s", operation, service)
		}
	case "cache.set":
		if key, ok := tags["cache.key"].(string); ok {
			detailedText = fmt.Sprintf("Started Cache SET: %s", key)
		} else {
			detailedText = fmt.Sprintf("Started %s operation in %s", operation, service)
		}
	case "http.request":
		if method, ok := tags["http.method"].(string); ok {
			if url, ok := tags["http.url"].(string); ok {
				detailedText = fmt.Sprintf("Started HTTP %s: %s", method, url)
			} else {
				detailedText = fmt.Sprintf("Started HTTP %s request", method)
			}
		} else {
			detailedText = fmt.Sprintf("Started %s operation in %s", operation, service)
		}
	case "mq.publish":
		if topic, ok := tags["mq.topic"].(string); ok {
			detailedText = fmt.Sprintf("Started Message Publish to: %s", topic)
		} else {
			detailedText = fmt.Sprintf("Started %s operation in %s", operation, service)
		}
	case "mq.consume":
		if topic, ok := tags["mq.topic"].(string); ok {
			detailedText = fmt.Sprintf("Started Message Consume from: %s", topic)
		} else {
			detailedText = fmt.Sprintf("Started %s operation in %s", operation, service)
		}
	default:
		detailedText = fmt.Sprintf("Started %s operation in %s", operation, service)
	}

	// Emit span start event
	emit(Event{
		Ts:         now(),
		Type:       "SpanStart",
		Goroutine:  goroutineName,
		SpanID:     span.ID,
		ParentSpan: span.ParentID,
		Operation:  operation,
		Service:    service,
		Tags:       tags,
		StartTime:  float64(span.StartTime.UnixNano()) / 1e9,
		Text:       detailedText,
		IsUpdate:   true,
	})

	// Store span in context
	newCtx := context.WithValue(ctx, "span", span)
	return newCtx, span
}

// FinishOperation completes a traced operation
func FinishOperation(ctx context.Context, span *Span, err error, result interface{}) {
	spanContext.FinishSpan(span.ID, err)

	goroutineName := ctx.Value("g").(string)

	var errorStr *string
	if err != nil {
		errMsg := err.Error()
		errorStr = &errMsg
	}

	// Generate detailed completion text
	var completedText string
	switch span.Operation {
	case "db.query":
		if stmt, ok := span.Tags["db.statement"].(string); ok {
			if err != nil {
				completedText = fmt.Sprintf("DB Query Failed: %s (%.2fms) - %s", stmt, span.Duration.Seconds()*1000, err.Error())
			} else {
				completedText = fmt.Sprintf("DB Query Completed: %s (%.2fms)", stmt, span.Duration.Seconds()*1000)
			}
		} else {
			completedText = fmt.Sprintf("Completed %s operation (%.2fms)", span.Operation, span.Duration.Seconds()*1000)
		}
	case "cache.get":
		if key, ok := span.Tags["cache.key"].(string); ok {
			hit := "miss"
			if h, ok := span.Tags["cache.hit"].(bool); ok && h {
				hit = "hit"
			}
			completedText = fmt.Sprintf("Cache GET %s: %s (%.2fms)", hit, key, span.Duration.Seconds()*1000)
		} else {
			completedText = fmt.Sprintf("Completed %s operation (%.2fms)", span.Operation, span.Duration.Seconds()*1000)
		}
	case "cache.set":
		if key, ok := span.Tags["cache.key"].(string); ok {
			if err != nil {
				completedText = fmt.Sprintf("Cache SET Failed: %s (%.2fms) - %s", key, span.Duration.Seconds()*1000, err.Error())
			} else {
				completedText = fmt.Sprintf("Cache SET Completed: %s (%.2fms)", key, span.Duration.Seconds()*1000)
			}
		} else {
			completedText = fmt.Sprintf("Completed %s operation (%.2fms)", span.Operation, span.Duration.Seconds()*1000)
		}
	case "http.request":
		if method, ok := span.Tags["http.method"].(string); ok {
			if url, ok := span.Tags["http.url"].(string); ok {
				status := ""
				if statusCode, ok := span.Tags["http.status_code"].(int); ok {
					status = fmt.Sprintf(" [%d]", statusCode)
				}
				completedText = fmt.Sprintf("HTTP %s%s: %s (%.2fms)", method, status, url, span.Duration.Seconds()*1000)
			} else {
				completedText = fmt.Sprintf("HTTP %s completed (%.2fms)", method, span.Duration.Seconds()*1000)
			}
		} else {
			completedText = fmt.Sprintf("Completed %s operation (%.2fms)", span.Operation, span.Duration.Seconds()*1000)
		}
	case "mq.publish":
		if topic, ok := span.Tags["mq.topic"].(string); ok {
			if err != nil {
				completedText = fmt.Sprintf("Message Publish Failed to %s (%.2fms) - %s", topic, span.Duration.Seconds()*1000, err.Error())
			} else {
				completedText = fmt.Sprintf("Message Published to %s (%.2fms)", topic, span.Duration.Seconds()*1000)
			}
		} else {
			completedText = fmt.Sprintf("Completed %s operation (%.2fms)", span.Operation, span.Duration.Seconds()*1000)
		}
	case "mq.consume":
		if topic, ok := span.Tags["mq.topic"].(string); ok {
			completedText = fmt.Sprintf("Message Consumed from %s (%.2fms)", topic, span.Duration.Seconds()*1000)
		} else {
			completedText = fmt.Sprintf("Completed %s operation (%.2fms)", span.Operation, span.Duration.Seconds()*1000)
		}
	default:
		completedText = fmt.Sprintf("Completed %s operation (%.2fms)", span.Operation, span.Duration.Seconds()*1000)
	}

	// Emit span finish event
	emit(Event{
		Ts:        now(),
		Type:      "SpanEnd",
		Goroutine: goroutineName,
		SpanID:    span.ID,
		Operation: span.Operation,
		Service:   span.Service,
		Duration:  span.Duration.Seconds(),
		EndTime:   float64(span.EndTime.UnixNano()) / 1e9,
		Error:     errorStr,
		Response:  fmt.Sprintf("%v", result),
		Tags:      span.Tags,
		Metadata:  span.Metadata,
		Text:      completedText,
		IsUpdate:  true,
	})
}

// Database operation helpers
func DBQuery(ctx context.Context, query string, args ...interface{}) (interface{}, error) {
	tags := map[string]interface{}{
		"db.type":      "postgresql",
		"db.statement": query,
		"db.args":      fmt.Sprintf("%v", args),
	}

	newCtx, span := StartOperation(ctx, "db.query", "database", tags)

	// Simulate database query
	time.Sleep(time.Duration(20+len(query)/10) * time.Millisecond)

	var result interface{}
	var err error

	// Simulate different query types
	if strings.Contains(strings.ToLower(query), "select") {
		result = map[string]interface{}{
			"rows":     []map[string]interface{}{{"id": 1, "name": "test"}},
			"rowCount": 1,
		}
	} else if strings.Contains(strings.ToLower(query), "insert") {
		result = map[string]interface{}{"insertedId": 123, "affectedRows": 1}
	} else if strings.Contains(strings.ToLower(query), "update") {
		result = map[string]interface{}{"affectedRows": 2}
	} else {
		result = map[string]interface{}{"success": true}
	}

	// Simulate occasional errors
	if strings.Contains(query, "error") {
		err = fmt.Errorf("database connection timeout")
	}

	span.Metadata["query_length"] = len(query)
	span.Metadata["args_count"] = len(args)

	FinishOperation(newCtx, span, err, result)
	return result, err
}

// Cache operation helpers
func CacheGet(ctx context.Context, key string) (interface{}, error) {
	tags := map[string]interface{}{
		"cache.operation": "get",
		"cache.key":       key,
		"cache.type":      "redis",
	}

	newCtx, span := StartOperation(ctx, "cache.get", "cache", tags)

	// Simulate cache lookup
	time.Sleep(time.Duration(2+len(key)/5) * time.Millisecond)

	var result interface{}
	var err error

	// Simulate cache hit/miss
	if len(key)%3 == 0 {
		result = fmt.Sprintf("cached_value_%s", key)
		span.Tags["cache.hit"] = true
	} else {
		span.Tags["cache.hit"] = false
		err = fmt.Errorf("cache miss")
	}

	span.Metadata["key_length"] = len(key)

	FinishOperation(newCtx, span, err, result)
	return result, err
}

func CacheSet(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	tags := map[string]interface{}{
		"cache.operation": "set",
		"cache.key":       key,
		"cache.ttl":       ttl.Seconds(),
		"cache.type":      "redis",
	}

	newCtx, span := StartOperation(ctx, "cache.set", "cache", tags)

	// Simulate cache set
	time.Sleep(time.Duration(3+len(key)/3) * time.Millisecond)

	var err error
	result := "OK"

	// Simulate occasional errors
	if len(key) > 100 {
		err = fmt.Errorf("key too long")
	}

	span.Metadata["key_length"] = len(key)
	span.Metadata["value_size"] = len(fmt.Sprintf("%v", value))

	FinishOperation(newCtx, span, err, result)
	return err
}

// HTTP client operation
func HTTPRequest(ctx context.Context, method, url string, body interface{}) (interface{}, error) {
	tags := map[string]interface{}{
		"http.method": method,
		"http.url":    url,
		"http.client": "go-http",
	}

	newCtx, span := StartOperation(ctx, "http.request", "http-client", tags)

	// Simulate HTTP request
	time.Sleep(time.Duration(50+len(url)/5) * time.Millisecond)

	var result interface{}
	var err error

	// Simulate different response codes
	statusCode := 200
	if strings.Contains(url, "error") {
		statusCode = 500
		err = fmt.Errorf("internal server error")
	} else if strings.Contains(url, "notfound") {
		statusCode = 404
		err = fmt.Errorf("not found")
	}

	result = map[string]interface{}{
		"statusCode": statusCode,
		"body":       fmt.Sprintf("Response from %s", url),
		"headers":    map[string]string{"Content-Type": "application/json"},
	}

	span.Tags["http.status_code"] = statusCode
	span.Metadata["url_length"] = len(url)
	if body != nil {
		span.Metadata["body_size"] = len(fmt.Sprintf("%v", body))
	}

	FinishOperation(newCtx, span, err, result)
	return result, err
}

// Message queue operations
func PublishMessage(ctx context.Context, topic string, message interface{}) error {
	tags := map[string]interface{}{
		"mq.operation": "publish",
		"mq.topic":     topic,
		"mq.type":      "kafka",
	}

	newCtx, span := StartOperation(ctx, "mq.publish", "message-queue", tags)

	// Simulate message publishing
	time.Sleep(time.Duration(10+len(topic)/2) * time.Millisecond)

	var err error
	result := "published"

	// Simulate occasional errors
	if strings.Contains(topic, "dead") {
		err = fmt.Errorf("topic not found")
	}

	span.Metadata["topic_length"] = len(topic)
	span.Metadata["message_size"] = len(fmt.Sprintf("%v", message))

	FinishOperation(newCtx, span, err, result)
	return err
}

func ConsumeMessage(ctx context.Context, topic string) (interface{}, error) {
	tags := map[string]interface{}{
		"mq.operation": "consume",
		"mq.topic":     topic,
		"mq.type":      "kafka",
	}

	newCtx, span := StartOperation(ctx, "mq.consume", "message-queue", tags)

	// Simulate message consumption
	time.Sleep(time.Duration(15+len(topic)/3) * time.Millisecond)

	var result interface{}
	var err error

	result = map[string]interface{}{
		"message":   fmt.Sprintf("Message from %s", topic),
		"offset":    12345,
		"partition": 0,
	}

	span.Metadata["topic_length"] = len(topic)

	FinishOperation(newCtx, span, err, result)
	return result, err
}

// Register a new goroutine
func registerGoroutine(name string, params interface{}, paramTypes string, isLongRunning bool) *GoroutineInfo {
	goroutinesMutex.Lock()
	defer goroutinesMutex.Unlock()

	info := &GoroutineInfo{
		ID:               name,
		Name:             name,
		Status:           "starting",
		StartTime:        time.Now(),
		Params:           params,
		ParamTypes:       paramTypes,
		PauseChannel:     make(chan bool, 1),
		ResumeChannel:    make(chan bool, 1),
		TerminateChannel: make(chan bool, 1),
		IsLongRunning:    isLongRunning,
		LastHeartbeat:    time.Now(),
		CallCount:        0,
		Rendered:         false,
	}

	goroutines[name] = info
	return info
}

// Get goroutine information
func getGoroutineInfo(name string) (*GoroutineInfo, bool) {
	goroutinesMutex.RLock()
	defer goroutinesMutex.RUnlock()

	info, exists := goroutines[name]
	return info, exists
}

// List all goroutines
func listGoroutines() []*GoroutineInfo {
	goroutinesMutex.RLock()
	defer goroutinesMutex.RUnlock()

	var list []*GoroutineInfo
	for _, info := range goroutines {
		// Only include goroutines that are actually registered and not just internal ones
		if info.Name != "" && info.Name != "main" {
			list = append(list, info)
		}
	}
	return list
}

// Clean up terminated goroutines periodically
func cleanupGoroutines() {
	goroutinesMutex.Lock()
	defer goroutinesMutex.Unlock()

	now := time.Now()
	for name, info := range goroutines {
		// Remove terminated goroutines after 30 seconds
		if info.Status == "terminated" && info.EndTime != nil && now.Sub(*info.EndTime) > 30*time.Second {
			delete(goroutines, name)
		}
	}
}

// Clear all goroutines (for starting new examples)
func clearAllGoroutines() {
	goroutinesMutex.Lock()
	defer goroutinesMutex.Unlock()

	// Terminate all running goroutines
	for _, info := range goroutines {
		if info.Status == "running" || info.Status == "paused" {
			select {
			case info.TerminateChannel <- true:
			default:
			}
		}
	}

	// Clear the map
	goroutines = make(map[string]*GoroutineInfo)
} ////////////////////////////////////////////////////////////////////////////////
// 2) Instrumentation helpers: GoStart/GoEnd, channels, mutexes, regions, stacks
////////////////////////////////////////////////////////////////////////////////

func now() float64 {
	return time.Since(startTime).Seconds()
}

// Launch a named goroutine with control capabilities
func GoWithParamsAndControl(ctx context.Context, name string, params interface{}, paramTypes string, isLongRunning bool, fn func(ctx context.Context, params interface{}, control *GoroutineInfo) interface{}) {
	info := registerGoroutine(name, params, paramTypes, isLongRunning)
	paramsStr := fmt.Sprintf("%v", params)

	emit(Event{Ts: now(), Type: "GoStart", Goroutine: name, GoroutineID: name, Params: paramsStr, ParamTypes: paramTypes, Status: "running"})

	go func() {
		defer func() {
			if r := recover(); r != nil {
				emit(Event{Ts: now(), Type: "Panic", Goroutine: name, Text: fmt.Sprintf("Goroutine panicked: %v", r)})
			}
		}()

		// Mark as rendered for visualization
		goroutinesMutex.Lock()
		info.Rendered = true
		goroutinesMutex.Unlock()

		// capture stack at start
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, false)
		emit(Event{Ts: now(), Type: "Stack", Goroutine: name, Stack: string(buf[:n])})

		// Set up heartbeat for long-running goroutines
		var heartbeatTicker *time.Ticker
		if isLongRunning {
			heartbeatTicker = time.NewTicker(5 * time.Second) // Reduced frequency
			go func() {
				defer heartbeatTicker.Stop()
				for {
					select {
					case <-heartbeatTicker.C:
						goroutinesMutex.RLock()
						if info.Status == "terminated" {
							goroutinesMutex.RUnlock()
							return
						}
						goroutinesMutex.RUnlock()

						// Only update internal state, don't emit events
						goroutinesMutex.Lock()
						info.LastHeartbeat = time.Now()
						atomic.AddInt64(&info.CallCount, 1)
						goroutinesMutex.Unlock()
					case <-info.TerminateChannel:
						return
					}
				}
			}()
		}

		// Execute the function with control
		ctx = context.WithValue(ctx, "g", name)
		result := fn(ctx, params, info)

		if heartbeatTicker != nil {
			heartbeatTicker.Stop()
			// Send termination signal
			select {
			case info.TerminateChannel <- true:
			default:
			}
		}

		resultStr := fmt.Sprintf("%v", result)
		resultType := fmt.Sprintf("%T", result)

		// Update goroutine info
		goroutinesMutex.Lock()
		info.Response = result
		info.RespType = resultType
		goroutinesMutex.Unlock()

		emit(Event{Ts: now(), Type: "GoEnd", Goroutine: name, GoroutineID: name, Response: resultStr, RespType: resultType, Status: "terminated"})
	}()
}

// Launch a named goroutine, emitting start/end events with parameters.
func GoWithParams(ctx context.Context, name string, params interface{}, paramTypes string, fn func(ctx context.Context, params interface{}) interface{}) {
	GoWithParamsAndControl(ctx, name, params, paramTypes, false, func(ctx context.Context, params interface{}, control *GoroutineInfo) interface{} {
		return fn(ctx, params)
	})
}

// Launch a named goroutine, emitting start/end events.
func Go(ctx context.Context, name string, fn func(ctx context.Context)) {
	GoWithParams(ctx, name, nil, "", func(ctx context.Context, params interface{}) interface{} {
		fn(ctx)
		return nil
	})
}

// Helper function to check for pause/resume/terminate signals
func CheckControl(info *GoroutineInfo) bool {
	select {
	case <-info.PauseChannel:
		emit(Event{Ts: now(), Type: "StatusChange", Goroutine: info.Name, Status: "paused", Text: "Goroutine paused"})
		goroutinesMutex.Lock()
		info.Status = "paused"
		goroutinesMutex.Unlock()

		// Wait for resume or terminate - blocking call
		for {
			select {
			case <-info.ResumeChannel:
				emit(Event{Ts: now(), Type: "StatusChange", Goroutine: info.Name, Status: "running", Text: "Goroutine resumed"})
				goroutinesMutex.Lock()
				info.Status = "running"
				goroutinesMutex.Unlock()
				return false
			case <-info.TerminateChannel:
				emit(Event{Ts: now(), Type: "StatusChange", Goroutine: info.Name, Status: "terminated", Text: "Goroutine terminated"})
				goroutinesMutex.Lock()
				info.Status = "terminated"
				goroutinesMutex.Unlock()
				return true
			case <-time.After(100 * time.Millisecond):
				// Continue waiting, but allow periodic checks
				continue
			}
		}
	case <-info.TerminateChannel:
		emit(Event{Ts: now(), Type: "StatusChange", Goroutine: info.Name, Status: "terminated", Text: "Goroutine terminated"})
		goroutinesMutex.Lock()
		info.Status = "terminated"
		goroutinesMutex.Unlock()
		return true
	default:
		return false
	}
}

type IChan[T any] struct {
	C    chan T
	name string
}

func NewChan[T any](buf int, name string) *IChan[T] {
	return &IChan[T]{C: make(chan T, buf), name: name}
}

func (c *IChan[T]) Send(ctx context.Context, v T) {
	valueStr := fmt.Sprintf("%v", v)
	valueType := fmt.Sprintf("%T", v)
	emit(Event{Ts: now(), Type: "ChanSend", Goroutine: ctx.Value("g").(string), Name: c.name, Value: valueStr, ParamTypes: valueType})
	c.C <- v
	emit(Event{Ts: now(), Type: "ChanSent", Goroutine: ctx.Value("g").(string), Name: c.name, Value: valueStr, ParamTypes: valueType})
}

func (c *IChan[T]) Recv(ctx context.Context) T {
	emit(Event{Ts: now(), Type: "ChanRecv", Goroutine: ctx.Value("g").(string), Name: c.name})
	v := <-c.C
	valueStr := fmt.Sprintf("%v", v)
	valueType := fmt.Sprintf("%T", v)
	emit(Event{Ts: now(), Type: "ChanReceived", Goroutine: ctx.Value("g").(string), Name: c.name, Value: valueStr, RespType: valueType})
	return v
}

type IMutex struct {
	mu   sync.Mutex
	name string
}

func NewMutex(name string) *IMutex { return &IMutex{name: name} }

func (m *IMutex) Lock(ctx context.Context) {
	emit(Event{Ts: now(), Type: "MutexLock", Goroutine: ctx.Value("g").(string), Name: m.name})
	m.mu.Lock()
	emit(Event{Ts: now(), Type: "MutexLocked", Goroutine: ctx.Value("g").(string), Name: m.name})
}

func (m *IMutex) Unlock(ctx context.Context) {
	emit(Event{Ts: now(), Type: "MutexUnlock", Goroutine: ctx.Value("g").(string), Name: m.name})
	m.mu.Unlock()
	emit(Event{Ts: now(), Type: "MutexUnlocked", Goroutine: ctx.Value("g").(string), Name: m.name})
}

// User region helper
func WithRegion(ctx context.Context, region string, fn func()) {
	emit(Event{Ts: now(), Type: "RegionStart", Goroutine: ctx.Value("g").(string), Name: region})
	fn()
	emit(Event{Ts: now(), Type: "RegionEnd", Goroutine: ctx.Value("g").(string), Name: region})
}

// Periodic full-stack snapshots
func stackSampler(ctx context.Context, interval time.Duration) {
	g := ctx.Value("g").(string)
	buf := make([]byte, 1<<20)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		n := runtime.Stack(buf, true)
		emit(Event{Ts: now(), Type: "Stack", Goroutine: g, Stack: string(buf[:n])})
	}
}

////////////////////////////////////////////////////////////////////////////////
// 3) WebSocket handler: register client, stream JSON events until client closes
////////////////////////////////////////////////////////////////////////////////

func wsHandler(ws *websocket.Conn) {
	clientCh := hub.Register()
	defer hub.Unregister(clientCh)
	for ev := range clientCh {
		if err := websocket.JSON.Send(ws, ev); err != nil {
			return
		}
	}
}

////////////////////////////////////////////////////////////////////////////////
// 4) Snippet execution functions
////////////////////////////////////////////////////////////////////////////////

func runBufferedChannelSnippet(w http.ResponseWriter, r *http.Request) {
	clearAllGoroutines() // Clear previous goroutines
	startTime = time.Now()
	ctx := context.WithValue(context.Background(), "g", "main")
	emit(Event{Ts: now(), Type: "TraceStart", Goroutine: "main", Text: "Buffered Channel Example"})

	ch := NewChan[int](100, "buffered-channel")

	// Producer goroutine
	GoWithParams(ctx, "producer", map[string]interface{}{"count": 1000, "channelSize": 100}, "map[string]interface{}", func(ctx context.Context, params interface{}) interface{} {
		ctx = context.WithValue(ctx, "g", "producer")
		p := params.(map[string]interface{})
		count := p["count"].(int)
		sent := 0

		for i := 0; i < count; i++ {
			ch.Send(ctx, i)
			sent++
			if i%100 == 0 {
				time.Sleep(10 * time.Millisecond) // simulate some work
			}
		}
		return map[string]interface{}{"sent": sent}
	})

	// Consumer goroutine
	GoWithParams(ctx, "consumer", map[string]interface{}{"expectedCount": 1000}, "map[string]interface{}", func(ctx context.Context, params interface{}) interface{} {
		ctx = context.WithValue(ctx, "g", "consumer")
		p := params.(map[string]interface{})
		expectedCount := p["expectedCount"].(int)
		received := 0

		for received < expectedCount {
			value := ch.Recv(ctx)
			received++
			if received%100 == 0 {
				emit(Event{Ts: now(), Type: "Progress", Goroutine: "consumer", Response: fmt.Sprintf("received %d/%d", received, expectedCount), Value: fmt.Sprintf("%d", value)})
				time.Sleep(5 * time.Millisecond) // simulate processing
			}
		}
		return map[string]interface{}{"received": received}
	})

	time.AfterFunc(15*time.Second, func() {
		emit(Event{Ts: now(), Type: "TraceEnd", Goroutine: "main"})
	})

	fmt.Fprintln(w, "Buffered channel example started")
}

func runMutexSnippet(w http.ResponseWriter, r *http.Request) {
	clearAllGoroutines() // Clear previous goroutines
	startTime = time.Now()
	ctx := context.WithValue(context.Background(), "g", "main")
	emit(Event{Ts: now(), Type: "TraceStart", Goroutine: "main", Text: "Mutex Lock Example"})

	mu := NewMutex("shared-mutex")
	sharedCounter := 0

	var wg sync.WaitGroup
	numGoroutines := 5
	incrementsPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		name := fmt.Sprintf("incrementer-%d", i)
		GoWithParams(ctx, name, map[string]interface{}{"id": i, "increments": incrementsPerGoroutine}, "map[string]interface{}", func(ctx context.Context, params interface{}) interface{} {
			defer wg.Done()
			ctx = context.WithValue(ctx, "g", name)
			p := params.(map[string]interface{})
			id := p["id"].(int)
			increments := p["increments"].(int)
			localIncrements := 0

			for j := 0; j < increments; j++ {
				WithRegion(ctx, "critical-section", func() {
					mu.Lock(ctx)
					oldValue := sharedCounter
					time.Sleep(1 * time.Millisecond) // simulate critical work
					sharedCounter++
					localIncrements++
					mu.Unlock(ctx)

					if j%20 == 0 {
						emit(Event{Ts: now(), Type: "CounterUpdate", Goroutine: name,
							Params:   fmt.Sprintf("oldValue: %d", oldValue),
							Response: fmt.Sprintf("newValue: %d", sharedCounter),
							Value:    fmt.Sprintf("increment #%d", j+1)})
					}
				})
			}
			return map[string]interface{}{"localIncrements": localIncrements, "id": id}
		})
	}

	go func() {
		wg.Wait()
		emit(Event{Ts: now(), Type: "TraceEnd", Goroutine: "main",
			Response: fmt.Sprintf("Final counter value: %d", sharedCounter),
			Text:     fmt.Sprintf("Expected: %d, Actual: %d", numGoroutines*incrementsPerGoroutine, sharedCounter)})
	}()

	fmt.Fprintln(w, "Mutex example started")
}

func runWaitGroupSnippet(w http.ResponseWriter, r *http.Request) {
	clearAllGoroutines() // Clear previous goroutines
	startTime = time.Now()
	ctx := context.WithValue(context.Background(), "g", "main")
	emit(Event{Ts: now(), Type: "TraceStart", Goroutine: "main", Text: "WaitGroup Example"})

	var wg sync.WaitGroup
	numWorkers := 3
	tasksPerWorker := 5

	wg.Add(numWorkers)

	for i := 1; i <= numWorkers; i++ {
		name := fmt.Sprintf("worker-%d", i)
		GoWithParams(ctx, name, map[string]interface{}{"workerId": i, "tasks": tasksPerWorker, "delay": i * 100}, "map[string]interface{}", func(ctx context.Context, params interface{}) interface{} {
			defer wg.Done()
			ctx = context.WithValue(ctx, "g", name)
			p := params.(map[string]interface{})
			workerId := p["workerId"].(int)
			tasks := p["tasks"].(int)
			delay := p["delay"].(int)

			completedTasks := 0
			for j := 1; j <= tasks; j++ {
				WithRegion(ctx, fmt.Sprintf("task-%d", j), func() {
					time.Sleep(time.Duration(delay) * time.Millisecond)
					completedTasks++
					emit(Event{Ts: now(), Type: "TaskCompleted", Goroutine: name,
						Params:   fmt.Sprintf("task #%d", j),
						Response: fmt.Sprintf("completed %d/%d tasks", completedTasks, tasks),
						Value:    fmt.Sprintf("worker-%d result", workerId)})
				})
			}

			emit(Event{Ts: now(), Type: "WorkerCompleted", Goroutine: name,
				Response: fmt.Sprintf("All %d tasks completed", completedTasks)})
			return map[string]interface{}{"completedTasks": completedTasks, "workerId": workerId}
		})
	}

	// Main goroutine waits for all workers
	GoWithParams(ctx, "waiter", map[string]interface{}{"expectedWorkers": numWorkers}, "map[string]interface{}", func(ctx context.Context, params interface{}) interface{} {
		ctx = context.WithValue(ctx, "g", "waiter")
		emit(Event{Ts: now(), Type: "WaitStart", Goroutine: "waiter", Text: "Waiting for all workers to complete"})
		wg.Wait()
		emit(Event{Ts: now(), Type: "WaitEnd", Goroutine: "waiter", Text: "All workers completed"})
		emit(Event{Ts: now(), Type: "TraceEnd", Goroutine: "main"})
		return map[string]interface{}{"status": "all_workers_completed"}
	})

	fmt.Fprintln(w, "WaitGroup example started")
}

func runLongRunningServerSnippet(w http.ResponseWriter, r *http.Request) {
	clearAllGoroutines() // Clear previous goroutines
	startTime = time.Now()
	ctx := context.WithValue(context.Background(), "g", "main")
	emit(Event{Ts: now(), Type: "TraceStart", Goroutine: "main", Text: "Long-Running Server Example"})

	// HTTP Server goroutine
	GoWithParamsAndControl(ctx, "http-server", map[string]interface{}{"port": 9999}, "map[string]interface{}", true, func(ctx context.Context, params interface{}, control *GoroutineInfo) interface{} {
		ctx = context.WithValue(ctx, "g", "http-server")
		p := params.(map[string]interface{})
		port := p["port"].(int)

		requestCount := int64(0)

		mux := http.NewServeMux()
		mux.HandleFunc("/api/status", func(w http.ResponseWriter, r *http.Request) {
			if CheckControl(control) {
				return
			}
			atomic.AddInt64(&requestCount, 1)

			// Use APM-style HTTP tracing for incoming requests
			requestCtx := context.WithValue(ctx, "g", "http-server")

			// Simulate some database calls for the status endpoint
			go func(reqNum int64) {
				// Check system health
				_, _ = DBQuery(requestCtx, "SELECT COUNT(*) as active_connections FROM pg_stat_activity", nil)

				// Get recent error count
				_, _ = CacheGet(requestCtx, "error_count_last_hour")

				// Log the request
				_, _ = DBQuery(requestCtx, "INSERT INTO request_logs (endpoint, timestamp, request_id) VALUES (?, NOW(), ?)", "/api/status", fmt.Sprintf("req_%d", reqNum))
			}(requestCount)

			emit(Event{Ts: now(), Type: "HTTPRequest", Goroutine: "http-server",
				Value:    fmt.Sprintf("GET /api/status (request #%d)", requestCount),
				Response: fmt.Sprintf("200 OK (total: %d)", requestCount), IsUpdate: true})
			fmt.Fprintf(w, `{"status": "ok", "requests": %d}`, requestCount)
		})

		server := &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: mux,
		}

		go func() {
			if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				emit(Event{Ts: now(), Type: "Error", Goroutine: "http-server", Text: fmt.Sprintf("Server error: %v", err)})
			}
		}()

		emit(Event{Ts: now(), Type: "ServerStarted", Goroutine: "http-server",
			Response: fmt.Sprintf("Server listening on port %d", port)})

		// Keep running until terminated
		for {
			if CheckControl(control) {
				server.Close()
				break
			}
			time.Sleep(1 * time.Second)
		}

		return map[string]interface{}{"requestsHandled": requestCount, "status": "shutdown"}
	})

	// Database connection pool simulator
	GoWithParamsAndControl(ctx, "db-pool", map[string]interface{}{"maxConnections": 10}, "map[string]interface{}", true, func(ctx context.Context, params interface{}, control *GoroutineInfo) interface{} {
		ctx = context.WithValue(ctx, "g", "db-pool")
		p := params.(map[string]interface{})
		maxConn := p["maxConnections"].(int)

		connections := 0
		queries := int64(0)

		for {
			if CheckControl(control) {
				break
			}

			// Simulate database activity
			if connections < maxConn {
				connections++
				atomic.AddInt64(&queries, 1)

				// Use APM-style database query tracing
				queryType := []string{"SELECT * FROM users WHERE active = true", "SELECT COUNT(*) FROM orders WHERE status = 'pending'", "UPDATE session_logs SET last_accessed = NOW() WHERE user_id = ?", "INSERT INTO page_views (url, user_id, timestamp) VALUES (?, ?, NOW())"}
				selectedQuery := queryType[queries%int64(len(queryType))]

				// Simulate actual database query with APM tracing
				go func(queryNum int64, query string) {
					queryCtx := context.WithValue(ctx, "g", "db-pool")
					_, _ = DBQuery(queryCtx, query, fmt.Sprintf("param_%d", queryNum))

					// Also emit the legacy event for compatibility
					emit(Event{Ts: now(), Type: "DBQuery", Goroutine: "db-pool",
						Value:    fmt.Sprintf("Query #%d: %s", queryNum, query),
						Response: fmt.Sprintf("connections: %d/%d", connections, maxConn), IsUpdate: true})
				}(queries, selectedQuery)

				// Simulate query time
				time.Sleep(time.Duration(100+connections*50) * time.Millisecond)
				connections--
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}

		return map[string]interface{}{"totalQueries": queries, "maxConnections": maxConn}
	})

	// Background worker
	GoWithParamsAndControl(ctx, "background-worker", map[string]interface{}{"interval": "5s"}, "map[string]interface{}", true, func(ctx context.Context, params interface{}, control *GoroutineInfo) interface{} {
		ctx = context.WithValue(ctx, "g", "background-worker")

		tasksProcessed := int64(0)
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if CheckControl(control) {
					return map[string]interface{}{"tasksProcessed": tasksProcessed}
				}

				atomic.AddInt64(&tasksProcessed, 1)

				// Use APM-style tracing for background tasks
				taskCtx := context.WithValue(ctx, "g", "background-worker")

				// Simulate different types of background tasks
				taskTypes := []struct {
					name  string
					query string
				}{
					{"cleanup_sessions", "DELETE FROM user_sessions WHERE expires_at < NOW()"},
					{"update_statistics", "UPDATE daily_stats SET page_views = (SELECT COUNT(*) FROM page_views WHERE DATE(created_at) = CURRENT_DATE)"},
					{"process_queue", "SELECT * FROM job_queue WHERE status = 'pending' ORDER BY created_at ASC LIMIT 10"},
					{"cache_warmup", "SELECT id, data FROM popular_content WHERE view_count > 1000"},
				}

				selectedTask := taskTypes[tasksProcessed%int64(len(taskTypes))]

				go func(taskNum int64, task struct {
					name  string
					query string
				}) {
					// Perform the database operation
					_, _ = DBQuery(taskCtx, task.query, nil)

					// Also cache some results
					_ = CacheSet(taskCtx, fmt.Sprintf("task_result_%s_%d", task.name, taskNum),
						fmt.Sprintf("completed_%d", taskNum), 10*time.Minute)
				}(tasksProcessed, selectedTask)

				emit(Event{Ts: now(), Type: "TaskProcessed", Goroutine: "background-worker",
					Value:    fmt.Sprintf("Task #%d: %s", tasksProcessed, selectedTask.name),
					Response: fmt.Sprintf("background task completed: %s", selectedTask.name), IsUpdate: true})
			default:
				if CheckControl(control) {
					return map[string]interface{}{"tasksProcessed": tasksProcessed}
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	})

	fmt.Fprintln(w, "Long-running server example started")
}

func runAPMDemoSnippet(w http.ResponseWriter, r *http.Request) {
	clearAllGoroutines() // Clear previous goroutines
	startTime = time.Now()
	ctx := context.WithValue(context.Background(), "g", "main")
	emit(Event{Ts: now(), Type: "TraceStart", Goroutine: "main", Text: "APM Tracing Demo - Microservice Operations"})

	// User service goroutine
	GoWithParams(ctx, "user-service", map[string]interface{}{"service": "user-service", "version": "1.0"}, "map[string]interface{}", func(ctx context.Context, params interface{}) interface{} {
		ctx = context.WithValue(ctx, "g", "user-service")

		// Simulate user creation flow
		userID := 12345
		userData := map[string]interface{}{
			"name":  "John Doe",
			"email": "john@example.com",
		}

		// Database query to check if user exists
		_, err := DBQuery(ctx, "SELECT id FROM users WHERE email = ?", userData["email"])
		if err != nil {
			// User doesn't exist, create new user
			result, _ := DBQuery(ctx, "INSERT INTO users (name, email) VALUES (?, ?)", userData["name"], userData["email"])
			userID = result.(map[string]interface{})["insertedId"].(int)
		}

		// Cache user data
		CacheSet(ctx, fmt.Sprintf("user:%d", userID), userData, 1*time.Hour)

		// Publish user created event
		PublishMessage(ctx, "user.created", map[string]interface{}{
			"userId": userID,
			"email":  userData["email"],
		})

		return map[string]interface{}{"userId": userID, "status": "created"}
	})

	// Order service goroutine
	GoWithParams(ctx, "order-service", map[string]interface{}{"service": "order-service", "version": "1.2"}, "map[string]interface{}", func(ctx context.Context, params interface{}) interface{} {
		ctx = context.WithValue(ctx, "g", "order-service")

		orderID := 67890
		userID := 12345

		// Get user data from cache
		userData, err := CacheGet(ctx, fmt.Sprintf("user:%d", userID))
		if err != nil {
			// Cache miss, get from database
			userData, _ = DBQuery(ctx, "SELECT * FROM users WHERE id = ?", userID)
		}

		// Use userData for order creation
		emit(Event{Ts: now(), Type: "UserDataRetrieved", Goroutine: "order-service",
			Value: fmt.Sprintf("Retrieved user data: %v", userData), IsUpdate: true})

		// Create order
		orderData := map[string]interface{}{
			"userId": userID,
			"items":  []string{"item1", "item2"},
			"total":  99.99,
			"status": "pending",
		}

		DBQuery(ctx, "INSERT INTO orders (user_id, items, total, status) VALUES (?, ?, ?, ?)",
			userID, "item1,item2", 99.99, "pending")

		// Call payment service
		paymentResult, _ := HTTPRequest(ctx, "POST", "https://payment-service/api/charge", map[string]interface{}{
			"orderId": orderID,
			"amount":  99.99,
		})

		// Update order status based on payment
		if paymentResult.(map[string]interface{})["statusCode"].(int) == 200 {
			DBQuery(ctx, "UPDATE orders SET status = ? WHERE id = ?", "paid", orderID)
			orderData["status"] = "paid"
		}

		// Cache order data
		CacheSet(ctx, fmt.Sprintf("order:%d", orderID), orderData, 30*time.Minute)

		// Publish order created event
		PublishMessage(ctx, "order.created", orderData)

		return map[string]interface{}{"orderId": orderID, "status": orderData["status"]}
	})

	// Inventory service goroutine
	GoWithParams(ctx, "inventory-service", map[string]interface{}{"service": "inventory-service", "version": "1.1"}, "map[string]interface{}", func(ctx context.Context, params interface{}) interface{} {
		ctx = context.WithValue(ctx, "g", "inventory-service")

		// Consume order created events
		for i := 0; i < 3; i++ {
			message, _ := ConsumeMessage(ctx, "order.created")
			orderData := message.(map[string]interface{})["message"]

			// Check inventory
			items := []string{"item1", "item2"}
			for _, item := range items {
				// Check cache first
				stock, err := CacheGet(ctx, fmt.Sprintf("inventory:%s", item))
				if err != nil {
					// Cache miss, get from database
					stock, _ = DBQuery(ctx, "SELECT quantity FROM inventory WHERE item = ?", item)
				}

				// Log current stock level
				emit(Event{Ts: now(), Type: "StockCheck", Goroutine: "inventory-service",
					Value: fmt.Sprintf("Current stock for %s: %v", item, stock), IsUpdate: true})

				// Update inventory
				DBQuery(ctx, "UPDATE inventory SET quantity = quantity - 1 WHERE item = ?", item)

				// Update cache
				newStock := 99 // Simulated new stock
				CacheSet(ctx, fmt.Sprintf("inventory:%s", item), newStock, 10*time.Minute)
			}

			// Publish inventory updated event
			PublishMessage(ctx, "inventory.updated", map[string]interface{}{
				"items": items,
				"order": orderData,
			})

			time.Sleep(100 * time.Millisecond)
		}

		return map[string]interface{}{"processedOrders": 3}
	})

	// Notification service goroutine
	GoWithParams(ctx, "notification-service", map[string]interface{}{"service": "notification-service", "version": "1.0"}, "map[string]interface{}", func(ctx context.Context, params interface{}) interface{} {
		ctx = context.WithValue(ctx, "g", "notification-service")

		// Consume various events and send notifications
		events := []string{"user.created", "order.created", "inventory.updated"}

		for _, eventType := range events {
			message, _ := ConsumeMessage(ctx, eventType)

			// Get notification preferences
			userData, _ := CacheGet(ctx, "user:12345")
			if userData == nil {
				userData, _ = DBQuery(ctx, "SELECT email FROM users WHERE id = ?", 12345)
			}

			// Send email notification (simulated with HTTP call)
			HTTPRequest(ctx, "POST", "https://email-service/api/send", map[string]interface{}{
				"to":      "john@example.com",
				"subject": fmt.Sprintf("Event: %s", eventType),
				"body":    fmt.Sprintf("Event data: %v", message),
			})

			// Send push notification
			HTTPRequest(ctx, "POST", "https://push-service/api/send", map[string]interface{}{
				"userId":  12345,
				"message": fmt.Sprintf("New %s event", eventType),
			})

			time.Sleep(50 * time.Millisecond)
		}

		return map[string]interface{}{"sentNotifications": len(events) * 2}
	})

	// Analytics service goroutine
	GoWithParams(ctx, "analytics-service", map[string]interface{}{"service": "analytics-service", "version": "2.0"}, "map[string]interface{}", func(ctx context.Context, params interface{}) interface{} {
		ctx = context.WithValue(ctx, "g", "analytics-service")

		// Collect and analyze metrics
		metrics := []string{"user_signups", "order_volume", "inventory_changes"}

		for _, metric := range metrics {
			// Get current metrics from cache
			currentValue, err := CacheGet(ctx, fmt.Sprintf("metrics:%s", metric))
			if err != nil {
				// Calculate from database
				switch metric {
				case "user_signups":
					currentValue, _ = DBQuery(ctx, "SELECT COUNT(*) as count FROM users WHERE created_at > NOW() - INTERVAL 1 HOUR")
				case "order_volume":
					currentValue, _ = DBQuery(ctx, "SELECT SUM(total) as volume FROM orders WHERE created_at > NOW() - INTERVAL 1 HOUR")
				case "inventory_changes":
					currentValue, _ = DBQuery(ctx, "SELECT COUNT(*) as changes FROM inventory_log WHERE created_at > NOW() - INTERVAL 1 HOUR")
				}
			}

			// Update metrics cache
			CacheSet(ctx, fmt.Sprintf("metrics:%s", metric), currentValue, 5*time.Minute)

			// Send metrics to external analytics service
			HTTPRequest(ctx, "POST", "https://analytics.example.com/api/metrics", map[string]interface{}{
				"metric":    metric,
				"value":     currentValue,
				"timestamp": time.Now().Unix(),
			})

			time.Sleep(75 * time.Millisecond)
		}

		return map[string]interface{}{"processedMetrics": len(metrics)}
	})

	time.AfterFunc(20*time.Second, func() {
		emit(Event{Ts: now(), Type: "TraceEnd", Goroutine: "main", Text: "APM Demo completed"})
	})

	fmt.Fprintln(w, "APM tracing demo started - microservice operations with database, cache, HTTP, and message queue tracing")
}

// HTTP handlers for goroutine control
func handleGoroutineControl(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	action := r.URL.Query().Get("action")
	goroutineName := r.URL.Query().Get("name")

	if goroutineName == "" {
		http.Error(w, `{"error": "goroutine name required"}`, http.StatusBadRequest)
		return
	}

	info, exists := getGoroutineInfo(goroutineName)
	if !exists {
		http.Error(w, `{"error": "goroutine not found"}`, http.StatusNotFound)
		return
	}

	switch action {
	case "pause":
		if info.Status != "running" {
			fmt.Fprintf(w, `{"status": "cannot_pause", "goroutine": "%s", "current_status": "%s"}`, goroutineName, info.Status)
			return
		}
		select {
		case info.PauseChannel <- true:
			fmt.Fprintf(w, `{"status": "pause_requested", "goroutine": "%s"}`, goroutineName)
		default:
			fmt.Fprintf(w, `{"status": "pause_channel_full", "goroutine": "%s"}`, goroutineName)
		}
	case "resume":
		if info.Status != "paused" {
			fmt.Fprintf(w, `{"status": "not_paused", "goroutine": "%s", "current_status": "%s"}`, goroutineName, info.Status)
			return
		}
		select {
		case info.ResumeChannel <- true:
			fmt.Fprintf(w, `{"status": "resume_requested", "goroutine": "%s"}`, goroutineName)
		default:
			fmt.Fprintf(w, `{"status": "resume_channel_full", "goroutine": "%s"}`, goroutineName)
		}
	case "terminate":
		if info.Status == "terminated" {
			fmt.Fprintf(w, `{"status": "already_terminated", "goroutine": "%s"}`, goroutineName)
			return
		}
		select {
		case info.TerminateChannel <- true:
			fmt.Fprintf(w, `{"status": "terminate_requested", "goroutine": "%s"}`, goroutineName)
		default:
			fmt.Fprintf(w, `{"status": "terminate_channel_full", "goroutine": "%s"}`, goroutineName)
		}
	default:
		http.Error(w, `{"error": "invalid action"}`, http.StatusBadRequest)
	}
}

func handleGoroutineList(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	list := listGoroutines()

	type GoroutineStatus struct {
		ID            string  `json:"id"`
		Name          string  `json:"name"`
		Status        string  `json:"status"`
		StartTime     string  `json:"startTime"`
		EndTime       *string `json:"endTime,omitempty"`
		Duration      float64 `json:"duration,omitempty"`
		Params        string  `json:"params,omitempty"`
		ParamTypes    string  `json:"paramTypes,omitempty"`
		Response      string  `json:"response,omitempty"`
		RespType      string  `json:"respType,omitempty"`
		IsLongRunning bool    `json:"isLongRunning"`
		LastHeartbeat string  `json:"lastHeartbeat"`
		CallCount     int64   `json:"callCount"`
	}

	var statusList []GoroutineStatus
	for _, info := range list {
		status := GoroutineStatus{
			ID:            info.ID,
			Name:          info.Name,
			Status:        info.Status,
			StartTime:     info.StartTime.Format(time.RFC3339),
			Params:        fmt.Sprintf("%v", info.Params),
			ParamTypes:    info.ParamTypes,
			Response:      fmt.Sprintf("%v", info.Response),
			RespType:      info.RespType,
			IsLongRunning: info.IsLongRunning,
			LastHeartbeat: info.LastHeartbeat.Format(time.RFC3339),
			CallCount:     info.CallCount,
		}

		if info.EndTime != nil {
			endTimeStr := info.EndTime.Format(time.RFC3339)
			status.EndTime = &endTimeStr
			status.Duration = info.EndTime.Sub(info.StartTime).Seconds()
		}

		statusList = append(statusList, status)
	}

	if err := json.NewEncoder(w).Encode(statusList); err != nil {
		http.Error(w, `{"error": "encoding failed"}`, http.StatusInternalServerError)
	}
}

////////////////////////////////////////////////////////////////////////////////
// 5) Sample workload & HTTP server
////////////////////////////////////////////////////////////////////////////////

func main() {
	addr := flag.String("addr", ":8090", "listen address")
	flag.Parse()

	// Start cleanup routine
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			cleanupGoroutines()
		}
	}()

	http.Handle("/ws", websocket.Handler(wsHandler))
	http.Handle("/", http.FileServer(http.Dir("./ui")))

	// Goroutine control endpoints
	http.HandleFunc("/api/goroutines", handleGoroutineList)
	http.HandleFunc("/api/goroutine/control", handleGoroutineControl)
	http.HandleFunc("/api/clear", func(w http.ResponseWriter, r *http.Request) {
		clearAllGoroutines()
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintln(w, `{"status": "cleared"}`)
	})

	// Buffered Channel snippet
	http.HandleFunc("/run-buffered-channel", func(w http.ResponseWriter, r *http.Request) {
		runBufferedChannelSnippet(w, r)
	})

	// Mutex snippet
	http.HandleFunc("/run-mutex", func(w http.ResponseWriter, r *http.Request) {
		runMutexSnippet(w, r)
	})

	// WaitGroup snippet
	http.HandleFunc("/run-waitgroup", func(w http.ResponseWriter, r *http.Request) {
		runWaitGroupSnippet(w, r)
	})

	// Long-running server snippet
	http.HandleFunc("/run-long-running", func(w http.ResponseWriter, r *http.Request) {
		runLongRunningServerSnippet(w, r)
	})

	// APM Tracing Demo snippet
	http.HandleFunc("/run-apm-demo", func(w http.ResponseWriter, r *http.Request) {
		runAPMDemoSnippet(w, r)
	})

	// When the user hits /start, fire up a sample instrumented workload.
	http.HandleFunc("/start", func(w http.ResponseWriter, r *http.Request) {
		clearAllGoroutines() // Clear previous goroutines
		startTime = time.Now()
		ctx := context.WithValue(context.Background(), "g", "main")
		emit(Event{Ts: now(), Type: "TraceStart", Goroutine: "main"})

		// spawn stack sampler for "main"
		go stackSampler(ctx, 500*time.Millisecond)

		// instrumented channel and mutex
		jobs := NewChan[int](5, "jobs")
		mu := NewMutex("stateMu")

		// launch 3 workers
		var wg sync.WaitGroup
		for i := 1; i <= 3; i++ {
			wg.Add(1)
			name := fmt.Sprintf("worker-%d", i)
			GoWithParams(ctx, name, map[string]interface{}{"workerId": i, "jobCount": 3}, "map[string]interface{}", func(ctx context.Context, params interface{}) interface{} {
				defer wg.Done()
				// each worker gets its own ctx value
				ctx = context.WithValue(ctx, "g", name)
				p := params.(map[string]interface{})
				workerId := p["workerId"].(int)
				jobCount := p["jobCount"].(int)

				processedJobs := 0
				for j := 1; j <= jobCount; j++ {
					WithRegion(ctx, "processJob", func() {
						time.Sleep(time.Duration(100+workerId*50) * time.Millisecond)
					})
					mu.Lock(ctx)
					// pretend state update
					mu.Unlock(ctx)
					job := jobs.Recv(ctx)
					processedJobs++
					emit(Event{Ts: now(), Type: "JobProcessed", Goroutine: name, Value: fmt.Sprintf("job-%d", job), Response: fmt.Sprintf("processed %d jobs", processedJobs)})
				}
				return map[string]interface{}{"processedJobs": processedJobs, "workerId": workerId}
			})
		}

		// send 3 × 3 = 9 jobs
		GoWithParams(ctx, "job-feeder", map[string]interface{}{"totalJobs": 9}, "map[string]interface{}", func(ctx context.Context, params interface{}) interface{} {
			ctx = context.WithValue(ctx, "g", "job-feeder")
			p := params.(map[string]interface{})
			totalJobs := p["totalJobs"].(int)

			for j := 0; j < totalJobs; j++ {
				jobs.Send(ctx, j)
				time.Sleep(50 * time.Millisecond)
			}
			return map[string]interface{}{"jobsSent": totalJobs}
		})

		go func() {
			wg.Wait()
			emit(Event{Ts: now(), Type: "TraceEnd", Goroutine: "main"})
		}()

		fmt.Fprintln(w, "workload started")
	})

	log.Printf("listening on %s …\n", *addr)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
