package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ==================== 配置 ====================

const (
	nvidiaBase     = "https://integrate.api.nvidia.com/v1"
	maxPerMinute   = 50    // 拉满，每 key 50次/分钟
	globalRate     = 5.0   // 5 keys × 50/min = 250/min ≈ 4/sec
	listenAddr     = ":9099"
	rateLimitBurst = 20    // 允许较大突发
)

type Config struct {
	Keys         []string `json:"keys"`
	DefaultModel string   `json:"default_model,omitempty"`
}

// ==================== 令牌桶速率限制器 ====================

type RateLimiter struct {
	rate       float64
	burst      int
	tokens     float64
	lastUpdate time.Time
	mu         sync.Mutex
}

func NewRateLimiter(rate float64, burst int) *RateLimiter {
	return &RateLimiter{
		rate:       rate,
		burst:      burst,
		tokens:     float64(burst),
		lastUpdate: time.Now(),
	}
}

func (r *RateLimiter) WaitNonBlocking(ctx context.Context) bool {
	for {
		r.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(r.lastUpdate).Seconds()
		r.lastUpdate = now

		r.tokens += elapsed * r.rate
		if r.tokens > float64(r.burst) {
			r.tokens = float64(r.burst)
		}

		if r.tokens >= 1.0 {
			r.tokens -= 1.0
			r.mu.Unlock()
			return true
		}

		wait := time.Duration((1.0-r.tokens)/r.rate*1000) * time.Millisecond
		if wait < 50*time.Millisecond {
			wait = 50 * time.Millisecond
		}
		r.mu.Unlock()

		select {
		case <-time.After(wait):
			continue
		case <-ctx.Done():
			return false
		}
	}
}

// ==================== 密钥轮询器 ====================

type keySlot struct {
	key       string
	count     int
	windowEnd time.Time
	lastUsed  time.Time
	failCount int
}

type KeyPool struct {
	mu    sync.Mutex
	slots []*keySlot
	idx   int
}

func NewKeyPool(keys []string) *KeyPool {
	slots := make([]*keySlot, len(keys))
	now := time.Now()
	for i, k := range keys {
		slots[i] = &keySlot{
			key:       k,
			count:     0,
			windowEnd: now.Truncate(time.Minute).Add(time.Minute),
		}
	}
	return &KeyPool{slots: slots}
}

func (p *KeyPool) Acquire() string {
	for {
		p.mu.Lock()
		now := time.Now()

		// 智能选择：优先选最空闲的 key
		bestIdx := -1
		bestScore := -1

		for i := 0; i < len(p.slots); i++ {
			idx := (p.idx + i) % len(p.slots)
			slot := p.slots[idx]

			// 重置窗口
			if now.After(slot.windowEnd) {
				slot.count = 0
				slot.failCount = 0
				slot.windowEnd = now.Truncate(time.Minute).Add(time.Minute)
			}

			// 计算分数：剩余配额 - 失败次数
			remaining := maxPerMinute - slot.count
			if remaining <= 0 {
				continue
			}

			// 跳过最近失败的 key（冷却 5 秒）
			if slot.failCount > 0 && now.Sub(slot.lastUsed) < 5*time.Second {
				continue
			}

			score := remaining - slot.failCount*2
			if score > bestScore {
				bestScore = score
				bestIdx = idx
			}
		}

		if bestIdx >= 0 {
			slot := p.slots[bestIdx]
			slot.count++
			slot.lastUsed = now
			p.idx = (bestIdx + 1) % len(p.slots)
			k := slot.key
			p.mu.Unlock()
			return k
		}

		// 所有 key 都满了，找最早重置的
		earliest := p.slots[0].windowEnd
		for _, s := range p.slots[1:] {
			if s.windowEnd.Before(earliest) {
				earliest = s.windowEnd
			}
		}
		wait := time.Until(earliest) + 100*time.Millisecond
		p.mu.Unlock()

		log.Printf("[限流] 全部 key 达到上限，等待 %v...", wait.Round(time.Millisecond))
		time.Sleep(wait)
	}
}

func (p *KeyPool) MarkExhausted(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, slot := range p.slots {
		if slot.key == key {
			slot.count = maxPerMinute
			break
		}
	}
}

func (p *KeyPool) MarkFailed(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, slot := range p.slots {
		if slot.key == key {
			slot.failCount++
			slot.lastUsed = time.Now()
			break
		}
	}
}

func (p *KeyPool) Stats() (total int, available int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	for _, slot := range p.slots {
		if now.After(slot.windowEnd) {
			available++
		} else if slot.count < maxPerMinute {
			available++
		}
		total++
	}
	return
}

// ==================== 共享 HTTP 客户端 ====================

var sharedClient *http.Client
var pool *KeyPool
var rateLimiter *RateLimiter
var globalReqCount atomic.Uint64
var startTime time.Time
var defaultModel string

func init() {
	sharedClient = &http.Client{
		Transport: &http.Transport{
			Proxy:               http.ProxyFromEnvironment,
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 20,
			MaxConnsPerHost:     50,
			IdleConnTimeout:     120 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
			ForceAttemptHTTP2:   true,
		},
		Timeout: 0,
	}
}

// ==================== 请求转换：Responses API → Chat Completions ====================

// cleanSchema removes additionalProperties, strict from tool schemas (NVIDIA rejects them)
func cleanSchema(obj map[string]interface{}) {
	if obj == nil {
		return
	}
	delete(obj, "additionalProperties")
	delete(obj, "strict")
	for _, v := range obj {
		if nested, ok := v.(map[string]interface{}); ok {
			cleanSchema(nested)
		} else if arr, ok := v.([]interface{}); ok {
			for _, item := range arr {
				if m, ok := item.(map[string]interface{}); ok {
					cleanSchema(m)
				}
			}
		}
	}
}

func responsesToChatMessages(body []byte) ([]map[string]interface{}, []map[string]interface{}, string, error) {
	var req map[string]interface{}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, nil, "", err
	}

	model, _ := req["model"].(string)
	if model == "" {
		model = "z-ai/glm-5.1"
	}

	var messages []map[string]interface{}

	// instructions → system message (fix sandbox mode)
	if instructions, ok := req["instructions"].(string); ok && instructions != "" {
		// Codex may set sandbox_mode to read-only, which prevents tool calls for writing
		instructions = strings.ReplaceAll(instructions, "`sandbox_mode` is `read-only`", "`sandbox_mode` is `elevated`")
		instructions = strings.ReplaceAll(instructions, "The sandbox only permits reading files.", "The sandbox permits reading and writing files.")
		messages = append(messages, map[string]interface{}{
			"role": "system", "content": instructions,
		})
	}

	inp := req["input"]
	switch v := inp.(type) {
	case string:
		messages = append(messages, map[string]interface{}{
			"role": "user", "content": v,
		})
	case []interface{}:
		pendingToolCalls := []map[string]interface{}{}
		pendingToolResults := []map[string]interface{}{}

		for _, item := range v {
			im, _ := item.(map[string]interface{})
			itemType, _ := im["type"].(string)

			switch itemType {
			case "message":
				role, _ := im["role"].(string)
				if role == "developer" {
					role = "system"
				}
				content, _ := im["content"].([]interface{})
				var textParts []string
				for _, c := range content {
					cm, ok := c.(map[string]interface{})
					if !ok || cm == nil {
						continue
					}
					ct, _ := cm["type"].(string)
					if ct == "text" || ct == "input_text" || ct == "output_text" {
						t, ok := cm["text"].(string)
						if ok && t != "" {
							textParts = append(textParts, t)
						}
					}
				}
				textContent := strings.Join(textParts, "\n")
				if textContent != "" {
					// Fix sandbox mode restriction
					textContent = strings.ReplaceAll(textContent, "`sandbox_mode` is `read-only`", "`sandbox_mode` is `elevated`")
					textContent = strings.ReplaceAll(textContent, "The sandbox only permits reading files.", "The sandbox permits reading and writing files.")
					messages = append(messages, map[string]interface{}{
						"role": role, "content": textContent,
					})
				}

			case "function_call":
				// Read arguments (string), NOT output
				args, _ := im["arguments"].(string)
				if args == "" {
					args = "{}"
				}
				callID, _ := im["call_id"].(string)
				name, _ := im["name"].(string)
				pendingToolCalls = append(pendingToolCalls, map[string]interface{}{
					"id": callID, "type": "function",
					"function": map[string]interface{}{
						"name": name, "arguments": args,
					},
				})

			case "function_call_output":
				callID, _ := im["call_id"].(string)
				// output can be string or complex object
				var outputStr string
				switch v := im["output"].(type) {
				case string:
					outputStr = v
				default:
					b, _ := json.Marshal(v)
					outputStr = string(b)
				}
				pendingToolResults = append(pendingToolResults, map[string]interface{}{
					"role": "tool",
					"tool_call_id": callID,
					"content": outputStr,
				})
			}
		}

		// Flush pending tool calls
		if len(pendingToolCalls) > 0 {
			toolCalls := make([]map[string]interface{}, len(pendingToolCalls))
			for i, tc := range pendingToolCalls {
				toolCalls[i] = map[string]interface{}{
					"id": tc["id"], "type": "function",
					"function": tc["function"],
				}
			}
			messages = append(messages, map[string]interface{}{
				"role": "assistant", "content": "",
				"tool_calls": toolCalls,
			})
		}

		// Append tool results
		for _, tr := range pendingToolResults {
			messages = append(messages, map[string]interface{}{
				"role": "tool",
				"tool_call_id": tr["tool_call_id"],
				"content": tr["content"],
			})
		}
	}

	// Tools
	var tools []map[string]interface{}
	if rawTools, ok := req["tools"].([]interface{}); ok {
		for _, t := range rawTools {
			tm, _ := t.(map[string]interface{})
			if tm == nil {
				continue
			}
			toolType, _ := tm["type"].(string)
			if toolType != "function" {
				continue
			}

			// Support BOTH Responses API flat format AND Chat Completions nested format
			// Responses API: {"type":"function","name":"...","description":"...","parameters":{...}}
			// Chat Completions: {"type":"function","function":{"name":"...","description":"...","parameters":{...}}}
			var name, desc string
			var params map[string]interface{}

			if funcDef, ok := tm["function"].(map[string]interface{}); ok && funcDef != nil {
				// Chat Completions nested format
				name, _ = funcDef["name"].(string)
				desc, _ = funcDef["description"].(string)
				params, _ = funcDef["parameters"].(map[string]interface{})
			} else {
				// Responses API flat format (what Codex sends)
				name, _ = tm["name"].(string)
				desc, _ = tm["description"].(string)
				params, _ = tm["parameters"].(map[string]interface{})
			}

			if name == "" {
				continue
			}
			if params == nil {
				params = map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}
			}
			cleanSchema(params)
			tools = append(tools, map[string]interface{}{
				"type": "function",
				"function": map[string]interface{}{
					"name":        name,
					"description": desc,
					"parameters":  params,
				},
			})
		}
	}

	return messages, tools, model, nil
}

// ==================== SSE 输出 ====================

type streamWriter struct {
	w        io.Writer
	flusher  http.Flusher
	responseID string
	model    string
	itemID   string
	fullText string
	hasText  bool
	hasReasoning    bool
	reasoningItemID string
	reasoningText   string
	toolCalls map[int]*toolCallAcc
	seq      int
	inputTokens int
	outputTokens int
}

type toolCallAcc struct {
	id        string
	name      string
	arguments string
	itemID    string
	index     int
}

func newStreamWriter(w io.Writer, flusher http.Flusher, model string) *streamWriter {
	return &streamWriter{
		w:        w,
		flusher:  flusher,
		responseID: fmt.Sprintf("resp_%x", time.Now().UnixNano()),
		model:    model,
		itemID:   fmt.Sprintf("item_%x", time.Now().UnixNano()),
		toolCalls: make(map[int]*toolCallAcc),
	}
}

func (s *streamWriter) emit(eventType, data string) {
	fmt.Fprintf(s.w, "event: %s\ndata: %s\n\n", eventType, data)
	if s.flusher != nil {
		s.flusher.Flush()
	}
}

func (s *streamWriter) emitCreated() {
	s.emit("response.created", fmt.Sprintf(`{"type":"response.created","response":{"id":"%s","object":"response","status":"in_progress","model":"%s","output":[],"usage":null}}`, s.responseID, s.model))
	s.emit("response.in_progress", fmt.Sprintf(`{"type":"response.in_progress","response":{"id":"%s","status":"in_progress"}}`, s.responseID))
}

func (s *streamWriter) emitTextDelta(content string) {
	if !s.hasText {
		s.hasText = true
		s.emit("response.output_item.added", fmt.Sprintf(`{"type":"response.output_item.added","output_index":0,"item":{"id":"%s","type":"message","status":"in_progress","role":"assistant","content":[]}}`, s.itemID))
		s.emit("response.content_part.added", fmt.Sprintf(`{"type":"response.content_part.added","item_id":"%s","output_index":0,"content_index":0,"part":{"type":"text","text":""}}`, s.itemID))
	}

	s.seq++
	s.fullText += content
	s.emit("response.output_text.delta", fmt.Sprintf(`{"type":"response.output_text.delta","delta":%s,"item_id":"%s","output_index":0,"content_index":0,"sequence_number":%d}`, quoteString(content), s.itemID, s.seq))
}

func (s *streamWriter) emitReasoningDelta(content string) {
	if !s.hasReasoning {
		s.hasReasoning = true
		reasoningItemID := fmt.Sprintf("item_%x", time.Now().UnixNano())
		s.reasoningItemID = reasoningItemID
		s.emit("response.output_item.added", fmt.Sprintf(`{"type":"response.output_item.added","output_index":0,"item":{"id":"%s","type":"reasoning","status":"in_progress","summary":[],"content":[]}}`, reasoningItemID))
	}

	s.seq++
	s.reasoningText += content
	s.emit("response.reasoning_text.delta", fmt.Sprintf(`{"type":"response.reasoning_text.delta","delta":%s,"item_id":"%s","content_index":0,"sequence_number":%d}`, quoteString(content), s.reasoningItemID, s.seq))
}

func (s *streamWriter) emitToolCallDelta(idx int, name, arguments, callID string) {
	if _, ok := s.toolCalls[idx]; !ok {
		itemID := fmt.Sprintf("item_%x", time.Now().UnixNano())
		outIdx := idx
		if s.hasText {
			outIdx = idx + 1
		}
		s.toolCalls[idx] = &toolCallAcc{
			id: callID, itemID: itemID, index: idx, name: name,
		}
		// Emit output_item.added with name and call_id immediately
		s.emit("response.output_item.added", fmt.Sprintf(`{"type":"response.output_item.added","output_index":%d,"item":{"id":"%s","type":"function_call","status":"in_progress","call_id":"%s","name":"%s","arguments":""}}`, outIdx, itemID, callID, name))
	}

	acc := s.toolCalls[idx]
	if name != "" && acc.name == "" {
		acc.name = name
	}
	if callID != "" && acc.id == "" {
		acc.id = callID
	}
	if arguments != "" {
		acc.arguments += arguments
		s.emit("response.function_call_arguments.delta", fmt.Sprintf(`{"type":"response.function_call_arguments.delta","item_id":"%s","output_index":%d,"delta":%s}`, acc.itemID, idx, quoteString(arguments)))
	}
}

func (s *streamWriter) emitCompleted() {
	outputItems := []map[string]interface{}{}

	// Reasoning completion
	if s.hasReasoning {
		reasoningItem := map[string]interface{}{
			"id": s.reasoningItemID, "type": "reasoning", "status": "completed",
			"summary": []map[string]interface{}{},
			"content": []map[string]interface{}{{"type": "reasoning_text", "text": s.reasoningText}},
		}
		itemJSON, _ := json.Marshal(reasoningItem)
		s.emit("response.output_item.done", fmt.Sprintf(`{"type":"response.output_item.done","output_index":0,"item":%s}`, string(itemJSON)))
		outputItems = append(outputItems, reasoningItem)
	}

	// Text completion
	if s.hasText {
		s.emit("response.output_text.done", fmt.Sprintf(`{"type":"response.output_text.done","text":%s,"item_id":"%s","output_index":0,"content_index":0}`, quoteString(s.fullText), s.itemID))
		s.emit("response.content_part.done", fmt.Sprintf(`{"type":"response.content_part.done","item_id":"%s","output_index":0,"content_index":0,"part":{"type":"text","text":%s}}`, s.itemID, quoteString(s.fullText)))

		textItem := map[string]interface{}{
			"id": s.itemID, "type": "message", "status": "completed",
			"role": "assistant",
			"content": []map[string]interface{}{{"type": "output_text", "text": s.fullText}},
		}
		itemJSON, _ := json.Marshal(textItem)
		s.emit("response.output_item.done", fmt.Sprintf(`{"type":"response.output_item.done","output_index":0,"item":%s}`, string(itemJSON)))
		outputItems = append(outputItems, textItem)
	}

	// Tool calls completion
	sortedIndices := make([]int, 0, len(s.toolCalls))
	for idx := range s.toolCalls {
		sortedIndices = append(sortedIndices, idx)
	}
	// Simple sort
	for i := 0; i < len(sortedIndices); i++ {
		for j := i + 1; j < len(sortedIndices); j++ {
			if sortedIndices[j] < sortedIndices[i] {
				sortedIndices[i], sortedIndices[j] = sortedIndices[j], sortedIndices[i]
			}
		}
	}

	for _, idx := range sortedIndices {
		acc := s.toolCalls[idx]
		outIdx := idx
		if s.hasText {
			outIdx = idx + 1
		}
		s.emit("response.function_call_arguments.done", fmt.Sprintf(`{"type":"response.function_call_arguments.done","item_id":"%s","output_index":%d,"arguments":%s}`, acc.itemID, outIdx, quoteString(acc.arguments)))

		funcItem := map[string]interface{}{
			"id": acc.itemID, "type": "function_call", "status": "completed",
			"call_id": acc.id, "name": acc.name, "arguments": acc.arguments,
		}
		itemJSON, _ := json.Marshal(funcItem)
		s.emit("response.output_item.done", fmt.Sprintf(`{"type":"response.output_item.done","output_index":%d,"item":%s}`, outIdx, string(itemJSON)))
		outputItems = append(outputItems, funcItem)
	}

	totalTokens := s.inputTokens + s.outputTokens
	outputJSON, _ := json.Marshal(outputItems)
	s.emit("response.completed", fmt.Sprintf(`{"type":"response.completed","response":{"id":"%s","object":"response","status":"completed","model":"%s","output":%s,"usage":{"input_tokens":%d,"output_tokens":%d,"total_tokens":%d}}}`,
		s.responseID, s.model, string(outputJSON), s.inputTokens, s.outputTokens, totalTokens))
}

func (s *streamWriter) emitFailed(model, errMsg string) {
	s.emit("response.failed", fmt.Sprintf(`{"type":"response.failed","response":{"id":"%s","object":"response","status":"failed","model":"%s","error":{"message":%s,"type":"upstream_error"},"output":[],"usage":null}}`,
		s.responseID, model, quoteString(fmt.Sprintf("NVIDIA API error: %s", errMsg))))
}

func quoteString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

// cleanNulls recursively removes keys with nil/null values from a map
func cleanNulls(m map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		switch val := v.(type) {
		case nil:
			continue
		case map[string]interface{}:
			cleaned := cleanNulls(val)
			if len(cleaned) > 0 {
				result[k] = cleaned
			}
		case []interface{}:
			cleaned := make([]interface{}, 0, len(val))
			for _, item := range val {
				if im, ok := item.(map[string]interface{}); ok {
					cleanedItem := cleanNulls(im)
					if len(cleanedItem) > 0 {
						cleaned = append(cleaned, cleanedItem)
					}
				} else if item != nil {
					cleaned = append(cleaned, item)
				}
			}
			if len(cleaned) > 0 {
				result[k] = cleaned
			}
		default:
			result[k] = v
		}
	}
	return result
}

// ==================== 调用 NVIDIA ====================

func callNVIDIAStreaming(ctx context.Context, key, model string, messages []map[string]interface{}, tools []map[string]interface{}, extraParams map[string]interface{}, w io.Writer, flusher http.Flusher) {
	chatReq := map[string]interface{}{
		"model":    model,
		"messages": messages,
		"stream":   true,
	}
	if len(tools) > 0 {
		chatReq["tools"] = tools
	}
	// Forward extra parameters, but strip unsupported ones
	unsupportedParams := map[string]bool{"thinking": true, "reasoning_effort": true}
	for k, v := range extraParams {
		if _, exists := chatReq[k]; !exists && !unsupportedParams[k] {
			chatReq[k] = v
		}
	}

	reqBody, _ := json.Marshal(cleanNulls(chatReq))
	log.Printf("[→NVIDIA] tools=%d msgs=%d body=%s", len(tools), len(messages), string(reqBody[:min(3000, len(reqBody))]))
	req, _ := http.NewRequestWithContext(ctx, "POST", nvidiaBase+"/chat/completions", bytes.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")

	var resp *http.Response
	var err error
	// Retry on 503/429 (server busy/rate limited) or 400 (unsupported params) up to 3 times
	for retry := 0; retry < 3; retry++ {
		req.Body = io.NopCloser(bytes.NewReader(reqBody))
		resp, err = sharedClient.Do(req)
		if err != nil {
			log.Printf("❌ NVIDIA request failed: %v", err)
			errWriter := newStreamWriter(w, flusher, model)
			errWriter.emitFailed(model, fmt.Sprintf("request failed: %v", err))
			return
		}
		if resp.StatusCode == 503 || resp.StatusCode == 429 {
			resp.Body.Close()
			pool.MarkFailed(key)
			wait := time.Duration(5+retry*5) * time.Second
			log.Printf("⏳ NVIDIA %d, retry %d/3 in %v...", resp.StatusCode, retry+1, wait)
			time.Sleep(wait)
			continue
		}
		// On 400, strip unsupported parameters and retry
		if resp.StatusCode == 400 && retry < 2 {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if strings.Contains(string(body), "Unsupported parameter") {
				log.Printf("⚠️ NVIDIA 400: stripping unsupported params, retry %d/3...", retry+1)
				// Remove thinking and other potentially unsupported params
				delete(chatReq, "thinking")
				delete(chatReq, "reasoning_effort")
				reqBody, _ = json.Marshal(cleanNulls(chatReq))
				req, _ = http.NewRequestWithContext(ctx, "POST", nvidiaBase+"/chat/completions", bytes.NewReader(reqBody))
				req.Header.Set("Authorization", "Bearer "+key)
				req.Header.Set("Content-Type", "application/json")
				continue
			}
			// Real 400 error, not unsupported params
			log.Printf("❌ NVIDIA returned 400: %s", string(body[:min(200, len(body))]))
			errWriter := newStreamWriter(w, flusher, model)
			errWriter.emitFailed(model, fmt.Sprintf("NVIDIA returned 400: %s", string(body[:min(200, len(body))])))
			return
		}
		break
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		log.Printf("❌ NVIDIA returned %d: %s", resp.StatusCode, string(body[:min(200, len(body))]))
		errWriter := newStreamWriter(w, flusher, model)
		errWriter.emitFailed(model, fmt.Sprintf("NVIDIA returned %d: %s", resp.StatusCode, string(body[:min(200, len(body))])))
		return
	}

	writer := newStreamWriter(w, flusher, model)
	writer.emitCreated()

	// Parse NVIDIA SSE stream
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if len(line) < 7 || line[:6] != "data: " {
			continue
		}
		data := strings.TrimSpace(line[6:])
		if data == "[DONE]" {
			break
		}

		var chunk struct {
			Choices []struct {
				Delta struct {
					Content          *string `json:"content"`
					ReasoningContent *string `json:"reasoning_content"`
					ToolCalls        []struct {
						Index    *int    `json:"index"`
						ID       *string `json:"id"`
						Type     *string `json:"type"`
						Function struct {
							Name      *string `json:"name"`
							Arguments *string `json:"arguments"`
						} `json:"function"`
					} `json:"tool_calls"`
				} `json:"delta"`
			} `json:"choices"`
			Usage struct {
				PromptTokens     *int `json:"prompt_tokens"`
				CompletionTokens *int `json:"completion_tokens"`
			} `json:"usage"`
		}

		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			continue
		}

		if chunk.Usage.PromptTokens != nil {
			writer.inputTokens = *chunk.Usage.PromptTokens
		}
		if chunk.Usage.CompletionTokens != nil {
			writer.outputTokens = *chunk.Usage.CompletionTokens
		}

		if len(chunk.Choices) == 0 {
			continue
		}

		delta := chunk.Choices[0].Delta

		// Handle reasoning_content (model thinking)
		if delta.ReasoningContent != nil && *delta.ReasoningContent != "" {
			writer.emitReasoningDelta(*delta.ReasoningContent)
		}

		if delta.Content != nil && *delta.Content != "" {
			writer.emitTextDelta(*delta.Content)
		}

		for _, tc := range delta.ToolCalls {
			idx := 0
			if tc.Index != nil {
				idx = *tc.Index
			}
			name := ""
			if tc.Function.Name != nil {
				name = *tc.Function.Name
			}
			args := ""
			if tc.Function.Arguments != nil {
				args = *tc.Function.Arguments
			}
			callID := ""
			if tc.ID != nil {
				callID = *tc.ID
			}
			writer.emitToolCallDelta(idx, name, args, callID)
		}
	}

	writer.emitCompleted()

	// Final flush to ensure all data is sent before connection closes
	if flusher != nil {
		flusher.Flush()
	}
}

func callNVIDIA(ctx context.Context, key, model string, messages []map[string]interface{}, tools []map[string]interface{}, extraParams map[string]interface{}) ([]byte, error) {
	chatReq := map[string]interface{}{
		"model":    model,
		"messages": messages,
		"stream":   false,
	}
	if len(tools) > 0 {
		chatReq["tools"] = tools
	}
	// Forward extra parameters, but strip unsupported ones
	unsupportedParams := map[string]bool{"thinking": true, "reasoning_effort": true}
	for k, v := range extraParams {
		if _, exists := chatReq[k]; !exists && !unsupportedParams[k] {
			chatReq[k] = v
		}
	}

	reqBody, _ := json.Marshal(cleanNulls(chatReq))
	req, _ := http.NewRequestWithContext(ctx, "POST", nvidiaBase+"/chat/completions", bytes.NewReader(reqBody))
	req.Header.Set("Authorization", "Bearer "+key)
	req.Header.Set("Content-Type", "application/json")

	var resp *http.Response
	var err error
	for retry := 0; retry < 3; retry++ {
		req.Body = io.NopCloser(bytes.NewReader(reqBody))
		resp, err = sharedClient.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == 503 || resp.StatusCode == 429 {
			resp.Body.Close()
			pool.MarkFailed(key)
			wait := time.Duration(5+retry*5) * time.Second
			log.Printf("⏳ NVIDIA %d, retry %d/3 in %v...", resp.StatusCode, retry+1, wait)
			time.Sleep(wait)
			continue
		}
		// On 400, strip unsupported parameters and retry
		if resp.StatusCode == 400 && retry < 2 {
			bodyBytes, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if strings.Contains(string(bodyBytes), "Unsupported parameter") {
				log.Printf("⚠️ NVIDIA 400: stripping unsupported params, retry %d/3...", retry+1)
				delete(chatReq, "thinking")
				delete(chatReq, "reasoning_effort")
				reqBody, _ = json.Marshal(cleanNulls(chatReq))
				req, _ = http.NewRequestWithContext(ctx, "POST", nvidiaBase+"/chat/completions", bytes.NewReader(reqBody))
				req.Header.Set("Authorization", "Bearer "+key)
				req.Header.Set("Content-Type", "application/json")
				continue
			}
			return nil, fmt.Errorf("NVIDIA returned 400: %s", string(bodyBytes[:min(200, len(bodyBytes))]))
		}
		break
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("NVIDIA returned %d: %s", resp.StatusCode, string(body[:min(200, len(body))]))
	}

	// Convert Chat Completions response to Responses API format
	var chatResp struct {
		Choices []struct {
			Message struct {
				Content   string `json:"content"`
				ToolCalls []struct {
					ID       string `json:"id"`
					Function struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					} `json:"function"`
				} `json:"tool_calls"`
			} `json:"message"`
			FinishReason string `json:"finish_reason"`
		} `json:"choices"`
		Usage struct {
			PromptTokens     int `json:"prompt_tokens"`
			CompletionTokens int `json:"completion_tokens"`
			TotalTokens      int `json:"total_tokens"`
		} `json:"usage"`
	}

	json.Unmarshal(body, &chatResp)

	inputTokens := 0
	outputTokens := 0
	totalTokens := 0
	if chatResp.Usage.PromptTokens > 0 {
		inputTokens = chatResp.Usage.PromptTokens
	}
	if chatResp.Usage.CompletionTokens > 0 {
		outputTokens = chatResp.Usage.CompletionTokens
	}
	totalTokens = inputTokens + outputTokens

	choice := chatResp.Choices[0]
	output := []map[string]interface{}{}

	if len(choice.Message.ToolCalls) > 0 {
		for _, tc := range choice.Message.ToolCalls {
			output = append(output, map[string]interface{}{
				"type": "function_call", "id": tc.ID,
				"call_id": tc.ID, "name": tc.Function.Name,
				"arguments": tc.Function.Arguments, "status": "completed",
			})
		}
	}

	if choice.Message.Content != "" {
		output = append(output, map[string]interface{}{
			"type": "message",
			"id": fmt.Sprintf("msg_%x", time.Now().UnixNano()),
			"status": "completed", "role": "assistant",
			"content": []map[string]interface{}{{"type": "output_text", "text": choice.Message.Content}},
		})
	}

	status := "completed"
	if choice.FinishReason == "length" {
		status = "incomplete"
	}

	responseID := fmt.Sprintf("resp_%x", time.Now().UnixNano())
	result := map[string]interface{}{
		"id": responseID, "object": "response",
		"created_at": time.Now().Unix(), "status": status,
		"output": output,
		"usage": map[string]int{
			"input_tokens": inputTokens, "output_tokens": outputTokens,
			"total_tokens": totalTokens,
		},
	}

	return json.Marshal(result)
}

// ==================== HTTP 处理 ====================

func responsesHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "读取请求体失败", http.StatusBadRequest)
		return
	}
	r.Body.Close()

	var req struct {
		Model  string `json:"model"`
		Stream bool   `json:"stream"`
	}
	json.Unmarshal(body, &req)
	streaming := req.Stream
	model := req.Model
	if model == "" || !strings.Contains(model, "/") {
		// OpenAI model names (gpt-5.4, etc.) don't contain "/" — use default
		model = defaultModel
	}

	// Extract extra parameters to forward (thinking, etc.)
	extraParams := map[string]interface{}{}
	var rawReq map[string]interface{}
	json.Unmarshal(body, &rawReq)
	for _, key := range []string{"thinking", "reasoning_effort", "max_tokens", "top_p", "temperature", "frequency_penalty", "presence_penalty"} {
		if v, ok := rawReq[key]; ok {
			extraParams[key] = v
		}
	}

	// 速率限制（仅 key 池限制，不使用全局令牌桶）
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()

	// 转换请求
	messages, tools, _, err := responsesToChatMessages(body)
	if err != nil {
		http.Error(w, "请求格式转换失败: "+err.Error(), http.StatusBadRequest)
		return
	}
	log.Printf("[←Codex] raw_len=%d model=%s tools=%d msgs=%d", len(body), model, len(tools), len(messages))
	for i, m := range messages {
		role, _ := m["role"].(string)
		tc, _ := m["tool_calls"].([]map[string]interface{})
		if len(tc) > 0 {
			log.Printf("  msg[%d] role=%s tool_calls=%d", i, role, len(tc))
		} else {
			content, _ := m["content"].(string)
			log.Printf("  msg[%d] role=%s content=%s", i, role, content[:min(200, len(content))])
		}
	}

	usedKey := pool.Acquire()
	count := globalReqCount.Add(1)
	emoji := "✅"

	if !streaming {
		// 非流式
		respBody, err := callNVIDIA(ctx, usedKey, model, messages, tools, extraParams)
		if err != nil {
			log.Printf("%s [%d] #%d error: %v", emoji, 0, count, err)
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		log.Printf("%s [%d] #%d (Responses) %s model=%s key=...%s", emoji, 200, count, r.URL.Path, model, usedKey[len(usedKey)-6:])
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(respBody)
		return
	}

	// 流式
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	flusher, _ := w.(http.Flusher)
	log.Printf("%s [%d] #%d (Responses) %s model=%s key=...%s", emoji, 200, count, r.URL.Path, model, usedKey[len(usedKey)-6:])

	callNVIDIAStreaming(ctx, usedKey, model, messages, tools, extraParams, w, flusher)
}

func chatHandler(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "读取请求体失败", http.StatusBadRequest)
		return
	}
	r.Body.Close()

	// 检测是否流式
	var reqCheck struct {
		Stream bool `json:"stream"`
	}
	json.Unmarshal(body, &reqCheck)

	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Minute)
	defer cancel()

	usedKey := pool.Acquire()
	count := globalReqCount.Add(1)

	req, _ := http.NewRequestWithContext(ctx, "POST", nvidiaBase+"/chat/completions", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+usedKey)
	req.Header.Set("Content-Type", "application/json")

	var resp *http.Response
	// 重试逻辑（503 + 429）
	for retry := 0; retry < 3; retry++ {
		req.Body = io.NopCloser(bytes.NewReader(body))
		resp, err = sharedClient.Do(req)
		if err != nil {
			log.Printf("❌ [%d] NVIDIA error: %v", count, err)
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		if resp.StatusCode == 503 || resp.StatusCode == 429 {
			resp.Body.Close()
			pool.MarkFailed(usedKey)
			wait := time.Duration(5+retry*5) * time.Second
			log.Printf("⏳ [%d] NVIDIA %d, retry %d/3 in %v...", count, resp.StatusCode, retry+1, wait)
			// 换一个 key 重试
			usedKey = pool.Acquire()
			req, _ = http.NewRequestWithContext(ctx, "POST", nvidiaBase+"/chat/completions", bytes.NewReader(body))
			req.Header.Set("Authorization", "Bearer "+usedKey)
			req.Header.Set("Content-Type", "application/json")
			time.Sleep(wait)
			continue
		}
		break
	}
	defer resp.Body.Close()

	// 非流式：直接返回
	if !reqCheck.Stream {
		for k, vv := range resp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		emoji := "✅"
		if resp.StatusCode != 200 {
			emoji = "⚠️"
		}
		log.Printf("%s [%d] #%d (Chat) key=...%s", emoji, resp.StatusCode, count, usedKey[len(usedKey)-6:])
		return
	}

	// 流式：转发 SSE
	if resp.StatusCode != 200 {
		for k, vv := range resp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		io.Copy(w, resp.Body)
		log.Printf("⚠️ [%d] #%d (Chat-Stream) error", resp.StatusCode, count)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	flusher, _ := w.(http.Flusher)

	// 转发 SSE 数据
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Fprintf(w, "%s\n", line)
		if strings.HasPrefix(line, "data: ") && flusher != nil {
			flusher.Flush()
		}
	}

	log.Printf("✅ [%d] #%d (Chat-Stream) key=...%s", resp.StatusCode, count, usedKey[len(usedKey)-6:])
}

func modelsHandler(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	usedKey := pool.Acquire()
	req, _ := http.NewRequestWithContext(ctx, "GET", nvidiaBase+"/models", nil)
	req.Header.Set("Authorization", "Bearer "+usedKey)

	resp, err := sharedClient.Do(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
	log.Printf("✅ [%d] #%d (Models) %s", resp.StatusCode, globalReqCount.Add(1), r.URL.Path)
}

// ==================== 路由 ====================

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"uptime": time.Since(startTime).Round(time.Second).String(),
	})
}

func proxyHandler(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	if path == "/" && (r.Method == "GET" || r.Method == "HEAD") {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"service": "nvidia-proxy",
			"status":  "running",
			"endpoints": map[string]string{
				"responses": "http://localhost:9099/v1/responses",
				"chat":      "http://localhost:9099/v1/chat/completions",
				"models":    "http://localhost:9099/v1/models",
			},
		})
		return
	}

	if path == "/health" || path == "/healthz" {
		healthHandler(w, r)
		return
	}

	if path == "/stats" {
		total, available := pool.Stats()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"total_keys":     total,
			"available_keys": available,
			"global_rate":    globalRate,
			"rate_burst":     rateLimitBurst,
			"total_requests": globalReqCount.Load(),
			"uptime":         time.Since(startTime).Round(time.Second).String(),
		})
		return
	}

	if path == "/v1/responses" || path == "/responses" {
		responsesHandler(w, r)
		return
	}

	if strings.Contains(path, "chat/completions") {
		chatHandler(w, r)
		return
	}

	if path == "/v1/models" || path == "/models" {
		modelsHandler(w, r)
		return
	}

	http.NotFound(w, r)
}

// ==================== 主函数 ====================

func main() {
	cfgPath := "config.json"
	if len(os.Args) > 1 {
		cfgPath = os.Args[1]
	}

	data, err := os.ReadFile(cfgPath)
	if err != nil {
		log.Fatalf("读取 %s 失败: %v", cfgPath, err)
	}

	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		log.Fatalf("解析配置失败: %v", err)
	}
	if len(cfg.Keys) == 0 {
		log.Fatal("config.json 中没有 keys")
	}

	pool = NewKeyPool(cfg.Keys)
	rateLimiter = NewRateLimiter(globalRate, rateLimitBurst)
	startTime = time.Now()
	defaultModel = cfg.DefaultModel
	if defaultModel == "" {
		defaultModel = "z-ai/glm-5.1"
	}

	log.Printf("🔑 已加载 %d 个 API key", len(cfg.Keys))
	log.Printf("🎯 每 key 限 %d 次/分钟", maxPerMinute)
	log.Printf("🚦 全局速率限制: %.1f 次/秒 (%d 次/分钟)", globalRate, int(globalRate*60))
	fmt.Println()
	fmt.Printf("🚀 代理已启动: http://localhost%s\n", listenAddr)
	fmt.Printf("📡 转发目标: %s\n\n", nvidiaBase)
	fmt.Printf("  Responses API (Codex CLI): POST http://localhost%s/v1/responses\n", listenAddr)
	fmt.Printf("  Chat 备用:               POST http://localhost%s/v1/chat/completions\n", listenAddr)
	fmt.Printf("  Models:                   GET  http://localhost%s/v1/models\n", listenAddr)
	fmt.Println()

	http.HandleFunc("/", proxyHandler)
	log.Fatal(http.ListenAndServe(listenAddr, nil))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
