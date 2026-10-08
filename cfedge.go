package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

const cfAPIBase = "https://api.cloudflare.com/client/v4"

type cfAPIMsg struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type cfEnvelope[T any] struct {
	Success  bool       `json:"success"`
	Errors   []cfAPIMsg `json:"errors"`
	Messages []cfAPIMsg `json:"messages"`
	Result   T          `json:"result"`
}

type cfAPIError struct {
	StatusCode int
	Errors     []cfAPIMsg
}

func (e *cfAPIError) Error() string {
	return fmt.Sprintf("cloudflare api error (HTTP %d): %+v", e.StatusCode, e.Errors)
}

func (e *cfAPIError) IsPermissionOrPlan() bool {
	return e.StatusCode == http.StatusUnauthorized || e.StatusCode == http.StatusForbidden
}

type CloudflareClient struct {
	Token  string
	ZoneID string
	client *http.Client
}

func NewCloudflareClient(token, zoneID string) *CloudflareClient {
	return &CloudflareClient{
		Token:  token,
		ZoneID: zoneID,
		client: &http.Client{
			Timeout: 12 * time.Second,
		},
	}
}

func doCFRequest[T any](ctx context.Context, c *CloudflareClient, method, path string, body any) (T, error) {
	var zero T
	reqCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var reqBody io.Reader
	if body != nil {
		rawJSON, err := json.Marshal(body)
		if err != nil {
			return zero, fmt.Errorf("marshal payload: %w", err)
		}
		reqBody = bytes.NewReader(rawJSON)
	}

	req, err := http.NewRequestWithContext(reqCtx, method, cfAPIBase+path, reqBody)
	if err != nil {
		return zero, fmt.Errorf("build request: %w", err)
	}

	req.Header.Set("Authorization", "Bearer "+c.Token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "BERMUDA-EdgeOrchestrator/2.1 (Go-Standard-Library)")

	resp, err := c.client.Do(req)
	if err != nil {
		return zero, err
	}
	defer resp.Body.Close()

	rawBytes, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return zero, fmt.Errorf("read response body: %w", err)
	}

	var env cfEnvelope[T]
	if err := json.Unmarshal(rawBytes, &env); err != nil {
		return zero, &cfAPIError{
			StatusCode: resp.StatusCode,
			Errors: []cfAPIMsg{{
				Code:    resp.StatusCode,
				Message: string(rawBytes[:min(len(rawBytes), 256)]),
			}},
		}
	}

	if resp.StatusCode >= 400 || !env.Success {
		return zero, &cfAPIError{
			StatusCode: resp.StatusCode,
			Errors:     env.Errors,
		}
	}

	return env.Result, nil
}

type cfSettingResult struct {
	ID       string `json:"id"`
	Value    any    `json:"value"`
	Editable bool   `json:"editable"`
}

type cfSettingTarget struct {
	Name  string
	Path  string
	Value any
}

func (c *CloudflareClient) ensureSetting(ctx context.Context, target cfSettingTarget) {
	cur, err := doCFRequest[cfSettingResult](ctx, c, http.MethodGet, target.Path, nil)
	if err != nil {
		var apiErr *cfAPIError
		if errors.As(err, &apiErr) && apiErr.IsPermissionOrPlan() {
			log.Printf("[Cloudflare] Note: %s skipped (insufficient token permission or plan restriction)", target.Name)
			return
		}
		log.Printf("[Cloudflare] Warning: Read failed for %s: %v", target.Name, err)
		return
	}

	if fmt.Sprint(cur.Value) == fmt.Sprint(target.Value) {
		log.Printf("[Cloudflare] ✓ %s: ALREADY OPTIMAL (%v)", target.Name, cur.Value)
		return
	}

	if !cur.Editable {
		log.Printf("[Cloudflare] Note: %s is locked/read-only on this zone plan (current: %v)", target.Name, cur.Value)
		return
	}

	payload := map[string]any{"value": target.Value}
	_, err = doCFRequest[cfSettingResult](ctx, c, http.MethodPatch, target.Path, payload)
	if err != nil {
		var apiErr *cfAPIError
		if errors.As(err, &apiErr) && apiErr.IsPermissionOrPlan() {
			log.Printf("[Cloudflare] Note: Patch skipped for %s (insufficient permissions)", target.Name)
			return
		}
		log.Printf("[Cloudflare] Warning: Patch failed for %s: %v", target.Name, err)
		return
	}

	log.Printf("[Cloudflare] ✓ %s: ENFORCED -> %v", target.Name, target.Value)
}

type cfBotManagement struct {
	FightMode *bool `json:"fight_mode,omitempty"`
}

func (c *CloudflareClient) disableBotFightMode(ctx context.Context) {
	path := "/zones/" + c.ZoneID + "/bot_management"

	cur, err := doCFRequest[cfBotManagement](ctx, c, http.MethodGet, path, nil)
	if err == nil && cur.FightMode != nil && !*cur.FightMode {
		log.Println("[Cloudflare] ✓ Bot Fight Mode: ALREADY DISABLED")
		return
	}

	disableVal := false
	_, err = doCFRequest[cfBotManagement](ctx, c, http.MethodPut, path, cfBotManagement{FightMode: &disableVal})
	if err != nil {
		var apiErr *cfAPIError
		if errors.As(err, &apiErr) && apiErr.IsPermissionOrPlan() {
			log.Println("[Cloudflare] Note: Bot Fight Mode toggle skipped (token lacks 'Zone.Bot Management: Edit' permission)")
			return
		}
		log.Printf("[Cloudflare] Note: Bot Fight Mode update unapplied: %v", err)
		return
	}

	log.Println("[Cloudflare] ✓ Bot Fight Mode: DISABLED (Permit non-browser tunnels)")
}

type CFRule struct {
	ID               string         `json:"id,omitempty"`
	Ref              string         `json:"ref,omitempty"`
	Description      string         `json:"description,omitempty"`
	Expression       string         `json:"expression"`
	Action           string         `json:"action"`
	ActionParameters map[string]any `json:"action_parameters,omitempty"`
	Enabled          bool           `json:"enabled"`
}

type CFRuleset struct {
	ID    string   `json:"id,omitempty"`
	Rules []CFRule `json:"rules"`
}

func (c *CloudflareClient) upsertRule(ctx context.Context, phase string, want CFRule) error {
	basePath := "/zones/" + c.ZoneID + "/rulesets"
	entrypointPath := basePath + "/phases/" + phase + "/entrypoint"

	rs, err := doCFRequest[CFRuleset](ctx, c, http.MethodGet, entrypointPath, nil)
	var apiErr *cfAPIError
	if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
		_, err = doCFRequest[CFRuleset](ctx, c, http.MethodPut, entrypointPath, CFRuleset{
			Rules: []CFRule{want},
		})
		return err
	}
	if err != nil {
		return err
	}

	for _, rule := range rs.Rules {
		if rule.Ref != want.Ref {
			continue
		}

		a, _ := json.Marshal([]any{rule.Expression, rule.Action, rule.ActionParameters, rule.Enabled})
		b, _ := json.Marshal([]any{want.Expression, want.Action, want.ActionParameters, want.Enabled})
		if bytes.Equal(a, b) {
			log.Printf("[Cloudflare] ✓ Rule %q: ALREADY CONFIGURED (Idempotent match)", want.Ref)
			return nil
		}

		patchPath := fmt.Sprintf("%s/%s/rules/%s", basePath, rs.ID, rule.ID)
		_, err = doCFRequest[CFRule](ctx, c, http.MethodPatch, patchPath, want)
		if err == nil {
			log.Printf("[Cloudflare] ✓ Rule %q: UPDATED IN-PLACE", want.Ref)
		}
		return err
	}

	postPath := fmt.Sprintf("%s/%s/rules", basePath, rs.ID)
	_, err = doCFRequest[CFRule](ctx, c, http.MethodPost, postPath, want)
	if err == nil {
		log.Printf("[Cloudflare] ✓ Rule %q: CREATED & ACTIVE", want.Ref)
	}
	return err
}

func (c *CloudflareClient) Tune(parent context.Context) {
	log.Printf("[Cloudflare] Commencing Enterprise Edge Tuning for Zone %s...", c.ZoneID)
	ctx, cancel := context.WithTimeout(parent, 45*time.Second)
	defer cancel()

	standardSettings := []cfSettingTarget{
		{
			Name:  "0-RTT Connection Resumption (Zero-Lag Reconnect)",
			Path:  "/zones/" + c.ZoneID + "/settings/0rtt",
			Value: "on",
		},
		{
			Name:  "WebSockets L7 Support (Persistent Duplex Tunnels)",
			Path:  "/zones/" + c.ZoneID + "/settings/websockets",
			Value: "on",
		},
		{
			Name:  "HTTP/2 to Origin (Stream Multiplexing Acceleration)",
			Path:  "/zones/" + c.ZoneID + "/settings/http2",
			Value: "on",
		},
		{
			Name:  "HTTP/3 QUIC (Line-Rate UDP Protocol)",
			Path:  "/zones/" + c.ZoneID + "/settings/http3",
			Value: "on",
		},
		{
			Name:  "Minimum TLS Version 1.3 (Modern Cipher Enforcement)",
			Path:  "/zones/" + c.ZoneID + "/settings/min_tls_version",
			Value: "1.3",
		},
		{
			Name:  "Security Level (WAF Challenge Mitigation)",
			Path:  "/zones/" + c.ZoneID + "/settings/security_level",
			Value: "essentially_off",
		},
		{
			Name:  "Brotli Compression (Fast Compression Pipeline)",
			Path:  "/zones/" + c.ZoneID + "/settings/brotli",
			Value: "on",
		},
		{
			Name:  "Early Hints (RFC 8297 Preload Acceleration)",
			Path:  "/zones/" + c.ZoneID + "/settings/early_hints",
			Value: "on",
		},
		{
			Name:  "Smart Tiered Caching (Internal Backbone Fiber Routing)",
			Path:  "/zones/" + c.ZoneID + "/argo/tiered_caching",
			Value: "on",
		},
	}

	for _, setting := range standardSettings {
		if ctx.Err() != nil {
			log.Println("[Cloudflare] Edge tuning context cancelled/timed out")
			return
		}
		c.ensureSetting(ctx, setting)
	}

	if ctx.Err() == nil {
		c.disableBotFightMode(ctx)
	}

	if ctx.Err() == nil {
		cacheBypassRule := CFRule{
			Ref:         "stealth-gw-cache-bypass",
			Description: "BERMUDA Tunnel Stream Zero-Latency Cache Bypass",
			Expression:  `starts_with(http.request.uri.path, "/api/v1/")`,
			Action:      "set_cache_settings",
			ActionParameters: map[string]any{
				"cache": false,
			},
			Enabled: true,
		}

		if err := c.upsertRule(ctx, "http_request_cache_settings", cacheBypassRule); err != nil {
			var apiErr *cfAPIError
			if errors.As(err, &apiErr) && apiErr.IsPermissionOrPlan() {
				log.Println("[Cloudflare] Note: Cache rule skipped (token lacks 'Zone.Rulesets: Edit' permission)")
			} else {
				log.Printf("[Cloudflare] Warning: Cache rule sync: %v", err)
			}
		}
	}

	log.Printf("[Cloudflare] Edge Tuning sequence concluded for Zone %s", c.ZoneID)
}
