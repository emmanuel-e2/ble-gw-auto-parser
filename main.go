package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"ble-gw-auto-parser/db"
	"ble-gw-auto-parser/storage"
)

type Envelope struct {
	GWHW       string `json:"gw_hw"`  // "MKGW4" | "MKGW3" | "MKGW1BWPRO" | "MKGWMINI01" | ...
	GWMAC      string `json:"gw_mac"` // uppercase hex (12 chars, no separators)
	Topic      string `json:"topic"`
	Flag       string `json:"flag"`         // e.g. "self/30A0", "scan_incomplete/30A0", "msg/3004"
	DeviceTsMs int64  `json:"device_ts_ms"` // may be 0
	PayloadHex string `json:"payload_hex"`  // MKGW4: EF30.. hex; JSON gateways: minified JSON string
}

var (
	authToken string
	store     *storage.Store
)

func main() {
	if err := db.Connect(); err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer db.Pool.Close()

	authToken = os.Getenv("GWAUTO_AUTH_TOKEN")
	store = storage.New()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/auto", handleAuto)

	addr := ":8080"
	if v := os.Getenv("PORT"); v != "" {
		addr = ":" + v
	}
	log.Printf("gw-auto-parser listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func handleAuto(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// Auth
	if authToken != "" {
		t := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if t == "" || t != authToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	// Idempotency
	idem := r.Header.Get("X-Idempotency-Key")
	if idem == "" {
		http.Error(w, "missing idempotency", http.StatusBadRequest)
		return
	}
	dup, err := receiptsSeen(r.Context(), idem)
	if err != nil {
		log.Printf("idempotency check error: %v", err)
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}
	if dup {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true,"dup":true}`))
		return
	}

	// Body
	var env Envelope
	if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	if env.GWHW == "" || env.GWMAC == "" || env.PayloadHex == "" {
		http.Error(w, "missing fields", http.StatusBadRequest)
		return
	}
	gwMacBytes, err := ParseMAC12(strings.ToUpper(env.GWMAC))
	if err != nil {
		http.Error(w, "bad gw_mac", http.StatusBadRequest)
		return
	}
	ts := time.UnixMilli(env.DeviceTsMs) // may be 0 => 1970-01-01

	var (
		flagToStore    = env.Flag
		payloadToStore = env.PayloadHex
		st             *storage.AutoStatus
		fx             *storage.AutoFix
	)

	switch strings.ToUpper(env.GWHW) {
	case "MKGW4":
		// Try to decode TLV frames (only for known flags 3004/3089/30B1).
		if looksLikeHex(env.PayloadHex) {
			auto, ok, decErr := DecodeMKGW4Auto([]byte(env.PayloadHex))
			if decErr != nil {
				// Don’t fail: store raw
				log.Printf("decode warn (MKGW4): %v", decErr)
			}
			if ok && auto != nil {
				// prefer upstream flag if provided (it may be "self/…")
				if flagToStore == "" {
					flagToStore = strings.ToUpper(auto.Flag)
				}
				payloadToStore = strings.ToUpper(auto.Hex)
				if auto.Timestamp != 0 && env.DeviceTsMs == 0 {
					ts = time.Unix(auto.Timestamp, 0)
				}
				if auto.Status != nil {
					st = &storage.AutoStatus{
						NetworkType: auto.Status.NetworkType,
						CSQ:         auto.Status.CSQ,
						BattmV:      auto.Status.BattmV,
						AxisXmg:     auto.Status.AxisXmg,
						AxisYmg:     auto.Status.AxisYmg,
						AxisZmg:     auto.Status.AxisZmg,
						AccStatus:   auto.Status.AccStatus,
						IMEI:        auto.Status.IMEI,
						ICCID:       auto.Status.ICCID,
					}
				}
				if auto.Fix != nil {
					fx = &storage.AutoFix{
						FixMode:   auto.Fix.FixMode,
						FixResult: auto.Fix.FixResult,
						Longitude: auto.Fix.Longitude,
						Latitude:  auto.Fix.Latitude,
						TacLac:    auto.Fix.TacLac,
						CI:        auto.Fix.CI,
					}
				}
			} else {
				// unknown/other MKGW4 autos: store as-is, keep env.Flag
				if flagToStore == "" {
					flagToStore = deriveHeaderFlag(env.PayloadHex) // e.g., "30A0"
				}
				payloadToStore = strings.ToUpper(env.PayloadHex)
			}
		} else {
			// Unexpected (JSON) — store as-is
			if flagToStore == "" {
				flagToStore = "json"
			}
		}

	default:
		// JSON gateways (MKGW3/MKGW1BWPRO/MKGWMINI01…)
		// payload is a minified JSON string; just store it (st/fx nil).
		if flagToStore == "" {
			flagToStore = "json"
		}
	}

	// Persist
	if err := store.UpsertAuto(
		r.Context(),
		env.GWHW,
		gwMacBytes,
		env.Topic,
		flagToStore,
		payloadToStore,
		ts,
		st,
		fx,
	); err != nil {
		log.Printf("UpsertAuto error: %v", err)
		http.Error(w, "server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	log.Printf(`{"event":"stored","gw":"%s","flag":"%s","len":%d,"ts_ms":%d,"took_ms":%d}`,
		env.GWMAC, flagToStore, len(payloadToStore), env.DeviceTsMs, time.Since(start).Milliseconds())
}

// ---------- helpers ----------

func looksLikeHex(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= '0' && c <= '9') || (c >= 'A' && c <= 'F') {
			continue
		}
		if c == ' ' || c == ':' || c == '-' || c == '.' { // tolerate separators
			continue
		}
		if c >= 'a' && c <= 'f' {
			continue
		}
		return false
	}
	return true
}

// deriveHeaderFlag returns bytes[1..2] as hex (upper) from an EF30... frame in ASCII hex.
func deriveHeaderFlag(hexStr string) string {
	// strip separators
	clean := strings.NewReplacer(" ", "", ":", "", "-", "", ".", "").Replace(hexStr)
	clean = strings.ToUpper(clean)
	if len(clean) < 6 || clean[:2] != "EF" {
		return ""
	}
	return clean[2:6]
}

func receiptsSeen(ctx context.Context, key string) (dup bool, err error) {
	tag, err := db.Pool.Exec(ctx, `
		INSERT INTO gw_auto_receipts (idempotency_key) VALUES ($1)
		ON CONFLICT (idempotency_key) DO NOTHING
	`, key)
	if err != nil {
		return false, err
	}
	// If nothing was inserted, it was a duplicate.
	return tag.RowsAffected() == 0, nil
}

func receiptsInsert(ctx context.Context, key string) error {
	_, err := db.Pool.Exec(ctx, `
		INSERT INTO gw_auto_receipts (idempotency_key) VALUES ($1)
		ON CONFLICT (idempotency_key) DO NOTHING
	`, key)
	return err
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		panic(fmt.Sprintf("missing env %s", k))
	}
	return v
}
