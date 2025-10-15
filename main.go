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

	pubsub "cloud.google.com/go/pubsub"
)

type Envelope struct {
	RowID      *int64 `json:"row_id,omitempty"`
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
	psClient  *pubsub.Client
	psTopic   *pubsub.Topic
)

func main() {
	if err := db.Connect(); err != nil {
		log.Fatalf("db connect: %v", err)
	}
	defer db.Pool.Close()

	projectID := os.Getenv("PROJECT_ID")         // or "GOOGLE_CLOUD_PROJECT"
	topicID := os.Getenv("PUBSUB_TOPIC_GW_SELF") // e.g. "gateway-self.parsed"

	if projectID == "" || topicID == "" {
		log.Fatal("missing GCP_PROJECT or PUBSUB_TOPIC_GW_SELF")
	}

	var err error
	psClient, err = pubsub.NewClient(context.Background(), projectID)
	if err != nil {
		log.Fatalf("pubsub.NewClient: %v", err)
	}
	psTopic = psClient.Topic(topicID)
	// optional: enable ordering if you created the topic with ordering enabled
	// psTopic.EnableMessageOrdering = true

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

	// --- Auth (optional) ---
	if authToken != "" {
		tok := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if tok == "" || tok != authToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	// --- Idempotency key required ---
	idemKey := r.Header.Get("X-Idempotency-Key")
	if strings.TrimSpace(idemKey) == "" {
		log.Printf("400 missing idempotency; headers=%v", r.Header)
		http.Error(w, "missing idempotency", http.StatusBadRequest)
		return
	}
	dup, err := receiptsSeen(r.Context(), idemKey)
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

	// --- Parse body ---
	var env Envelope
	if err := json.NewDecoder(r.Body).Decode(&env); err != nil {
		log.Printf("400 bad json: %v", err)
		http.Error(w, "bad json", http.StatusBadRequest)
		return
	}
	env.GWHW = strings.ToUpper(strings.TrimSpace(env.GWHW))
	env.GWMAC = strings.ToUpper(strings.TrimSpace(env.GWMAC))

	if env.GWHW == "" || env.GWMAC == "" || env.PayloadHex == "" {
		log.Printf("400 missing fields: gw_hw=%q gw_mac=%q payload_hex_len=%d", env.GWHW, env.GWMAC, len(env.PayloadHex))
		http.Error(w, "missing fields (gw_hw, gw_mac, payload_hex)", http.StatusBadRequest)
		return
	}
	if len(env.GWMAC) != 12 {
		http.Error(w, "bad gw_mac (expect 12 hex chars, no separators)", http.StatusBadRequest)
		return
	}

	// --- Normalize / parse per gateway type ---
	ts := time.UnixMilli(env.DeviceTsMs) // may be zero -> 1970-01-01
	flagToStore := strings.TrimSpace(env.Flag)
	payloadToStore := env.PayloadHex

	var st *storage.AutoStatus
	var fx *storage.AutoFix

	switch env.GWHW {
	case "MKGW4":
		// We receive TLV BODY (no EF30 header). Use the provided flag.
		// Extract bare flag from "self/3004" -> "3004"
		bare := strings.ToUpper(strings.TrimSpace(strings.TrimPrefix(strings.ToLower(flagToStore), "self/")))
		if looksLikeHex(env.PayloadHex) && (bare == "3004" || bare == "3089" || bare == "30B1") {
			auto, ok, decErr := DecodeMKGW4Auto(bare, env.PayloadHex)
			// Debug head (trim to keep logs readable)
			if decErr != nil {
				log.Printf("decode warn (MKGW4 body): %v", decErr)
			}
			if ok && auto != nil {
				if flagToStore == "" {
					flagToStore = "self/" + strings.ToUpper(auto.Flag)
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
				// Unknown/other MKGW4 body → store as-is
				payloadToStore = strings.ToUpper(env.PayloadHex)
			}
		} else {
			// Non-hex / unsupported flag: store as JSON flavor
			if flagToStore == "" {
				flagToStore = "json"
			}
			payloadToStore = env.PayloadHex
		}

	default:
		// JSON gateways (MKGW3/MKGW1BWPRO/MINI...). Store JSON body as-is.
		if flagToStore == "" {
			flagToStore = "json"
		}
		payloadToStore = env.PayloadHex
	}

	// --- Update parsed view back into public.gateway_message if RowID present ---
	if env.RowID != nil && *env.RowID > 0 {
		parsed := map[string]any{
			"gw_hw":        env.GWHW,
			"gw_mac":       env.GWMAC,
			"flag":         flagToStore,
			"topic":        env.Topic,
			"device_ts":    ts.UTC().Format(time.RFC3339Nano),
			"device_ts_ms": ts.UnixMilli(),
			"source":       "ble-gw-auto-parser",
			"kind":         "gateway_self",
			"version":      1,
		}
		if st != nil {
			parsed["status"] = map[string]any{
				"network_type": st.NetworkType,
				"csq":          st.CSQ,
				"batt_mv":      st.BattmV,
				"axis_x_mg":    st.AxisXmg,
				"axis_y_mg":    st.AxisYmg,
				"axis_z_mg":    st.AxisZmg,
				"acc_status":   st.AccStatus,
				"imei":         st.IMEI,
				"iccid":        st.ICCID,
			}
		}
		if fx != nil {
			parsed["fix"] = map[string]any{
				"mode":    fx.FixMode,
				"result":  fx.FixResult,
				"lon":     fx.Longitude,
				"lat":     fx.Latitude,
				"tac_lac": fx.TacLac,
				"ci":      fx.CI,
			}
		}

		parserName := "gw_json:auto"
		if env.GWHW == "MKGW4" {
			parserName = "mkgw4:auto"
		}
		if err := store.UpdateGatewayParsedAndDenormByID(r.Context(), *env.RowID, parserName, parsed, st, fx); err != nil {
			// Not fatal; continue to publish.
			log.Printf("UpdateGatewayParsedAndDenormByID err (id=%d): %v", *env.RowID, err)
		}
	}

	// --- Publish parsed message to Pub/Sub (optional) ---
	if psTopic != nil {
		out := map[string]any{
			"type":          "gateway_self",
			"gw_hw":         env.GWHW,
			"gw_mac":        env.GWMAC,
			"flag":          flagToStore,
			"topic":         env.Topic,
			"device_ts_ms":  ts.UnixMilli(),
			"payload":       payloadToStore, // MKGW4: hex body; JSON GWs: JSON string
			"row_id":        env.RowID,      // may be nil
			"parsed_status": st,             // may be nil
			"parsed_fix":    fx,             // may be nil
		}
		b, _ := json.Marshal(out)

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		res := psTopic.Publish(ctx, &pubsub.Message{
			Data:       b,
			Attributes: map[string]string{"source": "ble-gw-auto-parser"},
		})
		if _, err := res.Get(ctx); err != nil {
			log.Printf("pubsub publish error: %v", err)
		}
	}

	// Done
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))

	log.Printf(`{"event":"stored+published","gw_hw":"%s","flag":"%s","len":%d,"row_id":%v,"took_ms":%d}`,
		env.GWHW, flagToStore, len(payloadToStore), env.RowID != nil, time.Since(start).Milliseconds())
}

// ---------- helpers ----------

func buildParsedJSON(env Envelope, decoded *Auto, jsonBody any) map[string]any {
	out := map[string]any{
		"gw_hw":        env.GWHW,
		"gw_mac":       env.GWMAC,
		"flag":         env.Flag,
		"topic":        env.Topic,
		"device_ts_ms": env.DeviceTsMs,
		"kind":         "gateway_self",
		"source":       "ble-gw-auto-parser",
		"version":      1,
	}
	switch {
	case decoded != nil:
		out["codec"] = "tlv:mkgw4"
		out["frame_flag"] = decoded.Flag
		if decoded.Status != nil {
			out["status"] = map[string]any{
				"network_type": decoded.Status.NetworkType,
				"csq":          decoded.Status.CSQ,
				"batt_mv":      decoded.Status.BattmV,
				"axis_x_mg":    decoded.Status.AxisXmg,
				"axis_y_mg":    decoded.Status.AxisYmg,
				"axis_z_mg":    decoded.Status.AxisZmg,
				"acc_status":   decoded.Status.AccStatus,
				"imei":         decoded.Status.IMEI,
				"iccid":        decoded.Status.ICCID,
			}
		}
		if decoded.Fix != nil {
			out["fix"] = map[string]any{
				"mode":      decoded.Fix.FixMode,
				"result":    decoded.Fix.FixResult,
				"longitude": decoded.Fix.Longitude,
				"latitude":  decoded.Fix.Latitude,
				"tac_lac":   decoded.Fix.TacLac,
				"ci":        decoded.Fix.CI,
			}
		}
		if decoded.Timestamp != 0 {
			out["frame_ts_s"] = decoded.Timestamp
		}

	case jsonBody != nil:
		out["codec"] = "json"
		out["body"] = jsonBody
	}
	return out
}

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

func publishParsed(ctx context.Context, parsed map[string]any) {
	if psTopic == nil {
		return
	}
	b, _ := json.Marshal(parsed)
	res := psTopic.Publish(ctx, &pubsub.Message{
		Data: b,
		Attributes: map[string]string{
			"gw_hw": fmt.Sprint(parsed["gw_hw"]),
			"flag":  fmt.Sprint(parsed["flag"]),
		},
	})
	if _, err := res.Get(ctx); err != nil {
		log.Printf("pubsub publish error: %v", err)
	}
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
