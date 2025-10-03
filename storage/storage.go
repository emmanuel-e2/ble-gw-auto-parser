package storage

import (
	"context"
	"time"

	"ble-gw-auto-parser/db"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func New() *Store {
	return &Store{pool: db.Pool} // uses the global Pool from db.Connect()
}

func (s *Store) UpsertAuto(
	ctx context.Context,
	gwHW string,
	gwMAC []byte,
	topic string,
	flag string,
	payloadHex string,
	deviceTS time.Time,
	st *AutoStatus, // from parser.go (redeclared here via type alias)
	fx *AutoFix,
) error {
	_, err := s.pool.Exec(ctx, `
INSERT INTO gateway_auto_message
(gw_hw, gw_mac, topic, flag, ts_device, payload_hex,
 network_type, csq, batt_mv, axis_x_mg, axis_y_mg, axis_z_mg, acc_status, imei, iccid,
 longitude, latitude, tac_lac, ci)
VALUES
($1,$2,$3,$4,$5,$6,
 $7,$8,$9,$10,$11,$12,$13,$14,
 $15,$16,$17,$18,$19)
ON CONFLICT (gw_mac, flag, ts_device, payload_hex) DO NOTHING
`,
		gwHW, gwMAC, topic, flag, deviceTS, payloadHex,
		nullText(st, func() string { return st.NetworkType }),
		nullInt(st, func() int { return st.CSQ }),
		nullInt(st, func() int { return st.BattmV }),
		nullInt(st, func() int { return st.AxisXmg }),
		nullInt(st, func() int { return st.AxisYmg }),
		nullInt(st, func() int { return st.AxisZmg }),
		nullInt(st, func() int { return st.AccStatus }),
		nullText(st, func() string { return st.IMEI }),
		nullText(st, func() string { return st.ICCID }),
		nullFloat(fx, func() float64 { return fx.Longitude }),
		nullFloat(fx, func() float64 { return fx.Latitude }),
		nullInt(fx, func() int { return fx.TacLac }),
		nullInt64(fx, func() int64 { return fx.CI }),
	)
	return err
}

func nullText[T any](ptr *T, f func() string) any {
	if ptr == nil {
		return nil
	}
	return f()
}
func nullInt[T any](ptr *T, f func() int) any {
	if ptr == nil {
		return nil
	}
	return f()
}
func nullInt64[T any](ptr *T, f func() int64) any {
	if ptr == nil {
		return nil
	}
	return f()
}
func nullFloat[T any](ptr *T, f func() float64) any {
	if ptr == nil {
		return nil
	}
	return f()
}

// Type aliases to reuse parser types without import cycles (storage ↔ parser):
type AutoStatus = struct {
	NetworkType string
	CSQ         int
	BattmV      int
	AxisXmg     int
	AxisYmg     int
	AxisZmg     int
	AccStatus   int
	IMEI        string
	ICCID       string
}
type AutoFix = struct {
	FixMode   string
	FixResult string
	Longitude float64
	Latitude  float64
	TacLac    int
	CI        int64
}
