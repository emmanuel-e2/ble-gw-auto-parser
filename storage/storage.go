package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"ble-gw-auto-parser/db"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func New() *Store {
	return &Store{pool: db.Pool} // uses the global Pool from db.Connect()
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

// Update parsed JSON AND denormalized columns into the SAME row.
func (s *Store) UpdateGatewayParsedAndDenormByID(
	ctx context.Context,
	id int64,
	parser string,
	parsed any,
	deviceTs time.Time,
	st *AutoStatus,
	fx *AutoFix,
) error {
	b, _ := json.Marshal(parsed)

	// Precompute all nullable params as `any` so nil stays nil and COALESCE works.
	// Build nullable values
	var (
		lat, lon *float64
		tac, csq *int
		ci       *int64
	)
	var netType, imei, iccid *string
	var batt, ax, ay, az, acc *int

	if fx != nil {
		lon = &fx.Longitude
		lat = &fx.Latitude
		tac = &fx.TacLac
		ci = &fx.CI
	}

	if st != nil {
		if st.NetworkType != "" {
			netType = &st.NetworkType
		}
		if st.IMEI != "" {
			imei = &st.IMEI
		}
		if st.ICCID != "" {
			imei = &st.ICCID
		}
		c := st.CSQ
		b := st.BattmV
		x := st.AxisXmg
		y := st.AxisYmg
		z := st.AxisZmg
		a := st.AccStatus

		csq = &c
		batt = &b
		ax = &x
		ay = &y
		az = &z
		acc = &a
	}

	// We update ts_device to decoded deviceTs if not zero.
	var tsDev *time.Time
	if !deviceTs.IsZero() {
		tmp := deviceTs.UTC()
		tsDev = &tmp
	}

	// Note: use COALESCE to allow nulls; we set explicitely whatever we have now.
	ct, err := s.pool.Exec(ctx, `
		UPDATE public.gateway_message
		SET 
			parser			= $2,
			parser_json		= $3,
			ts_device		= COALESCE($4, ts_device),
			latitude		= $5,
			longitude		= $6,
			tac				= $7,
			cell_id			= $8,
			network_type	= $9,
			csq				= $10,
			batt_mv			= $11,
			axis_x_mg		= $12,
			axis_y_mg		= $13,
			axis_z_mg		= $14,
			acc_status		= $15,
			imei			= $16,
			iccid			= $17
		WHERE id = $1
	`, id, parser, json.RawMessage(b),
		tsDev,
		lat, lon, tac, ci,
		netType, csq, batt, ax, ay, az, acc, imei, iccid,
	)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}

	return nil

}

func (s *Store) UpdateGatewayParsedColumnsByID(
	ctx context.Context, id int64, st *AutoStatus, fx *AutoFix,
) error {
	if st != nil {
		if _, err := s.pool.Exec(ctx, `
            UPDATE public.gateway_message
            SET network_type = $2,
                csq          = $3
            WHERE id = $1
        `, id, st.NetworkType, st.CSQ); err != nil {
			return err
		}
	}
	if fx != nil {
		if _, err := s.pool.Exec(ctx, `
            UPDATE public.gateway_message
            SET longitude = $2,
                latitude  = $3,
                tac       = $4,
                cell_id   = $5
            WHERE id = $1
        `, id, fx.Longitude, fx.Latitude, fx.TacLac, fx.CI); err != nil {
			return err
		}
	}
	return nil
}

// Update parser output into the SAME row in public.gateway_message
func (s *Store) UpdateGatewayParsedByID(ctx context.Context, id int64, parser string, parsed any) error {
	b, _ := json.Marshal(parsed)
	ct, err := s.pool.Exec(ctx, `
        UPDATE public.gateway_message
        SET parser = $2,
            parser_json = $3
        WHERE id = $1
    `, id, parser, json.RawMessage(b))
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}

// Fallback lookup when RowID was not provided (avoid if possible).
// Tries (gw_mac, ts_device, payload_hex) and, for JSON self-frames, raw_json->>'payload_hex'
func (s *Store) FindGatewayRowID(ctx context.Context, gwMAC []byte, ts time.Time, payloadHex string) (int64, error) {
	var id int64
	// 1) direct (gw_mac, ts_device, payload_hex)
	err := s.pool.QueryRow(ctx, `
        SELECT id
        FROM public.gateway_message
        WHERE gw_mac = $1 AND ts_device = $2 AND payload_hex = $3
        ORDER BY id DESC
        LIMIT 1
    `, gwMAC, ts, payloadHex).Scan(&id)
	if err == nil {
		return id, nil
	}

	// 2) JSON case: payload_hex column may be empty, but env is in raw_json
	// Try to match by the original JSON string (minified) stored inside raw_json.
	if payloadHex != "" && (strings.HasPrefix(payloadHex, "{") || strings.HasPrefix(payloadHex, "[")) {
		err2 := s.pool.QueryRow(ctx, `
            SELECT id
            FROM public.gateway_message
            WHERE gw_mac = $1 AND ts_device = $2
              AND (payload_hex = '' OR payload_hex IS NULL)
              AND raw_json->>'payload_hex' = $3
            ORDER BY id DESC
            LIMIT 1
        `, gwMAC, ts, payloadHex).Scan(&id)
		if err2 == nil {
			return id, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) && !errors.Is(err2, pgx.ErrNoRows) {
			return 0, fmt.Errorf("find id: %w; %v", err, err2)
		}
	}
	return 0, pgx.ErrNoRows
}
