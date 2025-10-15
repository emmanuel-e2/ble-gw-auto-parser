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

// Update parsed JSON AND denormalized columns into the SAME row.
func (s *Store) UpdateGatewayParsedAndDenormByID(
	ctx context.Context,
	id int64,
	parser string,
	parsed any,
	st *AutoStatus,
	fx *AutoFix,
) error {
	b, _ := json.Marshal(parsed)

	// Precompute all nullable params as `any` so nil stays nil and COALESCE works.
	var (
		networkType any
		csq         any
		battmv      any
		ax          any
		ay          any
		az          any
		acc         any
		imei        any
		iccid       any

		longitude any
		latitude  any
		taclac    any
		ci        any
	)

	if st != nil {
		networkType = st.NetworkType
		csq = st.CSQ
		battmv = st.BattmV
		ax = st.AxisXmg
		ay = st.AxisYmg
		az = st.AxisZmg
		acc = st.AccStatus
		imei = st.IMEI
		iccid = st.ICCID
	}
	if fx != nil {
		longitude = fx.Longitude
		latitude = fx.Latitude
		taclac = fx.TacLac
		ci = fx.CI
	}

	ct, err := s.pool.Exec(ctx, `
		UPDATE public.gateway_message
		SET
			parser 			= $2,
			parser_json 	= $3,
			network_type 	= COALESCE($4, network_type),
			csq 			= COALESCE($5, csq),
			batt_mv 		= COALESCE($6, batt_mv),
			axis_x_mg 		= COALESCE($7, axis_x_mg),
			axis_y_mg 		= COALESCE($8, axis_y_mg),
			axis_z_mg 		= COALESCE($9, axis_z_mg),
			acc_status		= COALESCE($10, acc_status),
			imei			= COALESCE($11, imei),
			iccid 			= COALESCE($12, iccid),
			longitude		= COALESCE($13, longitude),
			latitude		= COALESCE($14, latitude),
			tac				= COALESCE($15, tac),
			cell_id			= COALESCE($16, cell_id)
		WHERE id = $1
		`, id, parser, json.RawMessage(b),
		networkType, csq, battmv, ax, ay, az, acc, imei, iccid,
		longitude, latitude, taclac, ci,
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
