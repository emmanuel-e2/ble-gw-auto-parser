package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
)

// Auto is the parsed representation of MKGW4 gateway auto frames.
type Auto struct {
	Flag      string      // "3004", "3089", "30b1"
	Timestamp int64       // seconds (from frame)
	Hex       string      // full frame hex (uppercase)
	Status    *AutoStatus // only for 3004
	Fix       *AutoFix    // only for 3089/30b1
}

type AutoStatus struct {
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

type AutoFix struct {
	FixMode   string
	FixResult string
	Longitude float64
	Latitude  float64
	TacLac    int
	CI        int64
}

// DecodeMKGW4Auto accepts either ASCII-hex or raw bytes (we get hex).
func DecodeMKGW4Auto(flagHex string, bodyHex string) (*Auto, bool, error) {
	flag := strings.ToUpper(strings.TrimSpace(flagHex))

	h := strings.ToUpper(strings.TrimSpace(string(bodyHex)))
	h = strings.NewReplacer(" ", "", ":", "", "-", "", ".", "").Replace(h)

	b, err := hex.DecodeString(h)
	if err != nil {
		return nil, false, fmt.Errorf("hex decode: %w", err)
	}

	a := &Auto{Flag: strings.ToLower(flag), Hex: h}

	switch flag {
	case "3004":
		log.Printf("In case 3004")
		st, ts, err := parseStatusTLV(b)
		if err != nil {
			return nil, true, err
		}
		a.Status = st
		if ts == 0 {
			ts = time.Now().Unix()
		}
		a.Timestamp = ts
		return a, true, nil
	case "3089", "30B1":
		log.Printf("In case 3089/30B1")
		fx, ts, err := parseFixTLV(b)
		if err != nil {
			return nil, true, err
		}
		a.Fix = fx
		if ts == 0 {
			ts = time.Now().Unix()
		}
		a.Timestamp = ts
		return a, true, nil
	default:
		return nil, false, nil
	}
}

func parseStatusTLV(body []byte) (*AutoStatus, int64, error) {
	st := &AutoStatus{}
	var ts int64
	i := 0
	for i < len(body) {
		tag := body[i]
		i++
		if i+2 > len(body) {
			return nil, 0, errors.New("status tlv len OOB")
		}
		ln := be16(body[i:])
		i += 2
		if i+ln > len(body) {
			return nil, 0, errors.New("status tlv OOB")
		}
		switch tag {
		case 0x00: // timestamp (4B)
			if ln >= 4 {
				ts = be32(body[i : i+4])
			}
		case 0x01: // network type (ASCII)
			st.NetworkType = string(body[i : i+ln])
		case 0x02: // csq
			if ln >= 1 {
				st.CSQ = int(body[i])
			}
		case 0x03: // batt mV (2B)
			if ln >= 2 {
				st.BattmV = be16(body[i : i+2])
			}
		case 0x04: // axis x/y/z
			if ln >= 3 {
				st.AxisXmg = int(body[i+0])
				st.AxisYmg = int(body[i+1])
				st.AxisZmg = int(body[i+2])
			}
		case 0x05: // acc status
			if ln >= 1 {
				st.AccStatus = int(body[i])
			}
		case 0x06: // IMEI (ASCII)
			st.IMEI = string(body[i : i+ln])
		}
		i += ln
	}
	return st, ts, nil
}

var fixModeNames = []string{"Periodic", "Motion", "Downlink"}
var fixResultNames = []string{
	"GPS fix success", "LBS fix success", "Interrupted by Downlink",
	"GPS serial port is used", "GPS aiding timeout", "GPS timeout", "PDOP limit", "LBS failure",
}

func parseFixTLV(body []byte) (*AutoFix, int64, error) {
	f := &AutoFix{}
	var ts int64
	i := 0
	for i < len(body) {
		tag := body[i]
		i++
		if i+2 > len(body) {
			return nil, 0, errors.New("fix tlv len OOB")
		}
		ln := be16(body[i:])
		i += 2
		if i+ln > len(body) {
			return nil, 0, errors.New("fix tlv OOB")
		}
		switch tag {
		case 0x00: // timestamp
			if ln >= 4 {
				ts = be32(body[i : i+4])
			}
		case 0x01: // fix mode
			if ln >= 1 {
				idx := int(body[i])
				if idx >= 0 && idx < len(fixModeNames) {
					f.FixMode = fixModeNames[idx]
				}
			}
		case 0x02: // fix result
			if ln >= 1 {
				idx := int(body[i])
				if idx >= 0 && idx < len(fixResultNames) {
					f.FixResult = fixResultNames[idx]
				}
			}
		case 0x03: // lon/lat (int32 each, * 1e-7)
			if ln >= 8 {
				lon := int32(body[i])<<24 | int32(body[i+1])<<16 | int32(body[i+2])<<8 | int32(body[i+3])
				lat := int32(body[i+4])<<24 | int32(body[i+5])<<16 | int32(body[i+6])<<8 | int32(body[i+7])
				f.Longitude = float64(int32(lon)) * 0.0000001
				f.Latitude = float64(int32(lat)) * 0.0000001
			}
		case 0x04: // tac/lac + ci (simplified extraction)
			if ln >= 6 {
				ci := int64(body[i+0])<<24 | int64(body[i+1])<<16 | int64(body[i+2])<<8 | int64(body[i+3])
				tac := int(body[i+4])<<8 | int(body[i+5])
				f.CI = ci
				f.TacLac = tac
			}
		}
		i += ln
	}
	return f, ts, nil
}

func onlyHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}
func be16(b []byte) int   { return int(b[0])<<8 | int(b[1]) }
func be32(b []byte) int64 { return int64(b[0])<<24 | int64(b[1])<<16 | int64(b[2])<<8 | int64(b[3]) }

// ParseMAC12 converts "CCE01BA20624" -> []byte{0xCC,0xE0,0x1B,0xA2,0x06,0x24}
func ParseMAC12(s string) ([]byte, error) {
	s = strings.ToUpper(strings.TrimSpace(s))
	if len(s) != 12 {
		return nil, fmt.Errorf("mac must be 12 hex chars")
	}
	out := make([]byte, 6)
	for i := 0; i < 6; i++ {
		var v byte
		_, err := fmt.Sscanf(s[i*2:i*2+2], "%02X", &v)
		if err != nil {
			return nil, fmt.Errorf("bad mac at %d: %w", i, err)
		}
		out[i] = v
	}
	return out, nil
}
