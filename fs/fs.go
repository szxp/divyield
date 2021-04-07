package fs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"

	"szakszon.com/divyield"
)

type DB struct {
	DataDir string
}

func (s *DB) Dividends(
	ticker string,
	f *divyield.DividendFilter,
) ([]*divyield.Dividend, error) {
	p := filepath.Join(s.DataDir, ticker, "dividends.json")

	ph, err := os.Open(p)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []*divyield.Dividend{}, nil
		}
		return nil, fmt.Errorf("open dividends file %s: %s", p, err)
	}
	defer ph.Close()

	dividends, err := parseDividends(ph)
	if err != nil {
		return nil, fmt.Errorf("parse dividends: %s", err)
	}

	sortDividendsDesc(dividends)
	return dividends, nil
}

func parseDividends(r io.Reader) ([]*divyield.Dividend, error) {
	dividends := make([]*divyield.Dividend, 0)

	dec := json.NewDecoder(r)
	// read open bracket
	_, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("open bracket: %s", err)
	}

	// while the array contains values
	for dec.More() {
		var v Dividend
		err := dec.Decode(&v)
		if err != nil {
			return nil, fmt.Errorf("decode: %s", err)
		}
		dividends = append(dividends, &v)
	}

	// read closing bracket
	_, err = dec.Token()
	if err != nil {
		return nil, fmt.Errorf("closing bracket: %s", err)
	}

	return dividends, nil
}

func sortDividendsDesc(dividends []*divyield.Dividend) {
	sort.SliceStable(dividends, func(i, j int) bool {
		ti := time.Time(dividends[i].ExDate)
		tj := time.Time(dividends[j].ExDate)
		return ti.After(tj)
	})
}

func (s *DB) PrependDividends(ticker string, dividends []*divyield.Dividend) error {
	// calc overlaping
	// save
	return nil
}

func save(ticker string, dividends []*divyield.Dividend) error {
	p := filepath.Join(s.DataDir, ticker, "dividends.json")
	tmp, err := saveJsonTmp(filepath.Dir(p), "dividends.tmp.json", dividends)
	if err != nil {
		return fmt.Errorf("save temp dividends: %s", err)
	}
	defer os.Remove(tmp)

	if err = os.Rename(tmp, p); err != nil {
		return fmt.Errorf("rename %s -> %s: %s", tmp, p, err)
	}

	return nil
}

func saveJsonTmp(dir, name string, v interface{}) (string, error) {
	tmp, err := ioutil.TempFile(dir, name)
	if err != nil {
		return "", fmt.Errorf("create temp file: %s", err)
	}
	defer tmp.Close()

	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "    ")
	if err = enc.Encode(v); err != nil {
		return "", fmt.Errorf("encode: %s", err)
	}
	if err := tmp.Close(); err != nil {
		return "", fmt.Errorf("create temp file: %s", err)
	}

	return tmp.Name(), nil
}

func (s *DB) Prices(
	ticker string,
	f *PriceFilter,
) ([]*Price, error) {
	return nil, nil
}

func (s *DB) PrependPrices(
	ticker string,
	prices []*Price,
) error {
	return nil, nil
}
